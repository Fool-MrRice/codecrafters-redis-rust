use crate::handle::{
    handle_discard, handle_echo, handle_exec, handle_get, handle_incr, handle_info, handle_llen,
    handle_lpop, handle_lpush, handle_lrange, handle_multi, handle_ping, handle_replconf,
    handle_rpush, handle_set, handle_type, handle_unwatch, handle_watch, handle_xadd,
    handle_xrange,
};
use crate::utils::resp::{RespValue, deserialize_resp, serialize_resp};

use crate::utils::case::to_uppercase;
use std::sync::{Arc, Mutex, MutexGuard};

pub fn command_handler(
    data: &[u8],
    db: &mut MutexGuard<'_, crate::storage::DatabaseInner>,
    in_transaction: &mut bool,
    command_queue: &mut Vec<Vec<u8>>,
    watched_keys: &mut Vec<String>,
    dirty: &mut bool,
    config: &Arc<Mutex<crate::storage::Config>>,
) -> Result<Vec<u8>, String> {
    let resp = deserialize_resp(data)?;

    match resp {
        RespValue::Array(Some(a)) => {
            if let Some(RespValue::BulkString(Some(cmd))) = a.first() {
                let cmd_upper = to_uppercase(cmd);
                // 处理事务控制命令
                match cmd_upper.as_str() {
                    "MULTI" => handle_multi(in_transaction, command_queue),
                    "EXEC" => handle_exec(
                        db,
                        in_transaction,
                        command_queue,
                        watched_keys,
                        dirty,
                        config,
                    ),
                    "DISCARD" => handle_discard(in_transaction, command_queue, watched_keys, dirty),
                    "WATCH" => handle_watch(db, in_transaction, watched_keys, dirty, &a),
                    "UNWATCH" => handle_unwatch(watched_keys, dirty),
                    _ => {
                        // 其他命令
                        if *in_transaction {
                            // 事务中，将命令加入队列
                            command_queue.push(data.to_vec());
                            Ok(serialize_resp(RespValue::SimpleString(
                                "QUEUED".to_string(),
                            )))
                        } else {
                            // 非事务中，立即执行命令
                            let result = match cmd_upper.as_str() {
                                "PING" => handle_ping(),
                                "ECHO" => handle_echo(&a),
                                "SET" => handle_set(&a, db),
                                "GET" => handle_get(&a, db),
                                "RPUSH" => handle_rpush(&a, db),
                                "LPUSH" => handle_lpush(&a, db),
                                "LRANGE" => handle_lrange(&a, db),
                                "LLEN" => handle_llen(&a, db),
                                "LPOP" => handle_lpop(&a, db),
                                "TYPE" => handle_type(&a, db),
                                "XADD" => handle_xadd(&a, db),
                                "XRANGE" => handle_xrange(&a, db),
                                "INCR" => handle_incr(&a, db),
                                "INFO" => handle_info(&a, config),
                                "REPLCONF" => handle_replconf(&a, config),
                                _ => Ok(serialize_resp(RespValue::Error(
                                    "ERR unknown command".to_string(),
                                ))),
                            };

                            // 检查是否是修改键的命令
                            if let Some(RespValue::BulkString(Some(cmd))) = a.first() {
                                let cmd_upper = to_uppercase(cmd);
                                match cmd_upper.as_str() {
                                    "SET" | "RPUSH" | "LPUSH" | "LPOP" | "XADD" | "INCR" => {
                                        // 提取键并添加到dirty_keys
                                        if let Some(RespValue::BulkString(Some(key))) = a.get(1) {
                                            // 只有在非事务模式下才添加到dirty_keys
                                            // 事务中的命令不应该影响当前事务的执行
                                            // 这里的in_transaction参数是当前命令的执行模式
                                            // 而不是当前连接的事务状态
                                            if !*in_transaction {
                                                db.dirty_keys.insert(key.clone());
                                            }
                                        }
                                    }
                                    _ => {}
                                }
                            }

                            result
                        }
                    }
                }
            } else {
                if *in_transaction {
                    // 事务中，将命令加入队列
                    command_queue.push(data.to_vec());
                    Ok(serialize_resp(RespValue::SimpleString(
                        "QUEUED".to_string(),
                    )))
                } else {
                    Ok(serialize_resp(RespValue::Error(
                        "ERR unknown command".to_string(),
                    )))
                }
            }
        }
        _ => {
            if *in_transaction {
                // 事务中，将命令加入队列
                command_queue.push(data.to_vec());
                Ok(serialize_resp(RespValue::SimpleString(
                    "QUEUED".to_string(),
                )))
            } else {
                Ok(handle_ping()?)
            }
        }
    }
}
