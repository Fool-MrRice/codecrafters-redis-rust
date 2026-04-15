use crate::handle::{
    handle_echo, handle_get, handle_incr, handle_llen, handle_lpop, handle_lpush, handle_lrange,
    handle_rpush, handle_set, handle_type, handle_xadd, handle_xrange,
};
use crate::utils::resp::{RespValue, deserialize_resp, serialize_resp};

use crate::utils::case::to_uppercase;
use std::sync::MutexGuard;

pub fn command_handler(
    data: &[u8],
    db: &mut MutexGuard<'_, crate::storage::DatabaseInner>,
    in_transaction: &mut bool,
    command_queue: &mut Vec<Vec<u8>>,
    watched_keys: &mut Vec<String>,
    dirty: &mut bool,
) -> Result<Vec<u8>, String> {
    let resp = deserialize_resp(data)?;

    match resp {
        RespValue::Array(Some(a)) => {
            if let Some(RespValue::BulkString(Some(cmd))) = a.first() {
                let cmd_upper = to_uppercase(cmd);
                // 处理事务控制命令
                match cmd_upper.as_str() {
                    "MULTI" => {
                        *in_transaction = true;
                        command_queue.clear();
                        Ok(serialize_resp(RespValue::SimpleString("OK".to_string())))
                    }
                    "EXEC" => {
                        if !*in_transaction {
                            Ok(serialize_resp(RespValue::Error(
                                "ERR EXEC without MULTI".to_string(),
                            )))
                        } else {
                            *in_transaction = false;
                            // 检查被监视的键是否在dirty_keys中
                            for key in &*watched_keys {
                                if db.dirty_keys.contains(key) {
                                    *dirty = true;
                                    break;
                                }
                            }
                            // 检查dirty标记，如果为true则中止事务
                            if *dirty {
                                // 清除监视状态
                                watched_keys.clear();
                                *dirty = false;
                                // 清空dirty_keys集合
                                db.dirty_keys.clear();
                                // 返回空数组
                                Ok(serialize_resp(RespValue::Array(None)))
                            } else {
                                // 事务执行命令响应队列
                                let mut responses = Vec::new();
                                for cmd_data in command_queue.drain(..) {
                                    if let Ok(cmd_resp) = command_handler(
                                        &cmd_data,
                                        db,
                                        &mut false,
                                        &mut Vec::new(),
                                        &mut Vec::new(),
                                        &mut false,
                                    ) {
                                        responses.push(cmd_resp);
                                    } else {
                                        responses.push(serialize_resp(RespValue::Error(
                                            "ERR command execution failed".to_string(),
                                        )));
                                    }
                                }
                                // 清除监视状态
                                watched_keys.clear();
                                *dirty = false;
                                // 清空dirty_keys集合
                                db.dirty_keys.clear();
                                // 构建响应数组
                                // 这里没有想明白如何使用resp来构建响应数组，所以直接使用format!来构建
                                let mut result = Vec::new();
                                result.extend(format!("*{}\r\n", responses.len()).as_bytes());
                                for resp in responses {
                                    result.extend(resp);
                                }
                                Ok(result)
                            }
                        }
                    }
                    "DISCARD" => {
                        if !*in_transaction {
                            Ok(serialize_resp(RespValue::Error(
                                "ERR DISCARD without MULTI".to_string(),
                            )))
                        } else {
                            *in_transaction = false;
                            command_queue.clear();
                            // 清除监视状态
                            watched_keys.clear();
                            *dirty = false;
                            Ok(serialize_resp(RespValue::SimpleString("OK".to_string())))
                        }
                    }
                    "WATCH" => {
                        if *in_transaction {
                            // 在事务内调用WATCH，返回错误
                            Ok(serialize_resp(RespValue::Error(
                                "ERR WATCH inside MULTI is not allowed".to_string(),
                            )))
                        } else {
                            // 解析参数，添加键到监视集合
                            for i in 1..a.len() {
                                if let RespValue::BulkString(Some(key)) = &a[i] {
                                    watched_keys.push(key.clone());
                                }
                            }
                            Ok(serialize_resp(RespValue::SimpleString("OK".to_string())))
                        }
                    }
                    "UNWATCH" => {
                        // 清除所有监视的键
                        watched_keys.clear();
                        *dirty = false;
                        Ok(serialize_resp(RespValue::SimpleString("OK".to_string())))
                    }
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
                                "PING" => Ok(b"+PONG\r\n".to_vec()),
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
                                _ => Ok(b"-ERR unknown command\r\n".to_vec()),
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
                    Ok(b"+QUEUED\r\n".to_vec())
                } else {
                    Ok(b"-ERR unknown command\r\n".to_vec())
                }
            }
        }
        _ => {
            if *in_transaction {
                // 事务中，将命令加入队列
                command_queue.push(data.to_vec());
                Ok(b"+QUEUED\r\n".to_vec())
            } else {
                Ok(b"+PONG\r\n".to_vec())
            }
        }
    }
}
