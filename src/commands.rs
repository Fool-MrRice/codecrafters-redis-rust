// 命令分发器 - 根据命令类型调用相应的处理器
//
// 功能：
// 1. 解析RESP格式的命令
// 2. 根据命令名分发到对应的处理器
// 3. 处理事务状态（MULTI/EXEC/DISCARD）
// 4. 处理WATCH/UNWATCH
// 5. 跟踪修改的键（dirty_keys）
//
// 事务处理：
// - MULTI: 开始事务，后续命令进入队列
// - EXEC: 执行事务中的所有命令
// - DISCARD: 取消事务，清空队列
// - WATCH: 监视键，如果键被修改则事务失败
// - UNWATCH: 取消监视所有键

use crate::handle::*;
use crate::utils::resp::{RespValue, deserialize_resp, serialize_resp};

use crate::utils::case::to_uppercase;
use std::sync::Arc;

/// 异步命令处理器 - 主命令分发函数
///
/// 参数：
/// - data: 原始命令数据（RESP格式）
/// - db: 数据库引用（可变）
/// - in_transaction: 是否在事务中
/// - command_queue: 事务命令队列
/// - watched_keys: 被监视的键列表
/// - dirty: 是否有键被修改
/// - app_state: 应用状态（包含配置、副本连接等）
///
/// 返回：
/// - Result<Vec<u8>, String>: 命令执行结果（RESP格式）或错误信息
///
/// 功能：
/// 1. 解析命令
/// 2. 根据命令类型分发到对应的处理器
/// 3. 处理事务逻辑
/// 4. 跟踪修改的键（用于WATCH）
pub async fn command_handler_async(
    data: &[u8],
    db: &mut tokio::sync::MutexGuard<'_, crate::storage::DatabaseInner>,
    in_transaction: &mut bool,
    command_queue: &mut Vec<Vec<u8>>,
    watched_keys: &mut Vec<String>,
    dirty: &mut bool,
    app_state: &Arc<crate::storage::AppState>,
) -> Result<Vec<u8>, String> {
    // 解析RESP格式的命令
    let (resp, _) = deserialize_resp(data)?;
    let config = &app_state.config;

    match resp {
        RespValue::Array(Some(a)) => {
            if let Some(RespValue::BulkString(Some(cmd))) = a.first() {
                let cmd_upper = to_uppercase(cmd);

                // 处理事务控制命令
                match cmd_upper.as_str() {
                    "MULTI" => handle_multi(in_transaction, command_queue),
                    "EXEC" => {
                        handle_exec(
                            db,
                            in_transaction,
                            command_queue,
                            watched_keys,
                            dirty,
                            app_state,
                        )
                        .await
                    }
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
                                "PSYNC" => handle_psync(&a, config, db),
                                "WAIT" => handle_wait_async(&a, app_state).await,
                                "CONFIG" => handle_config(&a, config),
                                _ => Ok(serialize_resp(RespValue::Error(
                                    "ERR unknown command".to_string(),
                                ))),
                            };

                            // 检查是否是修改键的命令
                            // 这些命令需要添加到dirty_keys，用于WATCH机制
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
