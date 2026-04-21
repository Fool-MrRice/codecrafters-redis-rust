//! 阻塞命令处理模块 - 实现BLPOP、XREAD等阻塞命令
//!
//! 包含：
//! - BlockedCommandResult: 阻塞命令处理结果枚举
//! - prepare_blpop: BLPOP命令准备阶段
//! - prepare_xread: XREAD命令准备阶段
//! - wait_for_blocked_command: 等待阻塞命令完成

use crate::storage::BlockedClient;
use crate::utils::case::to_uppercase;
use crate::utils::resp::{RespValue, serialize_resp};
use std::time::{Duration, SystemTime};

/// 阻塞命令的处理结果
///
/// 有两种情况：
/// - Immediate: 可以立即返回响应
/// - Blocking: 需要阻塞，等待通知
pub enum BlockedCommandResult {
    Immediate(Vec<u8>),
    Blocking {
        key: String,
        timeout: Duration,
        rx: tokio::sync::oneshot::Receiver<Vec<u8>>,
    },
}

/// 处理 BLPOP 命令的准备阶段
///
/// 参数：
/// - args: 命令参数
/// - db: 数据库引用
///
/// 返回：
/// - BlockedCommandResult: 处理结果
///
/// 功能：
/// 1. 检查列表是否有元素
/// 2. 如果有，立即弹出并返回
/// 3. 如果没有，将客户端添加到阻塞列表
pub fn prepare_blpop(
    args: &[RespValue],
    db: &mut tokio::sync::MutexGuard<'_, crate::storage::DatabaseInner>,
) -> Result<BlockedCommandResult, String> {
    if let (
        Some(RespValue::BulkString(Some(key))),
        Some(RespValue::BulkString(Some(timeout_str))),
    ) = (args.get(1), args.get(2))
    {
        let timeout = match timeout_str.parse::<f64>() {
            Ok(seconds) => {
                if seconds < 0.0 {
                    Duration::from_secs(0)
                } else {
                    Duration::from_secs_f64(seconds)
                }
            }
            Err(_) => {
                return Ok(BlockedCommandResult::Immediate(serialize_resp(
                    RespValue::Error("ERR value is not a valid float".to_string()),
                )));
            }
        };

        let (list_clone, expiry) = if let Some(entry) = db.data.get(key) {
            if crate::storage::is_expired(&entry.expiry) {
                (None, None)
            } else {
                match &entry.value {
                    crate::storage::RedisValue::List(list) => {
                        (Some(list.clone()), Some(entry.expiry))
                    }
                    _ => {
                        return Ok(BlockedCommandResult::Immediate(serialize_resp(
                            RespValue::Error(
                                "WRONGTYPE Operation against a key holding the wrong kind of value"
                                    .to_string(),
                            ),
                        )));
                    }
                }
            }
        } else {
            (None, None)
        };

        if let Some(mut list) = list_clone
            && !list.is_empty()
        {
            let first = list.remove(0);
            let response = serialize_resp(RespValue::Array(Some(vec![
                RespValue::BulkString(Some(key.clone())),
                RespValue::BulkString(Some(first)),
            ])));

            db.data.insert(
                key.clone(),
                crate::storage::ValueWithExpiry {
                    value: crate::storage::RedisValue::List(list),
                    expiry: expiry.unwrap(),
                },
            );
            return Ok(BlockedCommandResult::Immediate(response));
        }

        let (tx, rx) = tokio::sync::oneshot::channel();

        let blocked_client = BlockedClient {
            key: key.clone(),
            timeout,
            start_time: current_timestamp(),
            last_id: "0-0".to_string(),
            tx,
        };
        db.blocked_clients.add_client(key.clone(), blocked_client);

        Ok(BlockedCommandResult::Blocking {
            key: key.clone(),
            timeout,
            rx,
        })
    } else {
        Ok(BlockedCommandResult::Immediate(serialize_resp(
            RespValue::Error("ERR wrong number of arguments for 'blpop' command".to_string()),
        )))
    }
}

/// 处理 XREAD 命令的准备阶段
///
/// 参数：
/// - args: 命令参数
/// - db: 数据库引用
///
/// 返回：
/// - BlockedCommandResult: 处理结果
///
/// 功能：
/// 1. 解析命令参数（包括BLOCK选项）
/// 2. 检查是否有新数据
/// 3. 如果有，立即返回
/// 4. 如果没有且设置了BLOCK，将客户端添加到阻塞列表
pub fn prepare_xread(
    args: &[RespValue],
    db: &mut tokio::sync::MutexGuard<'_, crate::storage::DatabaseInner>,
) -> Result<BlockedCommandResult, String> {
    let mut i = 1;
    let mut block = None;

    if i < args.len() {
        if let Some(RespValue::BulkString(Some(opt))) = args.get(i) {
            if to_uppercase(opt) == "BLOCK" {
                i += 1;
                if let Some(RespValue::BulkString(Some(timeout_str))) = args.get(i) {
                    if let Ok(timeout) = timeout_str.parse::<u64>() {
                        block = Some(timeout);
                        i += 1;
                    } else {
                        return Ok(BlockedCommandResult::Immediate(serialize_resp(
                            RespValue::Error("ERR invalid timeout".to_string()),
                        )));
                    }
                } else {
                    return Ok(BlockedCommandResult::Immediate(serialize_resp(
                        RespValue::Error(
                            "ERR wrong number of arguments for 'xread' command".to_string(),
                        ),
                    )));
                }
            }
        }
    }

    if i < args.len() {
        if let Some(RespValue::BulkString(Some(streams))) = args.get(i) {
            if to_uppercase(streams) != "STREAMS" {
                return Ok(BlockedCommandResult::Immediate(serialize_resp(
                    RespValue::Error("ERR syntax error".to_string()),
                )));
            }
            i += 1;
        } else {
            return Ok(BlockedCommandResult::Immediate(serialize_resp(
                RespValue::Error("ERR wrong number of arguments for 'xread' command".to_string()),
            )));
        }
    } else {
        return Ok(BlockedCommandResult::Immediate(serialize_resp(
            RespValue::Error("ERR wrong number of arguments for 'xread' command".to_string()),
        )));
    }

    let mut keys = Vec::new();
    let mut ids = Vec::new();

    let total_args = args.len() - i;
    if total_args % 2 != 0 {
        return Ok(BlockedCommandResult::Immediate(serialize_resp(
            RespValue::Error("ERR wrong number of arguments for 'xread' command".to_string()),
        )));
    }
    let key_count = total_args / 2;

    for j in 0..key_count {
        if let Some(RespValue::BulkString(Some(key))) = args.get(i + j) {
            keys.push(key.clone());
        } else {
            return Ok(BlockedCommandResult::Immediate(serialize_resp(
                RespValue::Error("ERR wrong number of arguments for 'xread' command".to_string()),
            )));
        }
    }

    for j in 0..key_count {
        if let Some(RespValue::BulkString(Some(id))) = args.get(i + key_count + j) {
            ids.push(id.clone());
        } else {
            return Ok(BlockedCommandResult::Immediate(serialize_resp(
                RespValue::Error("ERR wrong number of arguments for 'xread' command".to_string()),
            )));
        }
    }

    if keys.is_empty() {
        return Ok(BlockedCommandResult::Immediate(serialize_resp(
            RespValue::Error("ERR wrong number of arguments for 'xread' command".to_string()),
        )));
    }

    let mut processed_ids = Vec::new();
    for (key, id) in keys.iter().zip(ids.iter()) {
        if id == "$" {
            if let Some(entry) = db.data.get(key) {
                if !crate::storage::is_expired(&entry.expiry) {
                    match &entry.value {
                        crate::storage::RedisValue::Stream(stream) => {
                            if let Some(last_entry) = stream.last() {
                                processed_ids.push(last_entry.id.clone());
                            } else {
                                processed_ids.push("0-0".to_string());
                            }
                        }
                        _ => processed_ids.push("0-0".to_string()),
                    }
                } else {
                    processed_ids.push("0-0".to_string());
                }
            } else {
                processed_ids.push("0-0".to_string());
            }
        } else {
            processed_ids.push(id.clone());
        }
    }

    let mut response = Vec::new();
    let mut has_data = false;

    for (key, last_id) in keys.iter().zip(processed_ids.iter()) {
        if let Some(entry) = db.data.get(key) {
            if !crate::storage::is_expired(&entry.expiry) {
                match &entry.value {
                    crate::storage::RedisValue::Stream(stream) => {
                        let mut stream_result = Vec::new();
                        for stream_entry in stream {
                            if crate::stream_id::is_id_greater(&stream_entry.id, last_id) {
                                let mut fields_array = Vec::new();
                                for field_map in &stream_entry.fields {
                                    for (k, v) in field_map {
                                        fields_array.push(RespValue::BulkString(Some(k.clone())));
                                        fields_array.push(RespValue::BulkString(Some(v.clone())));
                                    }
                                }

                                let entry_array = vec![
                                    RespValue::BulkString(Some(stream_entry.id.clone())),
                                    RespValue::Array(Some(fields_array)),
                                ];
                                stream_result.push(RespValue::Array(Some(entry_array)));
                                has_data = true;
                            }
                        }

                        if !stream_result.is_empty() {
                            let stream_array = vec![
                                RespValue::BulkString(Some(key.clone())),
                                RespValue::Array(Some(stream_result)),
                            ];
                            response.push(RespValue::Array(Some(stream_array)));
                        } else {
                            let stream_array = vec![
                                RespValue::BulkString(Some(key.clone())),
                                RespValue::Array(Some(Vec::new())),
                            ];
                            response.push(RespValue::Array(Some(stream_array)));
                        }
                    }
                    _ => {
                        let stream_array = vec![
                            RespValue::BulkString(Some(key.clone())),
                            RespValue::Array(Some(Vec::new())),
                        ];
                        response.push(RespValue::Array(Some(stream_array)));
                    }
                }
            } else {
                let stream_array = vec![
                    RespValue::BulkString(Some(key.clone())),
                    RespValue::Array(Some(Vec::new())),
                ];
                response.push(RespValue::Array(Some(stream_array)));
            }
        } else {
            let stream_array = vec![
                RespValue::BulkString(Some(key.clone())),
                RespValue::Array(Some(Vec::new())),
            ];
            response.push(RespValue::Array(Some(stream_array)));
        }
    }

    if has_data {
        let final_response = serialize_resp(RespValue::Array(Some(response)));
        Ok(BlockedCommandResult::Immediate(final_response))
    } else if let Some(timeout) = block {
        if keys.len() > 1 {
            let final_response = serialize_resp(RespValue::Array(Some(Vec::new())));
            Ok(BlockedCommandResult::Immediate(final_response))
        } else {
            let key = keys[0].clone();
            let last_id = processed_ids[0].clone();
            let (tx, rx) = tokio::sync::oneshot::channel();

            let blocked_client = BlockedClient {
                key: key.clone(),
                timeout: Duration::from_millis(timeout),
                start_time: current_timestamp(),
                last_id,
                tx,
            };
            db.blocked_clients.add_client(key.clone(), blocked_client);

            Ok(BlockedCommandResult::Blocking {
                key,
                timeout: Duration::from_millis(timeout),
                rx,
            })
        }
    } else {
        let final_response = serialize_resp(RespValue::Array(Some(Vec::new())));
        Ok(BlockedCommandResult::Immediate(final_response))
    }
}

/// 获取当前时间戳（毫秒）
///
/// 返回：
/// - 从UNIX纪元开始的毫秒数
pub fn current_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

/// 处理阻塞命令的等待阶段
///
/// 参数：
/// - result: 阻塞命令处理结果
///
/// 返回：
/// - 命令响应（RESP格式）
///
/// 功能：
/// 1. 如果是Immediate，直接返回
/// 2. 如果是Blocking，等待通知或超时
pub async fn wait_for_blocked_command(result: BlockedCommandResult) -> Vec<u8> {
    match result {
        BlockedCommandResult::Immediate(response) => response,
        BlockedCommandResult::Blocking {
            key: _,
            timeout,
            rx,
        } => {
            let result = if timeout.is_zero() {
                rx.await.ok()
            } else {
                match tokio::time::timeout(timeout, rx).await {
                    Ok(Ok(value)) => Some(value),
                    _ => None,
                }
            };

            match result {
                Some(response) => response,
                None => serialize_resp(RespValue::Array(None)),
            }
        }
    }
}
