use crate::storage::BlockedClient;
use crate::utils::case::to_uppercase;
use crate::utils::resp::{RespValue, serialize_resp};
use std::time::{Duration, SystemTime};

// 阻塞命令的处理结果
pub enum BlockedCommandResult {
    // 立即返回响应
    Immediate(Vec<u8>),
    // 需要阻塞，等待通知
    Blocking {
        key: String,
        timeout: Duration,
        rx: tokio::sync::oneshot::Receiver<Vec<u8>>,
    },
}

// 处理 BLPOP 命令的准备阶段
pub fn prepare_blpop(
    args: &[RespValue],
    db: &mut std::sync::MutexGuard<'_, crate::storage::DatabaseInner>,
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

        // 先检查列表是否有元素
        let (list_clone, expiry) = if let Some(entry) = db.data.get(key) {
            if crate::storage::is_expired(&entry.expiry) {
                // 键已过期，视为不存在
                (None, None)
            } else {
                match &entry.value {
                    crate::storage::RedisValue::List(list) => {
                        (Some(list.clone()), Some(entry.expiry))
                    }
                    _ => {
                        // 类型不匹配
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
            // 键不存在
            (None, None)
        };

        // 如果列表不为空，直接弹出元素
        if let Some(mut list) = list_clone
            && !list.is_empty()
        {
            // 移除第一个元素
            let first = list.remove(0);
            // 返回被移除的元素和列表名
            let response = serialize_resp(RespValue::Array(Some(vec![
                RespValue::BulkString(Some(key.clone())),
                RespValue::BulkString(Some(first)),
            ])));

            // 更新数据库
            db.data.insert(
                key.clone(),
                crate::storage::ValueWithExpiry {
                    value: crate::storage::RedisValue::List(list),
                    expiry: expiry.unwrap(),
                },
            );
            return Ok(BlockedCommandResult::Immediate(response));
        }

        // 列表为空，需要阻塞
        let (tx, rx) = tokio::sync::oneshot::channel();

        // 添加客户端到阻塞列表
        let blocked_client = BlockedClient {
            key: key.clone(),
            timeout,
            start_time: current_timestamp(), // 毫秒级时间戳
            last_id: "0-0".to_string(),      // 默认为0-0
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

// 处理 XREAD 命令的准备阶段
pub fn prepare_xread(
    args: &[RespValue],
    db: &mut std::sync::MutexGuard<'_, crate::storage::DatabaseInner>,
) -> Result<BlockedCommandResult, String> {
    // 解析命令参数
    let mut i = 1;
    let mut block = None;

    // 处理 BLOCK 选项
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

    // 处理 STREAMS 关键字
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

    // 解析流键和ID
    let mut keys = Vec::new();
    let mut ids = Vec::new();

    // 计算键的数量：总参数数的一半
    let total_args = args.len() - i;
    if total_args % 2 != 0 {
        return Ok(BlockedCommandResult::Immediate(serialize_resp(
            RespValue::Error("ERR wrong number of arguments for 'xread' command".to_string()),
        )));
    }
    let key_count = total_args / 2;

    // 读取键
    for j in 0..key_count {
        if let Some(RespValue::BulkString(Some(key))) = args.get(i + j) {
            keys.push(key.clone());
        } else {
            return Ok(BlockedCommandResult::Immediate(serialize_resp(
                RespValue::Error("ERR wrong number of arguments for 'xread' command".to_string()),
            )));
        }
    }

    // 读取ID
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

    // 处理 $ 符号
    let mut processed_ids = Vec::new();
    for (key, id) in keys.iter().zip(ids.iter()) {
        if id == "$" {
            // 获取流的最大ID
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

    // 构建响应
    let mut response = Vec::new();
    let mut has_data = false;

    for (key, last_id) in keys.iter().zip(processed_ids.iter()) {
        if let Some(entry) = db.data.get(key) {
            if !crate::storage::is_expired(&entry.expiry) {
                match &entry.value {
                    crate::storage::RedisValue::Stream(stream) => {
                        // 过滤出ID大于last_id的条目
                        let mut stream_result = Vec::new();
                        for stream_entry in stream {
                            if crate::stream_id::is_id_greater(&stream_entry.id, last_id) {
                                // 构建返回格式
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
                            // 即使没有数据，也要保持流的顺序
                            let stream_array = vec![
                                RespValue::BulkString(Some(key.clone())),
                                RespValue::Array(Some(Vec::new())),
                            ];
                            response.push(RespValue::Array(Some(stream_array)));
                        }
                    }
                    _ => {
                        // 类型不匹配，返回空数组
                        let stream_array = vec![
                            RespValue::BulkString(Some(key.clone())),
                            RespValue::Array(Some(Vec::new())),
                        ];
                        response.push(RespValue::Array(Some(stream_array)));
                    }
                }
            } else {
                // 键已过期，视为不存在
                let stream_array = vec![
                    RespValue::BulkString(Some(key.clone())),
                    RespValue::Array(Some(Vec::new())),
                ];
                response.push(RespValue::Array(Some(stream_array)));
            }
        } else {
            // 键不存在
            let stream_array = vec![
                RespValue::BulkString(Some(key.clone())),
                RespValue::Array(Some(Vec::new())),
            ];
            response.push(RespValue::Array(Some(stream_array)));
        }
    }

    // 如果有数据，直接返回
    if has_data {
        let final_response = serialize_resp(RespValue::Array(Some(response)));
        Ok(BlockedCommandResult::Immediate(final_response))
    } else if let Some(timeout) = block {
        // 没有数据且设置了BLOCK，需要阻塞
        if keys.len() > 1 {
            // 多流情况，暂时不支持阻塞
            let final_response = serialize_resp(RespValue::Array(Some(Vec::new())));
            Ok(BlockedCommandResult::Immediate(final_response))
        } else {
            // 单流情况，支持阻塞
            let key = keys[0].clone();
            let last_id = processed_ids[0].clone();
            let (tx, rx) = tokio::sync::oneshot::channel();

            // 添加客户端到阻塞列表
            let blocked_client = BlockedClient {
                key: key.clone(),
                timeout: Duration::from_millis(timeout),
                start_time: current_timestamp(), // 毫秒级时间戳
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
        // 没有数据且没有设置BLOCK，返回空响应
        let final_response = serialize_resp(RespValue::Array(Some(Vec::new())));
        Ok(BlockedCommandResult::Immediate(final_response))
    }
}

// 获取当前时间戳（毫秒）
pub fn current_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

// 处理阻塞命令的等待阶段
pub async fn wait_for_blocked_command(result: BlockedCommandResult) -> Vec<u8> {
    match result {
        BlockedCommandResult::Immediate(response) => response,
        BlockedCommandResult::Blocking {
            key: _,
            timeout,
            rx,
        } => {
            // 等待通知或超时
            let result = if timeout.is_zero() {
                // 无限期阻塞
                rx.await.ok()
            } else {
                // 有限期阻塞
                match tokio::time::timeout(timeout, rx).await {
                    Ok(Ok(value)) => Some(value),
                    _ => None,
                }
            };

            // 处理结果
            match result {
                Some(response) => response,
                None => serialize_resp(RespValue::Array(None)),
            }
        }
    }
}
