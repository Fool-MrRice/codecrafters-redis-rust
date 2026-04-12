use crate::resp::{RespValue, serialize_resp};
use crate::storage::BlockedClient;
use std::time::SystemTime;

// 阻塞命令的处理结果
pub enum BlockedCommandResult {
    // 立即返回响应
    Immediate(Vec<u8>),
    // 需要阻塞，等待通知
    Blocking {
        key: String,
        timeout: u64,
        rx: tokio::sync::oneshot::Receiver<(String, String)>,
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
        let timeout = timeout_str.parse::<u64>().unwrap();

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
                        return Ok(BlockedCommandResult::Immediate(
                            b"-ERR WRONGTYPE Operation against a key holding the wrong kind of value\r\n".to_vec()
                        ));
                    }
                }
            }
        } else {
            // 键不存在
            (None, None)
        };

        // 如果列表不为空，直接弹出元素
        if let Some(mut list) = list_clone {
            if !list.is_empty() {
                // 移除第一个元素
                let first = list.remove(0);
                // 返回被移除的元素和列表名
                let response = serialize_resp(RespValue::Array(vec![
                    RespValue::BulkString(Some(key.clone())),
                    RespValue::BulkString(Some(first)),
                ]));

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
        }

        // 列表为空，需要阻塞
        let (tx, rx) = tokio::sync::oneshot::channel();

        // 添加客户端到阻塞列表
        let blocked_client = BlockedClient {
            key: key.clone(),
            timeout,
            start_time: current_timestamp() / 1000, // 转换为秒
            tx,
        };
        db.blocked_clients.add_client(key.clone(), blocked_client);

        Ok(BlockedCommandResult::Blocking {
            key: key.clone(),
            timeout,
            rx,
        })
    } else {
        Ok(BlockedCommandResult::Immediate(
            b"-ERR wrong number of arguments for 'blpop' command\r\n".to_vec(),
        ))
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
            let result = if timeout == 0 {
                // 无限期阻塞
                match rx.await {
                    Ok(value) => Some(value),
                    Err(_) => None,
                }
            } else {
                // 有限期阻塞
                match tokio::time::timeout(tokio::time::Duration::from_secs(timeout), rx).await {
                    Ok(Ok(value)) => Some(value),
                    _ => None,
                }
            };

            // 处理结果
            match result {
                Some((list_name, element)) => serialize_resp(RespValue::Array(vec![
                    RespValue::BulkString(Some(list_name)),
                    RespValue::BulkString(Some(element)),
                ])),
                None => serialize_resp(RespValue::Array(vec![])),
            }
        }
    }
}
