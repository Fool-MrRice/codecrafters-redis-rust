use crate::resp::{RespValue, serialize_resp};
use crate::storage::{RedisValue, ValueWithExpiry, current_timestamp, is_expired};
use crate::utils::to_uppercase;

use std::sync::MutexGuard;

pub fn handle_echo(args: &[RespValue]) -> Result<Vec<u8>, String> {
    if let Some(msg) = args.get(1) {
        let response = serialize_resp(msg.clone());
        Ok(response)
    } else {
        Ok(b"-ERR wrong number of arguments for 'echo' command\r\n".to_vec())
    }
}

pub fn handle_set(
    args: &[RespValue],
    db: &mut MutexGuard<'_, crate::storage::DatabaseInner>,
) -> Result<Vec<u8>, String> {
    if let (Some(RespValue::BulkString(Some(key))), Some(RespValue::BulkString(Some(value)))) =
        (args.get(1), args.get(2))
    {
        let mut expiry = None;

        // 解析过期时间参数
        if args.len() >= 5 {
            if let (
                Some(RespValue::BulkString(Some(option))),
                Some(RespValue::BulkString(Some(time_str))),
            ) = (args.get(3), args.get(4))
            {
                if let Ok(time) = time_str.parse::<u64>() {
                    let option_upper = to_uppercase(option);
                    match option_upper.as_str() {
                        "EX" => expiry = Some(current_timestamp() + time * 1000), // 秒转毫秒
                        "PX" => expiry = Some(current_timestamp() + time),        // 直接用毫秒
                        _ => {}
                    }
                }
            }
        }

        // 存储键值对和过期时间
        db.data.insert(
            key.clone(),
            ValueWithExpiry {
                value: RedisValue::String(value.clone()),
                expiry,
            },
        );

        Ok(b"+OK\r\n".to_vec())
    } else {
        Ok(b"-ERR wrong number of arguments for 'set' command\r\n".to_vec())
    }
}

pub fn handle_get(
    args: &[RespValue],
    db: &mut MutexGuard<'_, crate::storage::DatabaseInner>,
) -> Result<Vec<u8>, String> {
    if let Some(RespValue::BulkString(Some(key))) = args.get(1) {
        // 检查键是否存在
        if let Some(entry) = db.data.get(key) {
            // 检查是否过期（惰性删除）
            if is_expired(&entry.expiry) {
                // 过期，删除键
                db.data.remove(key);
                // 返回空
                let response = serialize_resp(RespValue::BulkString(None));
                Ok(response)
            } else {
                // 未过期，检查类型
                match &entry.value {
                    RedisValue::String(value) => {
                        // 是字符串类型，返回值
                        let response = serialize_resp(RespValue::BulkString(Some(value.clone())));
                        Ok(response)
                    }
                    _ => {
                        // 类型不匹配，返回错误
                        Ok(b"-ERR WRONGTYPE Operation against a key holding the wrong kind of value\r\n".to_vec())
                    }
                }
            }
        } else {
            // 键不存在
            let response = serialize_resp(RespValue::BulkString(None));
            Ok(response)
        }
    } else {
        Ok(b"-ERR wrong number of arguments for 'get' command\r\n".to_vec())
    }
}

pub fn handle_rpush(
    args: &[RespValue],
    db: &mut MutexGuard<'_, crate::storage::DatabaseInner>,
) -> Result<Vec<u8>, String> {
    if let Some(RespValue::BulkString(Some(key))) = args.get(1) {
        // 收集所有值
        let mut values = Vec::new();
        for arg in args.iter().skip(2) {
            if let RespValue::BulkString(Some(value)) = arg {
                values.push(value.clone());
            }
        }

        // 更新数据库
        let mut list = if let Some(entry) = db.data.get(key) {
            if !is_expired(&entry.expiry) {
                match &entry.value {
                    RedisValue::List(existing_list) => existing_list.clone(),
                    _ => {
                        // 如果键存在但不是列表，返回错误
                        return Ok(b"-ERR WRONGTYPE Operation against a key holding the wrong kind of value\r\n".to_vec());
                    }
                }
            } else {
                // 键已过期，视为不存在
                Vec::new()
            }
        } else {
            // 键不存在，创建新列表
            Vec::new()
        };

        // 添加新值
        list.extend(values.clone());

        // 返回列表长度
        let list_len = list.len() as i64;

        // 存储回数据库
        db.data.insert(
            key.clone(),
            ValueWithExpiry {
                value: RedisValue::List(list),
                expiry: None, // 可以根据需要支持过期时间
            },
        );

        // 检查是否有阻塞的客户端等待这个列表
        if let Some(blocked_client) = db.blocked_clients.pop_client(&key) {
            // 通知阻塞的客户端
            let _ = blocked_client.tx.send((key.clone(), values[0].clone()));
        }

        let response = serialize_resp(RespValue::Integer(list_len));
        Ok(response)
    } else {
        Ok(b"-ERR wrong number of arguments for 'rpush' command\r\n".to_vec())
    }
}

pub fn handle_lrange(
    args: &[RespValue],
    db: &mut MutexGuard<'_, crate::storage::DatabaseInner>,
) -> Result<Vec<u8>, String> {
    fn wrong_lrange_response() -> Vec<u8> {
        let response = serialize_resp(RespValue::Array(Vec::new()));
        response
    }
    if let (
        Some(RespValue::BulkString(Some(key))),
        Some(RespValue::BulkString(Some(start))),
        Some(RespValue::BulkString(Some(stop))),
    ) = (args.get(1), args.get(2), args.get(3))
    {
        let mut start = start.parse::<i64>().unwrap();
        let mut stop = stop.parse::<i64>().unwrap();
        if let Some(ValueWithExpiry {
            value: RedisValue::List(list),
            ..
        }) = db.data.get(key)
        {
            let list_len = list.len() as i64;
            if start >= list_len {
                return Ok(wrong_lrange_response());
            }
            if start < 0 {
                start += list_len;
                if start < 0 {
                    start = 0;
                }
            }
            if stop < 0 {
                stop += list_len;
                if stop < 0 {
                    stop = 0;
                }
            }
            if stop >= list_len {
                stop = list_len - 1;
            }
            if start > stop {
                return Ok(wrong_lrange_response());
            }
            let mut response = Vec::new();
            for item in list[start as usize..=stop as usize].iter() {
                response.push(RespValue::BulkString(Some(item.clone())));
            }
            let response = serialize_resp(RespValue::Array(response));
            Ok(response)
        } else {
            Ok(wrong_lrange_response())
        }
    } else {
        Ok(b"-ERR wrong number of arguments for 'lrange' command\r\n".to_vec())
    }
}

pub fn handle_lpush(
    args: &[RespValue],
    db: &mut MutexGuard<'_, crate::storage::DatabaseInner>,
) -> Result<Vec<u8>, String> {
    if let Some(RespValue::BulkString(Some(key))) = args.get(1) {
        // 收集所有值
        let mut values = Vec::new();
        for arg in args.iter().skip(2) {
            if let RespValue::BulkString(Some(value)) = arg {
                values.push(value.clone());
            }
        }

        // 更新数据库
        let mut list = if let Some(entry) = db.data.get(key) {
            if !is_expired(&entry.expiry) {
                match &entry.value {
                    RedisValue::List(existing_list) => existing_list.clone(),
                    _ => {
                        // 如果键存在但不是列表，返回错误
                        return Ok(b"-ERR WRONGTYPE Operation against a key holding the wrong kind of value\r\n".to_vec());
                    }
                }
            } else {
                // 键已过期，视为不存在
                Vec::new()
            }
        } else {
            // 键不存在，创建新列表
            Vec::new()
        };

        //  prepend新值（不需要reverse，直接从后往前插入）
        for value in &values {
            list.insert(0, value.clone());
        }

        // 返回列表长度
        let list_len = list.len() as i64;

        // 存储回数据库
        db.data.insert(
            key.clone(),
            ValueWithExpiry {
                value: RedisValue::List(list),
                expiry: None, // 可以根据需要支持过期时间
            },
        );

        // 检查是否有阻塞的客户端等待这个列表
        if let Some(blocked_client) = db.blocked_clients.pop_client(&key) {
            // 通知阻塞的客户端
            let _ = blocked_client.tx.send((key.clone(), values[0].clone()));
        }

        let response = serialize_resp(RespValue::Integer(list_len));
        Ok(response)
    } else {
        Ok(b"-ERR wrong number of arguments for 'lpush' command\r\n".to_vec())
    }
}
pub fn handle_llen(
    args: &[RespValue],
    db: &mut MutexGuard<'_, crate::storage::DatabaseInner>,
) -> Result<Vec<u8>, String> {
    if let Some(RespValue::BulkString(Some(key))) = args.get(1) {
        if let Some(ValueWithExpiry {
            value: RedisValue::List(list),
            ..
        }) = db.data.get(key)
        {
            let list_len = list.len() as i64;
            let response = serialize_resp(RespValue::Integer(list_len));
            Ok(response)
        } else {
            let response = serialize_resp(RespValue::Integer(0));
            Ok(response)
        }
    } else {
        let response = serialize_resp(RespValue::Integer(0));
        Ok(response)
    }
}
pub fn handle_lpop(
    args: &[RespValue],
    db: &mut MutexGuard<'_, crate::storage::DatabaseInner>,
) -> Result<Vec<u8>, String> {
    if let (Some(RespValue::BulkString(Some(key))), count_opt) = (args.get(1), args.get(2)) {
        // 先获取键的值并克隆到局部变量
        let (list_clone, expiry) = if let Some(entry) = db.data.get(key) {
            if is_expired(&entry.expiry) {
                // 键已过期，视为不存在
                (None, None)
            } else {
                match &entry.value {
                    RedisValue::List(list) => (Some(list.clone()), Some(entry.expiry)),
                    _ => {
                        // 类型不匹配
                        return Ok(b"-ERR WRONGTYPE Operation against a key holding the wrong kind of value\r\n".to_vec());
                    }
                }
            }
        } else {
            // 键不存在
            (None, None)
        };

        // 处理弹出操作
        if let Some(list) = list_clone {
            if list.is_empty() {
                // 列表为空，返回nil
                let response = serialize_resp(RespValue::BulkString(None));
                Ok(response)
            } else {
                let count = match count_opt {
                    Some(RespValue::BulkString(Some(count))) => count.parse().unwrap(),
                    _ => 1,
                };
                // 创建新列表
                let mut updated_list = list;
                match count {
                    0 => {
                        // 弹出0个元素，返回nil
                        let response = serialize_resp(RespValue::BulkString(None));
                        Ok(response)
                    }
                    1 => {
                        // 移除第一个元素
                        let first = updated_list.remove(0);
                        // 返回被移除的元素
                        let response = serialize_resp(RespValue::BulkString(Some(first)));
                        // 更新数据库
                        db.data.insert(
                            key.clone(),
                            ValueWithExpiry {
                                value: RedisValue::List(updated_list),
                                expiry: expiry.unwrap(),
                            },
                        );
                        Ok(response)
                    }
                    mut count => {
                        // 弹出多个元素，返回被移除的元素
                        let mut pop_list = Vec::new();
                        if count > updated_list.len() {
                            count = updated_list.len();
                        }
                        for _ in 0..count {
                            pop_list.push(RespValue::BulkString(Some(updated_list.remove(0))));
                        }
                        let response = serialize_resp(RespValue::Array(pop_list));
                        // 更新数据库
                        db.data.insert(
                            key.clone(),
                            ValueWithExpiry {
                                value: RedisValue::List(updated_list),
                                expiry: expiry.unwrap(),
                            },
                        );
                        Ok(response)
                    }
                }
            }
        } else {
            // 键不存在或已过期
            let response = serialize_resp(RespValue::BulkString(None));
            Ok(response)
        }
    } else {
        let response = serialize_resp(RespValue::BulkString(None));
        Ok(response)
    }
}

pub fn handle_blpop(
    args: &[RespValue],
    db: &mut MutexGuard<'_, crate::storage::DatabaseInner>,
) -> Result<Vec<u8>, String> {
    if let (
        Some(RespValue::BulkString(Some(key))),
        Some(RespValue::BulkString(Some(timeout_str))),
    ) = (args.get(1), args.get(2))
    {
        let _timeout = timeout_str.parse::<u64>().unwrap();

        // 先检查列表是否有元素
        let (list_clone, expiry) = if let Some(entry) = db.data.get(key) {
            if is_expired(&entry.expiry) {
                // 键已过期，视为不存在
                (None, None)
            } else {
                match &entry.value {
                    RedisValue::List(list) => (Some(list.clone()), Some(entry.expiry)),
                    _ => {
                        // 类型不匹配
                        return Ok(b"-ERR WRONGTYPE Operation against a key holding the wrong kind of value\r\n".to_vec());
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
                    ValueWithExpiry {
                        value: RedisValue::List(list),
                        expiry: expiry.unwrap(),
                    },
                );
                return Ok(response);
            }
        }

        // 列表为空，返回空数组模拟超时
        let response = serialize_resp(RespValue::Array(vec![]));
        Ok(response)
    } else {
        Ok(b"-ERR wrong number of arguments for 'blpop' command\r\n".to_vec())
    }
}
