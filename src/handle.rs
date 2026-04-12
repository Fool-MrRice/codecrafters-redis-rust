use crate::resp::{RespValue, serialize_resp};
use crate::storage::{RedisValue, ValueWithExpiry, current_timestamp, is_expired};
use crate::utils::to_uppercase;
use std::collections::HashMap;
use std::io::Write;
use std::sync::MutexGuard;

pub fn handle_ping<W: Write>(stream: &mut W) {
    stream.write_all(b"+PONG\r\n").unwrap();
}

pub fn handle_echo<W: Write>(stream: &mut W, args: &[RespValue]) {
    if let Some(msg) = args.get(1) {
        let response = serialize_resp(msg.clone());
        stream.write_all(&response).unwrap();
    }
}

pub fn handle_set<W: Write>(
    stream: &mut W,
    args: &[RespValue],
    db: &mut MutexGuard<HashMap<String, ValueWithExpiry>>,
) {
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
        db.insert(
            key.clone(),
            ValueWithExpiry {
                value: RedisValue::String(value.clone()),
                expiry,
            },
        );

        stream.write_all(b"+OK\r\n").unwrap();
    }
}

pub fn handle_get<W: Write>(
    stream: &mut W,
    args: &[RespValue],
    db: &mut MutexGuard<HashMap<String, ValueWithExpiry>>,
) {
    if let Some(RespValue::BulkString(Some(key))) = args.get(1) {
        // 检查键是否存在
        if let Some(entry) = db.get(key) {
            // 检查是否过期（惰性删除）
            if is_expired(&entry.expiry) {
                // 过期，删除键
                db.remove(key);
                // 返回空
                let response = serialize_resp(RespValue::BulkString(None));
                stream.write_all(&response).unwrap();
            } else {
                // 未过期，检查类型
                match &entry.value {
                    RedisValue::String(value) => {
                        // 是字符串类型，返回值
                        let response = serialize_resp(RespValue::BulkString(Some(value.clone())));
                        stream.write_all(&response).unwrap();
                    }
                    _ => {
                        // 类型不匹配，返回错误
                        stream.write_all(b"-ERR WRONGTYPE Operation against a key holding the wrong kind of value\r\n").unwrap();
                    }
                }
            }
        } else {
            // 键不存在
            let response = serialize_resp(RespValue::BulkString(None));
            stream.write_all(&response).unwrap();
        }
    }
}

pub fn handle_unknown_command<W: Write>(stream: &mut W) {
    stream.write_all(b"-ERR unknown command\r\n").unwrap();
}

pub fn handle_rpush<W: Write>(
    stream: &mut W,
    args: &[RespValue],
    db: &mut MutexGuard<HashMap<String, ValueWithExpiry>>,
) {
    if let Some(RespValue::BulkString(Some(key))) = args.get(1) {
        // 收集所有值
        let mut values = Vec::new();
        for arg in args.iter().skip(2) {
            if let RespValue::BulkString(Some(value)) = arg {
                values.push(value.clone());
            }
        }

        // 更新数据库
        let mut list = if let Some(entry) = db.get(key) {
            if !is_expired(&entry.expiry) {
                match &entry.value {
                    RedisValue::List(existing_list) => existing_list.clone(),
                    _ => {
                        // 如果键存在但不是列表，返回错误
                        stream.write_all(b"-ERR WRONGTYPE Operation against a key holding the wrong kind of value\r\n").unwrap();
                        return;
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
        db.insert(
            key.clone(),
            ValueWithExpiry {
                value: RedisValue::List(list),
                expiry: None, // 可以根据需要支持过期时间
            },
        );

        stream
            .write_all(serialize_resp(RespValue::Integer(list_len)).as_slice())
            .unwrap();
    }
}

pub fn handle_lrange<W: Write>(
    stream: &mut W,
    a: &[RespValue],
    db: &mut MutexGuard<'_, HashMap<String, ValueWithExpiry>>,
) {
    fn wrong_lrange_response<W: Write>(stream: &mut W) {
        let response = serialize_resp(RespValue::Array(Vec::new()));
        stream.write_all(&response).unwrap();
    }
    if let (
        Some(RespValue::BulkString(Some(key))),
        Some(RespValue::BulkString(Some(start))),
        Some(RespValue::BulkString(Some(stop))),
    ) = (a.get(1), a.get(2), a.get(3))
    {
        let mut start = start.parse::<i64>().unwrap();
        let mut stop = stop.parse::<i64>().unwrap();
        if let Some(ValueWithExpiry {
            value: RedisValue::List(list),
            ..
        }) = db.get(key)
        {
            let list_len = list.len() as i64;
            if start >= list_len {
                wrong_lrange_response(stream);
                return;
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
                wrong_lrange_response(stream);
                return;
            }
            let mut response = Vec::new();
            for item in list[start as usize..=stop as usize].iter() {
                response.push(RespValue::BulkString(Some(item.clone())));
            }
            let response = serialize_resp(RespValue::Array(response));
            stream.write_all(&response).unwrap();
        } else {
            wrong_lrange_response(stream);
            return;
        }
    } else {
        handle_unknown_command(stream);
    }
}

pub fn handle_lpush<W: Write>(
    stream: &mut W,
    args: &[RespValue],
    db: &mut MutexGuard<HashMap<String, ValueWithExpiry>>,
) {
    if let Some(RespValue::BulkString(Some(key))) = args.get(1) {
        // 收集所有值
        let mut values = Vec::new();
        for arg in args.iter().skip(2) {
            if let RespValue::BulkString(Some(value)) = arg {
                values.push(value.clone());
            }
        }

        // 更新数据库
        let mut list = if let Some(entry) = db.get(key) {
            if !is_expired(&entry.expiry) {
                match &entry.value {
                    RedisValue::List(existing_list) => existing_list.clone(),
                    _ => {
                        // 如果键存在但不是列表，返回错误
                        stream.write_all(b"-ERR WRONGTYPE Operation against a key holding the wrong kind of value\r\n").unwrap();
                        return;
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
        for value in values {
            list.insert(0, value);
        }

        // 返回列表长度
        let list_len = list.len() as i64;

        // 存储回数据库
        db.insert(
            key.clone(),
            ValueWithExpiry {
                value: RedisValue::List(list),
                expiry: None, // 可以根据需要支持过期时间
            },
        );

        stream
            .write_all(serialize_resp(RespValue::Integer(list_len)).as_slice())
            .unwrap();
    }
}
pub fn handle_llen<W: Write>(
    stream: &mut W,
    a: &[RespValue],
    db: &mut MutexGuard<'_, HashMap<String, ValueWithExpiry>>,
) {
    if let Some(RespValue::BulkString(Some(key))) = a.get(1) {
        if let Some(ValueWithExpiry {
            value: RedisValue::List(list),
            ..
        }) = db.get(key)
        {
            let list_len = list.len() as i64;
            let response = serialize_resp(RespValue::Integer(list_len));
            stream.write_all(&response).unwrap();
        } else {
            let response = serialize_resp(RespValue::Integer(0));
            stream.write_all(&response).unwrap();
        }
    } else {
        let response = serialize_resp(RespValue::Integer(0));
        stream.write_all(&response).unwrap();
    }
}
