use crate::resp::{RespValue, deserialize_resp, serialize_resp};
use crate::storage::{RedisValue, ValueWithExpiry, current_timestamp, is_expired};
use crate::utils::{case_insensitive_eq, to_uppercase};
use std::collections::HashMap;
use std::io::Write;
use std::sync::MutexGuard;

pub fn handle_command<W: Write>(
    stream: &mut W,
    data: &[u8],
    db: &mut MutexGuard<HashMap<String, ValueWithExpiry>>,
) -> Result<(), String> {
    let resp = deserialize_resp(data)?;

    match resp {
        RespValue::Array(a) => {
            if let Some(RespValue::BulkString(Some(cmd))) = a.get(0) {
                let cmd_upper = to_uppercase(cmd);
                match cmd_upper.as_str() {
                    "PING" => handle_ping(stream),
                    "ECHO" => handle_echo(stream, &a),
                    "SET" => handle_set(stream, &a, db),
                    "GET" => handle_get(stream, &a, db),
                    "RPUSH" => handle_rpush(stream, &a, db),
                    _ => handle_unknown_command(stream),
                }
            } else {
                handle_unknown_command(stream);
            }
        }
        _ => {
            stream.write_all(b"+PONG\r\n").map_err(|e| e.to_string())?;
        }
    }

    Ok(())
}

fn handle_ping<W: Write>(stream: &mut W) {
    stream.write_all(b"+PONG\r\n").unwrap();
}

fn handle_echo<W: Write>(stream: &mut W, args: &[RespValue]) {
    if let Some(msg) = args.get(1) {
        let response = serialize_resp(msg.clone());
        stream.write_all(&response).unwrap();
    }
}

fn handle_set<W: Write>(
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

fn handle_get<W: Write>(
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

fn handle_unknown_command<W: Write>(stream: &mut W) {
    stream.write_all(b"-ERR unknown command\r\n").unwrap();
}

fn handle_rpush<W: Write>(
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

        // 存储回数据库
        db.insert(
            key.clone(),
            ValueWithExpiry {
                value: RedisValue::List(list),
                expiry: None, // 可以根据需要支持过期时间
            },
        );

        // 返回列表长度
        let response = format!(":{}\r\n", values.len());
        stream.write_all(response.as_bytes()).unwrap();
    }
}
