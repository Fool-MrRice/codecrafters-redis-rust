use crate::resp::{RespValue, deserialize_resp, serialize_resp};
use crate::storage::{ValueWithExpiry, current_timestamp, is_expired};
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
                value: value.clone(),
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
                // 未过期，返回值
                let response = serialize_resp(RespValue::BulkString(Some(entry.value.clone())));
                stream.write_all(&response).unwrap();
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
