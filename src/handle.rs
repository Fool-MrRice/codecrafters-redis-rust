use crate::storage::{RedisValue, ValueWithExpiry, current_timestamp, is_expired};
use crate::stream_id::{is_id_in_range, process_stream_id};
use crate::utils::case::to_uppercase;
use crate::utils::resp::{RespValue, serialize_resp};

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::io::AsyncWriteExt;
pub fn handle_ping() -> Result<Vec<u8>, String> {
    Ok(serialize_resp(RespValue::SimpleString("PONG".to_string())))
}

pub fn handle_echo(args: &[RespValue]) -> Result<Vec<u8>, String> {
    if let Some(msg) = args.get(1) {
        let response = serialize_resp(msg.clone());
        Ok(response)
    } else {
        Ok(serialize_resp(RespValue::Error(
            "ERR wrong number of arguments for 'echo' command".to_string(),
        )))
    }
}

pub fn handle_set(
    args: &[RespValue],
    db: &mut tokio::sync::MutexGuard<'_, crate::storage::DatabaseInner>,
) -> Result<Vec<u8>, String> {
    if let (Some(RespValue::BulkString(Some(key))), Some(RespValue::BulkString(Some(value)))) =
        (args.get(1), args.get(2))
    {
        let mut expiry = None;

        // 解析过期时间参数
        if args.len() >= 5
            && let (
                Some(RespValue::BulkString(Some(option))),
                Some(RespValue::BulkString(Some(time_str))),
            ) = (args.get(3), args.get(4))
            && let Ok(time) = time_str.parse::<u64>()
        {
            let option_upper = to_uppercase(&option);
            match option_upper.as_str() {
                "EX" => expiry = Some(current_timestamp() + time * 1000), // 秒转毫秒
                "PX" => expiry = Some(current_timestamp() + time),        // 直接用毫秒
                _ => {}
            }
        }
        let set_value = RedisValue::String(value.clone());

        // 存储键值对和过期时间
        db.data.insert(
            key.clone(),
            ValueWithExpiry {
                value: set_value,
                expiry,
            },
        );

        Ok(serialize_resp(RespValue::SimpleString("OK".to_string())))
    } else {
        Ok(serialize_resp(RespValue::Error(
            "ERR wrong number of arguments for 'set' command".to_string(),
        )))
    }
}

pub fn handle_get(
    args: &[RespValue],
    db: &mut tokio::sync::MutexGuard<'_, crate::storage::DatabaseInner>,
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
                        Ok(serialize_resp(RespValue::Error(
                            "WRONGTYPE Operation against a key holding the wrong kind of value"
                                .to_string(),
                        )))
                    }
                }
            }
        } else {
            // 键不存在
            let response = serialize_resp(RespValue::BulkString(None));
            Ok(response)
        }
    } else {
        Ok(serialize_resp(RespValue::Error(
            "ERR wrong number of arguments for 'get' command".to_string(),
        )))
    }
}

pub fn handle_rpush(
    args: &[RespValue],
    db: &mut tokio::sync::MutexGuard<'_, crate::storage::DatabaseInner>,
) -> Result<Vec<u8>, String> {
    if let Some(RespValue::BulkString(Some(key))) = args.get(1) {
        // 收集所有值
        let mut values = Vec::new();
        for arg in args.iter().skip(2) {
            if let RespValue::BulkString(Some(value)) = arg {
                values.push(value.clone());
            }
        }

        // 先检查是否有阻塞的客户端等待这个列表
        if let Some(blocked_client) = db.blocked_clients.pop_client(key) {
            // 通知阻塞的客户端，不添加元素到列表
            let response = serialize_resp(RespValue::Array(Some(vec![
                RespValue::BulkString(Some(key.clone())),
                RespValue::BulkString(Some(values[0].clone())),
            ])));
            let _ = blocked_client.tx.send(response);
            let response = serialize_resp(RespValue::Integer(1));
            return Ok(response);
        }

        // 更新数据库
        let mut list = if let Some(entry) = db.data.get(key) {
            if !is_expired(&entry.expiry) {
                match &entry.value {
                    RedisValue::List(existing_list) => existing_list.clone(),
                    _ => {
                        // 如果键存在但不是列表，返回错误
                        return Ok(serialize_resp(RespValue::Error(
                            "WRONGTYPE Operation against a key holding the wrong kind of value"
                                .to_string(),
                        )));
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

        let response = serialize_resp(RespValue::Integer(list_len));
        Ok(response)
    } else {
        Ok(serialize_resp(RespValue::SimpleString(
            "ERR wrong number of arguments for 'rpush' command".to_string(),
        )))
    }
}

pub fn handle_lrange(
    args: &[RespValue],
    db: &mut tokio::sync::MutexGuard<'_, crate::storage::DatabaseInner>,
) -> Result<Vec<u8>, String> {
    fn wrong_lrange_response() -> Vec<u8> {
        serialize_resp(RespValue::Array(Some(Vec::new())))
    }
    if let (
        Some(RespValue::BulkString(Some(key))),
        Some(RespValue::BulkString(Some(start))),
        Some(RespValue::BulkString(Some(stop))),
    ) = (args.get(1), args.get(2), args.get(3))
    {
        let mut start = start.parse::<i64>().map_err(|e| {
            eprintln!("解析start参数失败: {}", e);
            "ERR invalid range start".to_string()
        })?;
        let mut stop = stop.parse::<i64>().map_err(|e| {
            eprintln!("解析stop参数失败: {}", e);
            "ERR invalid range stop".to_string()
        })?;
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
            let response = serialize_resp(RespValue::Array(Some(response)));
            Ok(response)
        } else {
            // 键不存在或不是列表，返回空数组
            Ok(serialize_resp(RespValue::Array(Some(Vec::new()))))
        }
    } else {
        Ok(serialize_resp(RespValue::Error(
            "ERR wrong number of arguments for 'lrange' command".to_string(),
        )))
    }
}

pub fn handle_lpush(
    args: &[RespValue],
    db: &mut tokio::sync::MutexGuard<'_, crate::storage::DatabaseInner>,
) -> Result<Vec<u8>, String> {
    if let Some(RespValue::BulkString(Some(key))) = args.get(1) {
        // 收集所有值
        let mut values = Vec::new();
        for arg in args.iter().skip(2) {
            if let RespValue::BulkString(Some(value)) = arg {
                values.push(value.clone());
            }
        }

        // 先检查是否有阻塞的客户端等待这个列表
        if let Some(blocked_client) = db.blocked_clients.pop_client(key) {
            // 通知阻塞的客户端，不添加元素到列表
            let response = serialize_resp(RespValue::Array(Some(vec![
                RespValue::BulkString(Some(key.clone())),
                RespValue::BulkString(Some(values[0].clone())),
            ])));
            let _ = blocked_client.tx.send(response);
            let response = serialize_resp(RespValue::Integer(1));
            return Ok(response);
        }

        // 更新数据库
        let mut list = if let Some(entry) = db.data.get(key) {
            if !is_expired(&entry.expiry) {
                match &entry.value {
                    RedisValue::List(existing_list) => existing_list.clone(),
                    _ => {
                        // 如果键存在但不是列表，返回错误
                        return Ok(serialize_resp(RespValue::Error(
                            "WRONGTYPE Operation against a key holding the wrong kind of value"
                                .to_string(),
                        )));
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

        let response = serialize_resp(RespValue::Integer(list_len));
        Ok(response)
    } else {
        Ok(serialize_resp(RespValue::Error(
            "ERR wrong number of arguments for 'lpush' command".to_string(),
        )))
    }
}
pub fn handle_llen(
    args: &[RespValue],
    db: &mut tokio::sync::MutexGuard<'_, crate::storage::DatabaseInner>,
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
    db: &mut tokio::sync::MutexGuard<'_, crate::storage::DatabaseInner>,
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
                        return Ok(serialize_resp(RespValue::Error(
                            "WRONGTYPE Operation against a key holding the wrong kind of value"
                                .to_string(),
                        )));
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
                        let response = serialize_resp(RespValue::Array(Some(pop_list)));
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
pub fn handle_type(
    a: &[RespValue],
    db: &mut tokio::sync::MutexGuard<'_, crate::storage::DatabaseInner>,
) -> Result<Vec<u8>, String> {
    let response = if let Some(RespValue::BulkString(Some(key))) = a.get(1) {
        if let Some(entry) = db.data.get(key) {
            if is_expired(&entry.expiry) {
                // 键已过期，视为不存在
                serialize_resp(RespValue::SimpleString("none".to_string()))
            } else {
                match entry.value {
                    RedisValue::String(_) => {
                        serialize_resp(RespValue::SimpleString("string".to_string()))
                    }
                    RedisValue::Stream(_) => {
                        serialize_resp(RespValue::SimpleString("stream".to_string()))
                    }
                    _ => serialize_resp(RespValue::SimpleString("none".to_string())),
                }
            }
        } else {
            serialize_resp(RespValue::SimpleString("none".to_string()))
        }
    } else {
        serialize_resp(RespValue::Error("Please provide a key".to_string()))
    };
    Ok(response)
}
pub fn handle_xadd(
    a: &[RespValue],
    db: &mut tokio::sync::MutexGuard<'_, crate::storage::DatabaseInner>,
) -> Result<Vec<u8>, String> {
    // 内部函数用于处理xadd第3参数后面的一系列键值对，返回StreamEntry的fields
    fn handle_xadd_fields(a: &[RespValue]) -> Vec<HashMap<String, String>> {
        let mut fields: Vec<HashMap<String, String>> = Vec::new();
        let mut field_entry: HashMap<String, String> = HashMap::new();
        let range = (a.len() - 3).div_ceil(2);
        for i in 1..range + 1 {
            if let (
                Some(RespValue::BulkString(Some(key))),
                Some(RespValue::BulkString(Some(value))),
            ) = (a.get(2 * i + 1), a.get(2 * i + 2))
            {
                let key = key.clone().to_string();
                let value = value.clone().to_string();
                field_entry.insert(key, value);
            }
        }
        fields.push(field_entry);
        fields
    }
    if let (
        Some(RespValue::BulkString(Some(stream_key))),
        Some(RespValue::BulkString(Some(stream_id))),
    ) = (a.get(1), a.get(2))
    {
        // 成功获取流键和流ID
        // 检查流键是否存在且未过期
        if let Some(entry) = db.data.get(stream_key) {
            // db 中存在该键
            if is_expired(&entry.expiry) {
                // 键已过期，视为不存在
                // 处理流 ID
                match process_stream_id(stream_id, &[]) {
                    Ok(processed_id) => {
                        // 往流中添加新元素
                        let fields = handle_xadd_fields(a);
                        let the_stream: Vec<crate::storage::StreamEntry> =
                            vec![crate::storage::StreamEntry {
                                id: processed_id.clone(),
                                fields,
                            }];
                        // 存储回数据库
                        db.data.insert(
                            stream_key.clone(),
                            ValueWithExpiry {
                                value: RedisValue::Stream(the_stream),
                                expiry: None, // 可以根据需要支持过期时间
                            },
                        );
                        Ok(serialize_resp(RespValue::BulkString(Some(processed_id))))
                    }
                    Err(error) => {
                        // 返回错误
                        Ok(serialize_resp(RespValue::Error(error)))
                    }
                }
            } else {
                // 键未过期
                // 先获取当前流的所有元素
                let mut current_stream = match entry.value {
                    RedisValue::Stream(ref stream) => stream.clone(),
                    _ => Vec::new(),
                };
                // 处理流 ID
                match process_stream_id(stream_id, &current_stream) {
                    Ok(processed_id) => {
                        // 更新当前流元素列表
                        current_stream.push(crate::storage::StreamEntry {
                            id: processed_id.clone(),
                            fields: handle_xadd_fields(a),
                        });
                        // 存储回数据库
                        db.data.insert(
                            stream_key.clone(),
                            ValueWithExpiry {
                                value: RedisValue::Stream(current_stream.clone()),
                                expiry: None, // 可以根据需要支持过期时间
                            },
                        );

                        // 检查是否有阻塞的客户端等待这个流
                        // 注意：由于oneshot::Sender的特性，我们需要先移除客户端再发送通知
                        if db.blocked_clients.clients.contains_key(stream_key) {
                            let mut clients_to_process = Vec::new();

                            // 先将所有阻塞的客户端移到临时列表中
                            if let Some(clients) = db.blocked_clients.clients.remove(stream_key) {
                                for client in clients {
                                    // 检查新条目的ID是否大于客户端等待的ID
                                    if crate::stream_id::is_id_greater(
                                        &processed_id,
                                        &client.last_id,
                                    ) {
                                        // 构建响应
                                        let mut stream_result = Vec::new();
                                        for stream_entry in &current_stream {
                                            if crate::stream_id::is_id_greater(
                                                &stream_entry.id,
                                                &client.last_id,
                                            ) {
                                                let mut fields_array = Vec::new();
                                                for field_map in &stream_entry.fields {
                                                    for (k, v) in field_map {
                                                        fields_array.push(RespValue::BulkString(
                                                            Some(k.clone()),
                                                        ));
                                                        fields_array.push(RespValue::BulkString(
                                                            Some(v.clone()),
                                                        ));
                                                    }
                                                }

                                                let entry_array = vec![
                                                    RespValue::BulkString(Some(
                                                        stream_entry.id.clone(),
                                                    )),
                                                    RespValue::Array(Some(fields_array)),
                                                ];
                                                stream_result
                                                    .push(RespValue::Array(Some(entry_array)));
                                            }
                                        }

                                        let stream_array = vec![
                                            RespValue::BulkString(Some(stream_key.clone())),
                                            RespValue::Array(Some(stream_result)),
                                        ];

                                        let response =
                                            serialize_resp(RespValue::Array(Some(vec![
                                                RespValue::Array(Some(stream_array)),
                                            ])));

                                        // 通知客户端
                                        let _ = client.tx.send(response);
                                    } else {
                                        // 没有新数据，将客户端放回阻塞列表
                                        clients_to_process.push(client);
                                    }
                                }
                            }

                            // 将未通知的客户端放回阻塞列表
                            if !clients_to_process.is_empty() {
                                db.blocked_clients
                                    .clients
                                    .insert(stream_key.clone(), clients_to_process);
                            }
                        }

                        Ok(serialize_resp(RespValue::BulkString(Some(processed_id))))
                    }
                    Err(error) => {
                        // 返回错误
                        Ok(serialize_resp(RespValue::Error(error)))
                    }
                }
            }
        } else {
            // db 中不存在该键
            // 处理流 ID
            match process_stream_id(stream_id, &[]) {
                Ok(processed_id) => {
                    // 往流中添加新元素
                    let fields = handle_xadd_fields(a);
                    let the_stream: Vec<crate::storage::StreamEntry> =
                        vec![crate::storage::StreamEntry {
                            id: processed_id.clone(),
                            fields,
                        }];
                    // 存储回数据库
                    db.data.insert(
                        stream_key.clone(),
                        ValueWithExpiry {
                            value: RedisValue::Stream(the_stream),
                            expiry: None, // 可以根据需要支持过期时间
                        },
                    );
                    Ok(serialize_resp(RespValue::BulkString(Some(processed_id))))
                }
                Err(error) => {
                    // 返回错误
                    Ok(serialize_resp(RespValue::Error(error)))
                }
            }
        }
    } else {
        // 未成功获取流键和流ID
        Ok(serialize_resp(RespValue::Error(
            "Please provide a key and stream id".to_string(),
        )))
    }
}

pub fn handle_xrange(
    args: &[RespValue],
    db: &mut tokio::sync::MutexGuard<'_, crate::storage::DatabaseInner>,
) -> Result<Vec<u8>, String> {
    if let (
        Some(RespValue::BulkString(Some(key))),
        Some(RespValue::BulkString(Some(start))),
        Some(RespValue::BulkString(Some(end))),
    ) = (args.get(1), args.get(2), args.get(3))
    {
        // 检查键是否存在且未过期
        if let Some(entry) = db.data.get(key) {
            if !is_expired(&entry.expiry) {
                match &entry.value {
                    RedisValue::Stream(stream) => {
                        // 处理 start 和 end ID
                        let start_id = start.to_string();
                        let end_id = end.to_string();

                        // 过滤出符合范围的条目
                        let mut result = Vec::new();
                        for stream_entry in stream {
                            let entry_id = &stream_entry.id;
                            if is_id_in_range(entry_id, &start_id, &end_id) {
                                // 构建返回格式
                                let mut fields_array = Vec::new();
                                for field_map in &stream_entry.fields {
                                    for (k, v) in field_map {
                                        fields_array.push(RespValue::BulkString(Some(k.clone())));
                                        fields_array.push(RespValue::BulkString(Some(v.clone())));
                                    }
                                }

                                let entry_array = vec![
                                    RespValue::BulkString(Some(entry_id.clone())),
                                    RespValue::Array(Some(fields_array)),
                                ];
                                result.push(RespValue::Array(Some(entry_array)));
                            }
                        }

                        let response = serialize_resp(RespValue::Array(Some(result)));
                        Ok(response)
                    }
                    _ => Ok(serialize_resp(RespValue::Error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value"
                            .to_string(),
                    ))),
                }
            } else {
                // 键已过期，视为不存在
                let response = serialize_resp(RespValue::Array(Some(Vec::new())));
                Ok(response)
            }
        } else {
            // 键不存在
            let response = serialize_resp(RespValue::Array(Some(Vec::new())));
            Ok(response)
        }
    } else {
        Ok(serialize_resp(RespValue::Error(
            "ERR wrong number of arguments for 'xrange' command".to_string(),
        )))
    }
}

pub fn handle_incr(
    args: &[RespValue],
    db: &mut tokio::sync::MutexGuard<'_, crate::storage::DatabaseInner>,
) -> Result<Vec<u8>, String> {
    if let (Some(RespValue::BulkString(Some(key))),) = (args.get(1),) {
        // 检查键是否存在且未过期
        if let Some(entry) = db.data.get(key) {
            if !is_expired(&entry.expiry) {
                match &entry.value {
                    RedisValue::String(current_string_value) => {
                        // 尝试解析字符串为整数
                        let current_value =
                            if let Ok(int_value) = current_string_value.parse::<i64>() {
                                int_value + 1
                            } else {
                                // 解析失败，视为不存在
                                return Ok(serialize_resp(RespValue::Error(
                                    "ERR value is not an integer or out of range".to_string(),
                                )));
                            };

                        // 创建一个新的整数键
                        let new_entry = ValueWithExpiry {
                            value: RedisValue::String(current_value.to_string()),
                            expiry: entry.expiry.clone(),
                        };
                        // 更新数据库
                        db.data.insert(key.clone(), new_entry);
                        // 返回当前值
                        let response = serialize_resp(RespValue::Integer(current_value));
                        Ok(response)
                    }
                    // 应该执行不到该情况，因为set命令会将键值对设置为string类型进行存储
                    RedisValue::Integer(current_value) => {
                        let current_value = *current_value + 1;
                        // 创建一个新的整数键
                        let new_entry = ValueWithExpiry {
                            value: RedisValue::String(current_value.to_string()),
                            expiry: entry.expiry.clone(),
                        };
                        // 更新数据库
                        db.data.insert(key.clone(), new_entry);
                        // 返回当前值
                        let response = serialize_resp(RespValue::Integer(current_value));
                        Ok(response)
                    }
                    _ => Ok(serialize_resp(RespValue::Error(
                        "ERR value is not an integer or out of range".to_string(),
                    ))),
                }
            } else {
                // 键已过期，视为不存在
                // 创建一个新的整数键
                let new_entry = ValueWithExpiry {
                    value: RedisValue::String(1.to_string()),
                    expiry: None,
                };
                db.data.insert(key.clone(), new_entry);
                // 返回新创建的键值对
                let response = serialize_resp(RespValue::Integer(1));
                Ok(response)
            }
        } else {
            // 键不存在
            // 创建一个新的整数键
            let new_entry = ValueWithExpiry {
                value: RedisValue::String(1.to_string()),
                expiry: None,
            };
            db.data.insert(key.clone(), new_entry);
            // 返回新创建的键值对
            let response = serialize_resp(RespValue::Integer(1));
            Ok(response)
        }
    } else {
        Ok(serialize_resp(RespValue::Error(
            "ERR command incr: key not found".to_string(),
        )))
    }
}
pub fn handle_unwatch(watched_keys: &mut Vec<String>, dirty: &mut bool) -> Result<Vec<u8>, String> {
    // 清除所有监视的键
    watched_keys.clear();
    *dirty = false;
    Ok(serialize_resp(RespValue::SimpleString("OK".to_string())))
}

pub fn handle_watch(
    db: &mut tokio::sync::MutexGuard<'_, crate::storage::DatabaseInner>,
    in_transaction: &mut bool,
    watched_keys: &mut Vec<String>,
    dirty: &mut bool,
    a: &Vec<RespValue>,
) -> Result<Vec<u8>, String> {
    if *in_transaction {
        // 在事务内调用WATCH，返回错误
        Ok(serialize_resp(RespValue::Error(
            "ERR WATCH inside MULTI is not allowed".to_string(),
        )))
    } else {
        // 解析参数，添加键到监视集合
        for i in 1..a.len() {
            if let Some(RespValue::BulkString(Some(key))) = a.get(i) {
                watched_keys.push(key.clone());
                // 从dirty_keys中移除被监视的键
                // 这样WATCH命令只监视在它之后被修改的键
                db.dirty_keys.remove(key);
            }
        }
        // 重置dirty标记
        *dirty = false;
        Ok(serialize_resp(RespValue::SimpleString("OK".to_string())))
    }
}

pub fn handle_discard(
    in_transaction: &mut bool,
    command_queue: &mut Vec<Vec<u8>>,
    watched_keys: &mut Vec<String>,
    dirty: &mut bool,
) -> Result<Vec<u8>, String> {
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

pub async fn handle_exec(
    db: &mut tokio::sync::MutexGuard<'_, crate::storage::DatabaseInner>,
    in_transaction: &mut bool,
    command_queue: &mut Vec<Vec<u8>>,
    watched_keys: &mut Vec<String>,
    dirty: &mut bool,
    app_state: &Arc<crate::storage::AppState>,
) -> Result<Vec<u8>, String> {
    if !*in_transaction {
        Ok(serialize_resp(RespValue::Error(
            "ERR EXEC without MULTI".to_string(),
        )))
    } else {
        *in_transaction = false;
        // 重置dirty标记
        *dirty = false;
        // 检查被监视的键是否在dirty_keys中
        println!("DEBUG: watched_keys = {:?}", watched_keys);
        println!("DEBUG: dirty_keys = {:?}", db.dirty_keys);
        for key in &*watched_keys {
            if db.dirty_keys.contains(key) {
                println!("DEBUG: Key {} found in dirty_keys", key);
                *dirty = true;
                break;
            }
        }
        println!("DEBUG: dirty = {}", *dirty);
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
                // 直接解析并执行命令，避免递归调用
                if let Ok((resp, _)) = crate::utils::resp::deserialize_resp(&cmd_data) {
                    if let crate::utils::resp::RespValue::Array(Some(a)) = resp {
                        if let Some(crate::utils::resp::RespValue::BulkString(Some(cmd))) =
                            a.first()
                        {
                            let cmd_upper = to_uppercase(cmd);
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
                                "INFO" => handle_info(&a, &app_state.config),
                                "REPLCONF" => handle_replconf(&a, &app_state.config),
                                "PSYNC" => handle_psync(&a, &app_state.config, db),
                                "WAIT" => handle_wait_async(&a, app_state).await,
                                _ => Ok(serialize_resp(crate::utils::resp::RespValue::Error(
                                    "ERR unknown command".to_string(),
                                ))),
                            };
                            // 检查是否是修改键的命令
                            if let Some(crate::utils::resp::RespValue::BulkString(Some(cmd))) =
                                a.first()
                            {
                                let cmd_upper = to_uppercase(cmd);
                                match cmd_upper.as_str() {
                                    "SET" | "RPUSH" | "LPUSH" | "LPOP" | "XADD" | "INCR" => {
                                        // 提取键并添加到dirty_keys
                                        if let Some(crate::utils::resp::RespValue::BulkString(
                                            Some(key),
                                        )) = a.get(1)
                                        {
                                            db.dirty_keys.insert(key.clone());
                                        }
                                    }
                                    _ => {}
                                }
                            }
                            match result {
                                Ok(r) => responses.push(r),
                                Err(_) => responses.push(serialize_resp(
                                    crate::utils::resp::RespValue::Error(
                                        "ERR command execution failed".to_string(),
                                    ),
                                )),
                            }
                        }
                    }
                }
            }
            // 清除监视状态
            watched_keys.clear();
            *dirty = false;
            // 清空dirty_keys集合
            db.dirty_keys.clear();
            // 构建响应数组
            let mut result = Vec::new();
            result.extend(format!("*{}\r\n", responses.len()).as_bytes());
            for resp in responses {
                result.extend(resp);
            }
            Ok(result)
        }
    }
}

pub fn handle_multi(
    in_transaction: &mut bool,
    command_queue: &mut Vec<Vec<u8>>,
) -> Result<Vec<u8>, String> {
    *in_transaction = true;
    command_queue.clear();
    Ok(serialize_resp(RespValue::SimpleString("OK".to_string())))
}

pub fn handle_info(
    args: &[RespValue],
    config: &Arc<Mutex<crate::storage::Config>>,
) -> Result<Vec<u8>, String> {
    // 检查是否有参数
    if let Some(RespValue::BulkString(Some(section))) = args.get(1) {
        let section_upper = to_uppercase(section);
        match section_upper.as_str() {
            "REPLICATION" => {
                // 从config中获取复制角色信息
                let config_guard = config.lock().unwrap();
                let replicaof = &config_guard.replicaof;
                let master_replid = &config_guard.master_replid;
                let master_repl_offset = config_guard.master_repl_offset;
                let role = match replicaof {
                    crate::storage::ReplicaofRole::Master => "master",
                    crate::storage::ReplicaofRole::Slave(_, _) => "slave",
                };
                // 只返回replication部分的信息
                // role:master
                // master_replid：一个40个字符的字母数字字符串
                // master_repl_offset:0
                let info = format!(
                    "role:{}\r\nmaster_replid:{}\r\nmaster_repl_offset:{}\r\n",
                    role, master_replid, master_repl_offset
                );
                let response = serialize_resp(RespValue::BulkString(Some(info)));
                Ok(response)
            }
            _ => {
                // 其他部分暂时不支持
                let info = "";
                let response = serialize_resp(RespValue::BulkString(Some(info.to_string())));
                Ok(response)
            }
        }
    } else {
        // 没有参数，返回所有信息（暂时只返回replication部分）
        let config_guard = config.lock().unwrap();
        let role = match config_guard.replicaof {
            crate::storage::ReplicaofRole::Master => "master",
            crate::storage::ReplicaofRole::Slave(_, _) => "slave",
        };
        let info = format!("# Replication\r\nrole:{}\r\n", role);
        let response = serialize_resp(RespValue::BulkString(Some(info)));
        Ok(response)
    }
}
/*REPLCONF
该命令用于配置连接的副本。在收到对 的响应后，副本会向主控发送两个命令：REPLCONFPINGREPLCONF

REPLCONF listening-port <PORT>： 这会告诉主控复制品正在监听哪个端口。该值用于监控和日志记录，而非复制本身。
REPLCONF capa psync2：这会通知主人复制品的能力。
capa代表“能力”（capabilities）。它表明下一个参数是复制品支持的一个特征。
psync2表示副本支持 PSYNC2 协议的信号。PSYNC2 是用于将副本与主体重新同步的部分同步功能的改进版。
你现在可以安全地硬编码。capa psync2
这两个命令都应以RESP数组形式发送，因此具体字节大小如下：

# REPLCONF listening-port <PORT>
*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n6380\r\n

# REPLCONF capa psync2
*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n
对于这两个命令，主控都会响应 。那就是编码成简单字符串的字符串。+OK\r\nOK

REPLCONF GETACK *    # expect: REPLCONF ACK 0

PING                 # replica processes silently
REPLCONF GETACK *    # expect: REPLCONF ACK 51
# 51 = 37 (first REPLCONF) + 14 (PING)

SET foo 1             # replica processes silently
SET bar 2             # replica processes silently
REPLCONF GETACK *    # expect: REPLCONF ACK 146
# 146 = 51 + 37 (second REPLCONF) + 29 (SET foo) + 29 (SET bar)*/
pub fn handle_replconf(
    args: &[RespValue],
    config: &Mutex<crate::storage::Config>,
) -> Result<Vec<u8>, String> {
    // 检查是否有参数
    if let (Some(RespValue::BulkString(Some(arg_1))), Some(RespValue::BulkString(Some(arg_2)))) =
        (args.get(1), args.get(2))
    {
        let arg_1 = to_uppercase(arg_1);
        let arg_2 = to_uppercase(arg_2);
        match arg_1.as_str() {
            // REPLCONF listening-port <port> - 通知主节点从节点的监听端口
            "LISTENING-PORT" => {
                let _port: u16 = if let Some(RespValue::BulkString(Some(port))) = args.get(2) {
                    port.parse::<u16>().expect("port must be a right number")
                } else {
                    return Ok(serialize_resp(RespValue::Error(
                        "port is required".to_string(),
                    )));
                };
                let info = format!("OK");
                let response = serialize_resp(RespValue::SimpleString(info));
                Ok(response)
            }
            // REPLCONF capa psync2 - 通知主节点从节点支持PSYNC2协议（增量复制）
            "CAPA" => {
                if let Some(RespValue::BulkString(Some(arg_2))) = args.get(2) {
                    let arg_2 = to_uppercase(arg_2);
                    match arg_2.as_str() {
                        "PSYNC2" => {
                            // 支持PSYNC2协议
                            let info = format!("OK");
                            let response = serialize_resp(RespValue::SimpleString(info));
                            return Ok(response);
                        }
                        _ => {
                            return Ok(serialize_resp(RespValue::Error(
                                "capa must be psync2".to_string(),
                            )));
                        }
                    }
                } else {
                    return Ok(serialize_resp(RespValue::Error(
                        "capa require second parameter".to_string(),
                    )));
                }
            }
            "GETACK" => {
                println!(
                    "handle_replconf: Processing GETACK command, arg_2 = {}",
                    arg_2
                );
                let config_guard = config.lock().unwrap();
                let offset = config_guard.master_repl_offset;
                println!("handle_replconf: current master_repl_offset = {}", offset);
                let offset_str = offset.to_string();
                let info = match arg_2.as_str() {
                    "*" => {
                        println!(
                            "handle_replconf: Building ACK response with offset = {}",
                            offset_str
                        );
                        RespValue::Array(Some(vec![
                            RespValue::BulkString(Some("REPLCONF".to_string())),
                            RespValue::BulkString(Some("ACK".to_string())),
                            RespValue::BulkString(Some(offset_str)),
                        ]))
                    }
                    _ => RespValue::Error("getack must have parameter after".to_string()),
                };
                let response = serialize_resp(info);
                println!(
                    "handle_replconf: Sending response: {:?}",
                    String::from_utf8_lossy(&response)
                );
                Ok(response)
            }
            _ => {
                // 其他参数暂时不支持
                let info = "";
                let response = serialize_resp(RespValue::BulkString(Some(info.to_string())));
                Ok(response)
            }
        }
    } else {
        let info = format!("REPLCONF need parameter");
        let response = serialize_resp(RespValue::BulkString(Some(info)));
        Ok(response)
    }
}
pub fn handle_psync(
    args: &[RespValue],
    config: &Mutex<crate::storage::Config>,
    db: &mut tokio::sync::MutexGuard<'_, crate::storage::DatabaseInner>,
) -> Result<Vec<u8>, String> {
    if let (
        Some(RespValue::BulkString(Some(replication_id))),
        Some(RespValue::BulkString(Some(offset))),
    ) = (args.get(1), args.get(2))
    {
        let config_guard = config.lock().unwrap();
        match config_guard.replicaof {
            crate::storage::ReplicaofRole::Master => {
                match (replication_id.as_str(), offset.as_str()) {
                    ("?", "-1") => {
                        // +FULLRESYNC <REPL_ID> 0\r\n
                        let info = format!(
                            "FULLRESYNC {} {}",
                            &config_guard.master_replid, &config_guard.master_repl_offset
                        );
                        let mut response = serialize_resp(RespValue::SimpleString(info));
                        // 主控的反应分为两个步骤：
                        // 它通过回复确认（之前阶段已处理）FULLRESYNC
                        // 它以 RDB 文件的形式发送当前状态的快照。
                        response.append(&mut db.transport_binary_by_rdb());
                        Ok(response)
                    }
                    _ => {
                        let info = format!("psync need parameter");
                        let response = serialize_resp(RespValue::Error(info));
                        Ok(response)
                    }
                }
            }
            crate::storage::ReplicaofRole::Slave(_, _) => {
                let info = format!("temp don't support slave psync");
                let response = serialize_resp(RespValue::Error(info));
                Ok(response)
            }
        }
    } else {
        let info = format!("psync need parameter");
        let response = serialize_resp(RespValue::Error(info));
        Ok(response)
    }
}
pub async fn handle_wait_async(
    args: &[RespValue],
    app_state: &Arc<crate::storage::AppState>,
) -> Result<Vec<u8>, String> {
    let (
        Some(RespValue::BulkString(Some(numreplicas))),
        Some(RespValue::BulkString(Some(timeout_str))),
    ) = (args.get(1), args.get(2))
    else {
        return Ok(serialize_resp(RespValue::Error(
            "WAIT requires two arguments".to_string(),
        )));
    };

    let numreplicas: usize = match numreplicas.parse() {
        Ok(n) => n,
        Err(_) => {
            return Ok(serialize_resp(RespValue::Error(
                "WAIT: numreplicas must be a number".to_string(),
            )));
        }
    };

    let timeout_ms: u64 = match timeout_str.parse() {
        Ok(n) => n,
        Err(_) => {
            return Ok(serialize_resp(RespValue::Error(
                "WAIT: timeout must be a number".to_string(),
            )));
        }
    };

    if numreplicas == 0 {
        return Ok(serialize_resp(RespValue::Integer(0)));
    }

    let master_offset = {
        let config_guard = app_state.config.lock().unwrap();
        config_guard.master_repl_offset
    };

    let replicas = {
        let replicas_guard = app_state.replicas.lock().unwrap();
        replicas_guard.clone()
    };

    let replica_count = replicas.len();
    if replica_count == 0 {
        return Ok(serialize_resp(RespValue::Integer(0)));
    }

    // 如果没有待复制的命令，所有副本都是最新的，直接返回副本数量
    if master_offset == 0 {
        return Ok(serialize_resp(RespValue::Integer(replica_count as i64)));
    }

    let getack_cmd = serialize_resp(RespValue::Array(Some(vec![
        RespValue::BulkString(Some("REPLCONF".to_string())),
        RespValue::BulkString(Some("GETACK".to_string())),
        RespValue::BulkString(Some("*".to_string())),
    ])));

    for replica_write in &replicas {
        let cmd = getack_cmd.clone();
        let mut write_half = replica_write.lock().await;
        if let Err(e) = write_half.write_all(&cmd).await {
            eprintln!("Error sending GETACK to replica: {}", e);
        }
    }

    let mut rx = {
        let tx_option = app_state.wait_acks_tx.lock().unwrap();
        let tx = tx_option.as_ref().ok_or("wait_acks_tx not initialized")?;
        tx.subscribe()
    };

    let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_millis(timeout_ms);
    let mut acked_count = 0usize;

    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            break;
        }

        match tokio::time::timeout(remaining, rx.recv()).await {
            Ok(Ok(ack_offset)) => {
                if ack_offset >= master_offset {
                    acked_count += 1;
                    if acked_count >= numreplicas {
                        break;
                    }
                }
            }
            Ok(Err(_)) => {
                break;
            }
            Err(_) => {
                break;
            }
        }
    }

    Ok(serialize_resp(RespValue::Integer(acked_count as i64)))
}
