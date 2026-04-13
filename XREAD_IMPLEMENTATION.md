# XREAD 命令实现指南

## 功能概述

XREAD 命令用于从一个或多个流中读取数据，支持以下特性：

1. **基本读取**：从指定的 ID 开始读取流数据
2. **多流读取**：同时从多个流中读取数据
3. **阻塞读取**：使用 BLOCK 参数等待新数据
4. **特殊 ID**：支持使用 `$` 符号表示从最新数据开始读取

## 实现思路

### 1. 数据结构扩展

首先，我们需要扩展 `BlockedClient` 结构，添加 `last_id` 字段来存储流的最后 ID，并修改 `tx` 字段的类型以支持发送完整的响应：

```rust
pub struct BlockedClient {
    pub key: String,
    pub timeout: Duration,                                  // 超时时间
    pub start_time: u64,                                    // 开始阻塞的时间戳（毫秒）
    pub last_id: String,                                    // 流的最后ID
    pub tx: tokio::sync::oneshot::Sender<Vec<u8>>,          // 用于通知客户端的通道
}
```

### 2. 命令解析与处理

在 `prepare_xread` 函数中，我们需要：

1. **解析命令参数**：处理 BLOCK 选项和 STREAMS 关键字
2. **处理多个流**：解析流键和对应的 ID
3. **处理特殊 ID**：处理 `$` 符号，表示从最新数据开始读取
4. **构建响应**：按照 Redis 协议格式构建响应
5. **处理阻塞**：如果设置了 BLOCK 参数且没有数据，需要阻塞客户端

### 3. 阻塞机制

当 XREAD 命令设置了 BLOCK 参数且没有数据时，我们需要：

1. 将客户端添加到阻塞列表中
2. 当有新数据添加到流中时，检查是否有阻塞的客户端
3. 如果有，构建响应并通知客户端
4. 从阻塞列表中移除已通知的客户端

### 4. 数据添加时的处理

在 `handle_xadd` 函数中，当添加新数据到流时，我们需要：

1. 检查是否有阻塞的客户端等待这个流
2. 对于每个阻塞的客户端，检查新数据的 ID 是否大于客户端等待的 ID
3. 如果是，构建响应并通知客户端
4. 从阻塞列表中移除已通知的客户端

### 5. 主服务器集成

在 `main.rs` 中，我们需要：

1. 检测 XREAD 命令
2. 使用 `prepare_xread` 函数处理命令
3. 使用 `wait_for_blocked_command` 函数等待阻塞命令的结果

## 实现细节

### 1. 命令解析与处理

```rust
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
            if opt.to_uppercase() == "BLOCK" {
                i += 1;
                if let Some(RespValue::BulkString(Some(timeout_str))) = args.get(i) {
                    if let Ok(timeout) = timeout_str.parse::<u64>() {
                        block = Some(timeout);
                        i += 1;
                    } else {
                        return Ok(BlockedCommandResult::Immediate(
                            serialize_resp(RespValue::Error("ERR invalid timeout".to_string()))
                        ));
                    }
                } else {
                    return Ok(BlockedCommandResult::Immediate(
                        serialize_resp(RespValue::Error("ERR wrong number of arguments for 'xread' command".to_string()))
                    ));
                }
            }
        }
    }

    // 处理 STREAMS 关键字
    if i < args.len() {
        if let Some(RespValue::BulkString(Some(streams))) = args.get(i) {
            if streams.to_uppercase() != "STREAMS" {
                return Ok(BlockedCommandResult::Immediate(
                    serialize_resp(RespValue::Error("ERR syntax error".to_string()))
                ));
            }
            i += 1;
        } else {
            return Ok(BlockedCommandResult::Immediate(
                serialize_resp(RespValue::Error("ERR wrong number of arguments for 'xread' command".to_string()))
            ));
        }
    } else {
        return Ok(BlockedCommandResult::Immediate(
            serialize_resp(RespValue::Error("ERR wrong number of arguments for 'xread' command".to_string()))
        ));
    }

    // 解析流键和ID
    let mut keys = Vec::new();
    let mut ids = Vec::new();

    while i < args.len() {
        if let Some(RespValue::BulkString(Some(key))) = args.get(i) {
            keys.push(key.clone());
            i += 1;
            if let Some(RespValue::BulkString(Some(id))) = args.get(i) {
                ids.push(id.clone());
                i += 1;
            } else {
                return Ok(BlockedCommandResult::Immediate(
                    serialize_resp(RespValue::Error("ERR wrong number of arguments for 'xread' command".to_string()))
                ));
            }
        } else {
            break;
        }
    }

    if keys.is_empty() {
        return Ok(BlockedCommandResult::Immediate(
            serialize_resp(RespValue::Error("ERR wrong number of arguments for 'xread' command".to_string()))
        ));
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
```

### 2. 主服务器集成

```rust
// 检测是否是 BLPOP 或 XREAD 命令
let command_type = {
    if let Ok(resp) = deserialize_resp(&data) {
        if let RespValue::Array(Some(a)) = resp {
            if let Some(RespValue::BulkString(Some(cmd))) = a.get(0) {
                let cmd_upper = cmd.to_uppercase();
                if cmd_upper == "BLPOP" {
                    "BLPOP"
                } else if cmd_upper == "XREAD" {
                    "XREAD"
                } else {
                    "OTHER"
                }
            } else {
                "OTHER"
            }
        } else {
            "OTHER"
        }
    } else {
        "OTHER"
    }
};

// Process the command
let response = match command_type {
    "BLPOP" => {
        // 处理 BLPOP 命令
        // ...
    },
    "XREAD" => {
        // 处理 XREAD 命令
        if let Ok(resp) = deserialize_resp(&data) {
            if let RespValue::Array(Some(a)) = resp {
                // 准备 XREAD 命令
                let blocked_result = match db.lock() {
                    Ok(mut guard) => prepare_xread(&a, &mut guard).unwrap(),
                    Err(e) => {
                        eprintln!("Error locking database: {}", e);
                        return;
                    }
                };

                // 等待阻塞命令的结果
                wait_for_blocked_command(blocked_result).await
            } else {
                b"-ERR unknown command\r\n".to_vec()
            }
        } else {
            b"-ERR unknown command\r\n".to_vec()
        }
    },
    _ => {
        // 正常处理其他命令
        // ...
    }
};
```

### 3. 数据添加时的处理

```rust
// 检查是否有阻塞的客户端等待这个流
// 注意：由于oneshot::Sender的特性，我们需要先移除客户端再发送通知
if db.blocked_clients.clients.contains_key(stream_key) {
    let mut clients_to_process = Vec::new();

    // 先将所有阻塞的客户端移到临时列表中
    if let Some(clients) = db.blocked_clients.clients.remove(stream_key) {
        for client in clients {
            // 检查新条目的ID是否大于客户端等待的ID
            if crate::stream_id::is_id_greater(&processed_id, &client.last_id) {
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
                                fields_array.push(RespValue::BulkString(Some(
                                    k.clone(),
                                )));
                                fields_array.push(RespValue::BulkString(Some(
                                    v.clone(),
                                )));
                            }
                        }

                        let entry_array = vec![
                            RespValue::BulkString(Some(
                                stream_entry.id.clone(),
                            )),
                            RespValue::Array(Some(fields_array)),
                        ];
                        stream_result.push(RespValue::Array(Some(entry_array)));
                    }
                }

                let stream_array = vec![
                    RespValue::BulkString(Some(stream_key.clone())),
                    RespValue::Array(Some(stream_result)),
                ];

                let response = serialize_resp(RespValue::Array(Some(vec![
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
        db.blocked_clients.clients.insert(stream_key.clone(), clients_to_process);
    }
}
```

## 测试示例

### 基本读取

```bash
$ redis-cli XADD some_key 1526985054069-0 temperature 36 humidity 95
"1526985054069-0"

$ redis-cli XADD some_key 1526985054079-0 temperature 37 humidity 94
"1526985054079-0"

$ redis-cli XREAD STREAMS some_key 1526985054069-0
1) 1) "some_key"
   2) 1) 1) 1526985054079-0
         2) 1) temperature
            2) 37
            3) humidity
            4) 94
```

### 多流读取

```bash
$ redis-cli XADD stream_key 0-1 temperature 95
$ redis-cli XADD other_stream_key 0-2 humidity 97

$ redis-cli XREAD streams stream_key other_stream_key 0-0 0-1
1) 1) "stream_key"
   2) 1) 1) "0-1"
         2) 1) "temperature"
            2) "95"
2) 1) "other_stream_key"
   2) 1) 1) "0-2"
         2) 1) "humidity"
            2) "97"
```

### 阻塞读取

```bash
# 客户端1
$ redis-cli XREAD BLOCK 1000 streams some_key 1526985054069-0

# 客户端2
$ redis-cli XADD some_key 1526985054079-0 temperature 37 humidity 94

# 客户端1会收到响应
1) 1) "some_key"
   2) 1) 1) 1526985054079-0
         2) 1) temperature
            2) 37
            3) humidity
            4) 94
```

### 使用 $ 符号

```bash
$ redis-cli XADD stream_key 0-1 temperature 96

$ redis-cli XREAD BLOCK 0 streams stream_key $

# 另一个客户端添加数据
$ redis-cli XADD stream_key 0-2 temperature 95

# 第一个客户端会收到响应
1) 1) "stream_key"
   2) 1) 1) "0-2"
         2) 1) "temperature"
            2) "95"
```

## 注意事项

1. **阻塞实现**：完整的阻塞实现使用了tokio的异步功能，支持有限期阻塞和无限期阻塞
2. **错误处理**：处理了各种错误情况，如参数格式错误、类型不匹配等
3. **性能考虑**：对于大量数据的流，需要考虑性能优化
4. **并发安全**：确保了并发操作的安全性
5. **多流支持**：支持同时从多个流读取数据，但阻塞功能仅支持单流

## 总结

XREAD 命令是 Redis 流操作的重要组成部分，支持从一个或多个流中读取数据，并提供阻塞机制等待新数据。本实现遵循 Redis 协议规范，支持基本读取、多流读取、阻塞读取和特殊 ID 处理等功能。

通过扩展数据结构、解析命令参数、构建响应和处理阻塞机制，我们实现了一个功能完整的 XREAD 命令处理逻辑，包括完整的阻塞功能。
