# Redis事务实现思路与做法

## 项目概述

本项目实现了Redis事务相关的命令，包括`MULTI`、`EXEC`、`DISCARD`、`WATCH`和`UNWATCH`命令，以及乐观锁机制。

## 实现思路

1. **状态管理**：
   - 在每个客户端连接中维护事务状态（`in_transaction`）
   - 维护命令队列（`command_queue`）用于存储事务中的命令
   - 维护监视状态（`watched_keys`和`dirty`）用于实现乐观锁

2. **乐观锁实现**：
   - 使用`WATCH`命令监视键
   - 在数据库中维护`dirty_keys`集合，跟踪被修改的键
   - 在`EXEC`命令执行时，检查被监视的键是否被修改
   - 如果被修改，中止事务并返回空数组

3. **命令处理**：
   - `MULTI`：进入事务模式，清空命令队列
   - `EXEC`：执行队列中的命令，或在键被修改时中止事务
   - `DISCARD`：中止事务，清空命令队列和监视状态
   - `WATCH`：添加键到监视集合
   - `UNWATCH`：清空监视集合

## 具体实现

### 1. 监视状态管理

在`main.rs`中为每个客户端连接添加了以下状态：

```rust
// 监视状态管理
let mut watched_keys: Vec<String> = Vec::new();
let mut dirty = false;
```

### 2. 数据库结构扩展

在`db_storage.rs`中添加了`dirty_keys`集合，用于跟踪被修改的键：

```rust
pub struct DatabaseInner {
    pub data: HashMap<String, ValueWithExpiry>,
    pub blocked_clients: BlockedClients,
    pub dirty_keys: HashSet<String>,
}
```

### 3. WATCH命令实现

在`commands.rs`中实现了`WATCH`命令：

```rust
"WATCH" => {
    if *in_transaction {
        // 在事务内调用WATCH，返回错误
        Ok(serialize_resp(RespValue::Error("ERR WATCH inside MULTI is not allowed".to_string())))
    } else {
        // 解析参数，添加键到监视集合
        for i in 1..a.len() {
            if let RespValue::BulkString(Some(key)) = &a[i] {
                watched_keys.push(key.clone());
            }
        }
        Ok(serialize_resp(RespValue::SimpleString("OK".to_string())))
    }
}
```

### 4. UNWATCH命令实现

在`commands.rs`中实现了`UNWATCH`命令：

```rust
"UNWATCH" => {
    // 清除所有监视的键
    watched_keys.clear();
    *dirty = false;
    Ok(serialize_resp(RespValue::SimpleString("OK".to_string())))
}
```

### 5. EXEC命令实现

在`commands.rs`中修改了`EXEC`命令，添加了乐观锁检查：

```rust
"EXEC" => {
    if !*in_transaction {
        Ok(serialize_resp(RespValue::Error(
            "ERR EXEC without MULTI".to_string(),
        )))
    } else {
        *in_transaction = false;
        // 检查被监视的键是否在dirty_keys中
        for key in &*watched_keys {
            if db.dirty_keys.contains(key) {
                *dirty = true;
                break;
            }
        }
        // 检查dirty标记，如果为true则中止事务
        if *dirty {
            // 清除监视状态
            watched_keys.clear();
            *dirty = false;
            // 返回空数组
            Ok(serialize_resp(RespValue::Array(None)))
        } else {
            // 事务执行命令响应队列
            let mut responses = Vec::new();
            for cmd_data in command_queue.drain(..) {
                if let Ok(cmd_resp) = command_handler(
                    &cmd_data,
                    db,
                    &mut false,
                    &mut Vec::new(),
                    &mut Vec::new(),
                    &mut false,
                ) {
                    responses.push(cmd_resp);
                } else {
                    responses.push(serialize_resp(RespValue::Error(
                        "ERR command execution failed".to_string(),
                    )));
                }
            }
            // 清除监视状态
            watched_keys.clear();
            *dirty = false;
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
```

### 6. DISCARD命令实现

在`commands.rs`中修改了`DISCARD`命令，添加了监视状态清除：

```rust
"DISCARD" => {
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
```

### 7. 键修改跟踪

在`commands.rs`中添加了键修改跟踪逻辑：

```rust
// 检查是否是修改键的命令
if let Some(RespValue::BulkString(Some(cmd))) = a.first() {
    let cmd_upper = to_uppercase(cmd);
    match cmd_upper.as_str() {
        "SET" | "RPUSH" | "LPUSH" | "LPOP" | "XADD" | "INCR" => {
            // 提取键并添加到dirty_keys
            if let Some(RespValue::BulkString(Some(key))) = a.get(1) {
                db.dirty_keys.insert(key.clone());
            }
        }
        _ => {}
    }
}
```

## 测试方案

1. **基本命令测试**：
   - 测试`MULTI`命令
   - 测试`EXEC`命令
   - 测试`DISCARD`命令
   - 测试`WATCH`命令
   - 测试`UNWATCH`命令

2. **事务流程测试**：
   - 测试正常事务执行
   - 测试在事务内调用`WATCH`命令的错误处理
   - 测试被监视的键被修改时事务中止

3. **多键监视测试**：
   - 测试同时监视多个键
   - 测试其中一个键被修改时事务中止

4. **不存在键的监视测试**：
   - 测试监视不存在的键
   - 测试在监视后创建该键时事务中止

## 代码优化建议

1. **响应数组构建**：
   - 当前使用`format!`直接构建响应数组，可以改为使用`RespValue::Array`来构建，使代码更简洁。

2. **错误处理**：
   - 可以增加更多的错误处理和日志记录，提高代码的健壮性。

3. **性能优化**：
   - 对于大量键的监视，可以考虑使用更高效的数据结构，如哈希集合。

4. **代码模块化**：
   - 可以将事务相关的逻辑提取到单独的模块中，提高代码的可维护性。

## 总结

本实现完成了Redis事务相关命令的基本功能，包括：

- `MULTI`：进入事务模式
- `EXEC`：执行事务或在键被修改时中止
- `DISCARD`：中止事务
- `WATCH`：监视键以实现乐观锁
- `UNWATCH`：清除监视状态

实现了乐观锁机制，当被监视的键在事务执行前被修改时，事务会被中止，保证了事务的原子性和一致性。

该实现遵循了Redis的设计规范，使用了resp.rs来序列化和反序列化命令和响应，保持了与原代码风格的一致性。