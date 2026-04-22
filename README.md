# Redis Clone - Rust 实现

一个用 Rust 实现的 Redis 克隆版本，支持基本命令和主从复制功能。

## 功能列表

### 1. 字符串命令

| 命令   | 描述                     | 返回值示例                     |
| ------ | ------------------------ | ------------------------------ |
| `PING` | 返回 PONG                | `+PONG\r\n`                    |
| `ECHO` | 回显消息                 | `$3\r\nmsg\r\n`                |
| `SET`  | 设置键值对，支持过期时间 | `+OK\r\n`                      |
| `GET`  | 获取值（惰性删除过期键） | `$5\r\nvalue\r\n` 或 `$-1\r\n` |

**SET 命令实现逻辑：**

```Rust
// 1. 解析参数：key, value, [EX seconds | PX milliseconds]
// 2. 如果指定了过期时间，计算过期时间戳
// 3. 将键值对存储到 db.data HashMap 中
// 4. 返回 "+OK\r\n"
```

**GET 命令实现逻辑：**

```rust
// 1. 查找键是否存在
// 2. 如果存在，检查是否过期（惰性删除：只在访问时检查）
// 3. 如果已过期，删除键并返回 nil
// 4. 如果未过期，返回存储的值
```

### 2. 列表命令

| 命令     | 描述                       | 示例               |
| -------- | -------------------------- | ------------------ |
| `RPUSH`  | 从右端推入元素             | `RPUSH key v1 v2`  |
| `LPUSH`  | 从左端推入元素             | `LPUSH key v1 v2`  |
| `LRANGE` | 获取指定范围的元素         | `LRANGE key 0 -1`  |
| `LLEN`   | 获取列表长度               | `LLEN key`         |
| `LPOP`   | 从左端弹出元素（支持计数） | `LPOP key [count]` |

**RPUSH/LPUSH 实现逻辑：**

```rust
// 1. 检查是否有阻塞的客户端在等待这个键（BLPOP 场景）
// 2. 如果有，通知阻塞客户端，不添加到列表
// 3. 如果没有，获取现有列表（或创建新列表）
// 4. 检查键的类型是否匹配
// 5. 添加元素到列表末尾/开头
// 6. 存储回数据库，返回列表长度
```

**LPOP 实现逻辑：**

```rust
// 1. 获取列表和过期时间
// 2. 检查列表是否为空
// 3. 如果 count=1，移除并返回第一个元素
// 4. 如果 count>1，移除并返回前 count 个元素（作为数组）
// 5. 更新数据库
```

### 3. Stream 命令

| 命令     | 描述                     | 示例                     |
| -------- | ------------------------ | ------------------------ |
| `XADD`   | 添加流条目（自动生成ID） | `XADD key * field value` |
| `XRANGE` | 获取指定范围的条目       | `XRANGE key - +`         |

**XADD 实现逻辑：**

```rust
// 1. 解析流键和流 ID（* 表示自动生成）
// 2. 处理流 ID：
//    - "*"：生成格式为 <millisecondsTime>-<sequenceNumber> 的 ID
//    - 具体 ID：验证格式，检查是否大于已有 ID
// 3. 解析 field-value 对
// 4. 将条目添加到流的末尾
// 5. 检查是否有阻塞的 XREAD 客户端等待这个流
// 6. 如果有新数据，通知阻塞的客户端
// 7. 返回生成的流 ID
```

**XRANGE 实现逻辑：**

```rust
// 1. 解析键、起始 ID、结束 ID
// 2. 遍历流中的所有条目
// 3. 使用 is_id_in_range 检查条目是否在指定范围内
// 4. 构建返回数组：[[id, [field, value, ...]], ...]
```

### 4. 事务命令

| 命令      | 描述                 | 示例              |
| --------- | -------------------- | ----------------- |
| `MULTI`   | 开始事务             | `MULTI`           |
| `EXEC`    | 执行事务中的所有命令 | `EXEC`            |
| `DISCARD` | 丢弃事务             | `DISCARD`         |
| `WATCH`   | 监视键的变化         | `WATCH key1 key2` |
| `UNWATCH` | 取消监视所有键       | `UNWATCH`         |

**事务实现逻辑：**

```rust
// MULTI 命令：
// 1. 设置 in_transaction = true
// 2. 清空命令队列
// 3. 返回 "+OK\r\n"

// 命令入队（在事务中）：
// 1. 将命令数据加入 command_queue 队列
// 2. 返回 "+QUEUED\r\n"

// EXEC 命令：
// 1. 检查 in_transaction 状态
// 2. 检查 watched_keys 是否被修改（dirty_keys）
// 3. 如果被修改，返回空数组（事务中止）
// 4. 否则依次执行队列中的命令
// 5. 将所有响应组成数组返回
```

**WATCH 实现逻辑：**

```rust
// 1. 将被监视的键加入 watched_keys 列表
// 2. 从 dirty_keys 中移除这些键（WATCH 之后的修改才有效）
// 3. 重置 dirty 标记
```

### 5. 键过期机制

- **惰性删除**：过期键只在被访问时（如 GET）才检查并删除
- **过期时间精度**：支持秒（EX）和毫秒（PX）两种精度
- **存储结构**：`ValueWithExpiry { value, expiry }`

**过期检查逻辑：**

```rust
fn is_expired(expiry: &Option<u64>) -> bool {
    match expiry {
        Some(ts) => current_timestamp() > *ts,
        None => false,
    }
}
```

### 6. 主从复制

#### 主节点（Master）模式

**启动方式：**

```bash
cargo run -- --port 6379
```

**主节点职责：**

1. 接受副本连接
2. 维护已连接副本列表（`replicas`）
3. 传播写命令到所有副本：`SET`, `RPUSH`, `LPUSH`, `LPOP`, `XADD`, `INCR`, `BLPOP`

**主节点传播逻辑：**

```rust
// 1. 执行命令后，检查 is_change_command 标志
// 2. 如果是变更命令，遍历 replicas 列表
// 3. 将原始命令数据发送给每个副本
```

#### 从节点（Slave）模式

**启动方式：**

```bash
cargo run -- --port 6380 --replicaof "localhost 6379"
```

**从节点握手流程：**

```
1. 发送 PING → 期望收到 PONG
2. 发送 REPLCONF listening-port <port> → 期望收到 OK
3. 发送 REPLCONF capa psync2 → 期望收到 OK
4. 发送 PSYNC ? -1 → 期望收到 FULLRESYNC <replid> <offset>
```

**RDB 文件接收逻辑：**

```rust
// 1. 接收数据到 rdb_buffer
// 2. 检测 RDB 格式：$<length>\r\n<binary content>
// 3. 验证 REDIS 魔术签名
// 4. 完整接收后，处理后续数据
```

**从节点命令处理：**

```rust
// 1. 接收主节点传播的命令
// 2. 反序列化 RESP 命令
// 3. 执行命令并发送响应
// 4. 更新 master_repl_offset
```

### 7. 阻塞操作

| 命令    | 描述           | 示例                  |
| ------- | -------------- | --------------------- |
| `BLPOP` | 阻塞式从左弹出 | `BLPOP key timeout`   |
| `XREAD` | 阻塞式读取流   | `XREAD STREAMS key $` |

**BLPOP 实现逻辑：**

```rust
// 1. 尝试从列表中弹出元素
// 2. 如果列表为空，将客户端加入阻塞队列
// 3. 等待超时或被 LPUSH/RPUSH 唤醒
// 4. 被唤醒时，返回弹出的元素
```

**XREAD 实现逻辑：**

```rust
// 1. 解析流键和起始 ID（$ 表示最新 ID）
// 2. 如果没有新数据，将客户端加入阻塞队列
// 3. 等待超时或被 XADD 唤醒
// 4. 被唤醒时，返回新添加的条目
```

### 8. 其他命令

| 命令       | 描述                 | 示例                                   |
| ---------- | -------------------- | -------------------------------------- |
| `TYPE`     | 获取值的类型         | `TYPE key` → `string/list/stream/none` |
| `INCR`     | 递增（不存在则创建） | `INCR key`                             |
| `INFO`     | 获取服务器信息       | `INFO replication`                     |
| `REPLCONF` | 复制配置             | `REPLCONF GETACK *`                    |

**INCR 实现逻辑：**

```rust
// 1. 获取键当前值
// 2. 如果不存在，创建值为 1
// 3. 如果存在，解析为整数并 +1
// 4. 存储回数据库
// 5. 返回递增后的值
```

## 架构设计

### 项目结构

```
src/
├── main.rs           # 入口点，处理主/从模式切换
├── handle.rs        # 命令处理器实现
├── commands.rs       # 命令分发器
├── blocking.rs      # 阻塞操作支持
├── storage/
│   ├── mod.rs       # 数据库存储和 AppState
│   ├── config.rs    # 配置（主/从角色）
│   └── db_storage.rs # 数据库内部实现
├── stream_id.rs     # Stream ID 处理
└── utils/
    ├── resp.rs      # RESP 协议序列化/反序列化
    ├── case.rs      # 大小写转换工具
    └── mod.rs
```

### RESP 协议

Redis 序列化协议（RESP）用于客户端与服务器通信：

| 类型          | 格式                | 示例                               |
| ------------- | ------------------- | ---------------------------------- |
| Simple String | `+内容\r\n`         | `+OK\r\n`                          |
| Error         | `-错误\r\n`         | `-ERR\r\n`                         |
| Integer       | `:数字\r\n`         | `:100\r\n`                         |
| Bulk String   | `$长度\r\n内容\r\n` | `$5\r\nhello\r\n`                  |
| Array         | `*元素数\r\n`       | `*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n` |

### 键值存储结构

```rust
struct ValueWithExpiry {
    value: RedisValue,     // String, List, Stream 等
    expiry: Option<u64>,   // 过期时间戳（毫秒）
}

enum RedisValue {
    String(String),
    List(Vec<String>),
    Stream(Vec<StreamEntry>),
}
```

### 命令处理流程

```
1. 接收客户端数据
2. 反序列化 RESP 命令
3. 检查是否在事务中（队列或执行）
4. 调用命令处理器
5. 检查是否需要传播到副本
6. 序列化响应并发送
```

### 主从复制流程图

```
Master                    Slave
  |                         |
  |<-------- PING ---------| (slave 发起握手)
  |-------- PONG --------->|
  |                         |
  |<---- REPLCONF port ----| (通知监听端口)
  |-------- OK ----------->|
  |                         |
  |<---- REPLCONF capa ----| (通知支持 psync2)
  |-------- OK ----------->|
  |                         |
  |<------ PSYNC ? -1 -----| (请求全量同步)
  |--- FULLRESYNC + RDB --->|
  |                         |
  |======= 传播命令 =======>| (SET, RPUSH 等)
  |                         |
  |<----- REPLCONF GETACK -| (slave 报告 offset)
  |-------- ACK ---------->|
```

## 使用方法

### 启动为主节点（默认端口 6379）

```bash
cargo run -- --port 6379
```

### 启动为从节点

```bash
cargo run -- --port 6380 --replicaof "localhost 6379"
```

## 测试

```bash
cargo test
```

## 问题解决过程

在项目开发过程中，我们遇到了多个技术挑战，通过系统化的分析和解决，最终实现了完整的 RDB 文件解析和 KEYS 命令支持。以下是详细的问题解决过程记录。

### 1. RDB 文件解析挑战

#### 1.1 长度编码解析

**挑战描述：**
RDB 文件使用复杂的长度编码机制，根据前 2 位比特决定解析方式，包括 6 位、14 位、32 位长度以及特殊的字符串编码。

**技术背景：**

- 前 2 位为 0b00：长度在剩余 6 位（1 字节）
- 前 2 位为 0b01：长度在接下来的 14 位（2 字节，大端序）
- 前 2 位为 0b10：长度在接下来的 32 位（5 字节，大端序）
- 前 2 位为 0b11：特殊字符串编码

**解决方案：**

```rust
fn parse_length(&mut self) -> Result<usize, RdbError> {
    let byte = self.read_byte()?;
    let encoding_type = (byte & 0xC0) >> 6; // 获取前2位

    match encoding_type {
        // 00: 长度在剩余的6位中
        0 => Ok((byte & 0x3F) as usize),
        // 01: 长度在接下来的14位中（大端序）
        1 => {
            let next_byte = self.read_byte()?;
            let length = ((byte & 0x3F) as usize) << 8 | (next_byte as usize);
            Ok(length)
        }
        // 10: 长度在接下来的4字节中（大端序）
        2 => {
            let length = self.read_u32_be()? as usize;
            Ok(length)
        }
        // 11: 特殊编码（字符串编码）
        3 => Err(RdbError::InvalidLength),
        _ => unreachable!(),
    }
}
```

**经验教训：**

- 位操作是处理二进制格式的关键，需要仔细处理位掩码和位移
- 大端序和小端序的处理要严格按照规范执行
- 解析逻辑需要覆盖所有可能的编码情况

#### 1.2 字符串编码解析

**挑战描述：**
字符串编码不仅包含普通字符串，还支持 8 位、16 位、32 位整数的压缩存储，需要正确识别和转换。

**技术背景：**

- 0xC0：8 位整数
- 0xC1：16 位整数（小端序）
- 0xC2：32 位整数（小端序）
- 0xC3：LZF 压缩（本挑战不涉及）

**解决方案：**

```rust
fn parse_string(&mut self) -> Result<String, RdbError> {
    // 先查看长度编码类型
    let byte = self.peek_byte()?;
    let encoding_type = (byte & 0xC0) >> 6;

    if encoding_type == 3 {
        // 特殊字符串编码
        self.pos += 1; // 跳过类型字节
        let special_type = byte & 0x3F;

        match special_type {
            // 8位整数
            0 => {
                let value = self.read_byte()? as i8;
                Ok(value.to_string())
            }
            // 16位整数（小端序）
            1 => {
                let value = self.read_u16_le()? as i16;
                Ok(value.to_string())
            }
            // 32位整数（小端序）
            2 => {
                let value = self.read_u32_le()? as i32;
                Ok(value.to_string())
            }
            // LZF压缩（本挑战不涉及）
            3 => Err(RdbError::InvalidString),
            _ => Err(RdbError::InvalidEncoding(byte)),
        }
    } else {
        // 普通字符串
        let length = self.parse_length()?;
        if self.pos + length > self.data.len() {
            return Err(RdbError::UnexpectedEof);
        }
        let string_data = &self.data[self.pos..self.pos + length];
        let s = String::from_utf8_lossy(string_data).to_string();
        self.pos += length;
        Ok(s)
    }
}
```

**经验教训：**

- 字符串编码需要处理多种格式，包括压缩整数
- 小端序整数的读取需要特别注意字节顺序
- 边界检查对于防止缓冲区溢出至关重要

#### 1.3 过期时间处理

**挑战描述：**
RDB 文件中键的过期时间有两种格式：毫秒级（FC）和秒级（FD），需要正确解析并转换为统一的毫秒时间戳。

**技术背景：**

- FC：8 字节毫秒时间戳（小端序）
- FD：4 字节秒时间戳（小端序）

**解决方案：**

```rust
// 检查是否有过期时间
let byte = self.peek_byte()?;
match byte {
    // 毫秒级过期时间（8字节，小端序）
    0xFC => {
        self.pos += 1;
        expiry = Some(self.read_u64_le()?);
    }
    // 秒级过期时间（4字节，小端序）
    0xFD => {
        self.pos += 1;
        expiry = Some(self.read_u32_le()? as u64 * 1000); // 转换为毫秒
    }
    _ => {}
}
```

**经验教训：**

- 时间戳格式需要统一处理，避免混用秒和毫秒
- 小端序读取需要使用专门的字节顺序转换函数

### 2. 命令集成挑战

#### 2.1 KEYS 命令实现

**挑战描述：**
需要实现 `KEYS "*"` 命令，返回所有未过期的键，并按照 RESP 数组格式返回。

**技术背景：**

- 只支持 `*` 模式（返回所有键）
- 需要过滤已过期的键
- 响应格式为 RESP 数组

**解决方案：**

```rust
pub fn handle_keys(
    args: &[RespValue],
    db: &mut tokio::sync::MutexGuard<'_, crate::storage::DatabaseInner>,
) -> Result<Vec<u8>, String> {
    // 获取模式参数
    let pattern = if let Some(RespValue::BulkString(Some(p))) = args.get(1) {
        p.clone()
    } else {
        return Ok(serialize_resp(RespValue::Error(
            "ERR wrong number of arguments for 'keys' command".to_string(),
        )));
    };

    // 当前只支持 "*" 模式
    if pattern != "*" {
        return Ok(serialize_resp(RespValue::Error(
            "ERR only '*' pattern is supported".to_string(),
        )));
    }

    // 收集所有未过期的键
    let mut keys: Vec<String> = Vec::new();
    for (key, entry) in db.data.iter() {
        // 检查键是否过期
        if !is_expired(&entry.expiry) {
            keys.push(key.clone());
        }
    }

    // 构建RESP数组响应
    let mut response_array: Vec<RespValue> = Vec::new();
    for key in keys {
        response_array.push(RespValue::BulkString(Some(key)));
    }

    Ok(serialize_resp(RespValue::Array(Some(response_array))))
}
```

**经验教训：**

- 命令实现需要严格遵循 Redis 协议规范
- 过期键的过滤逻辑要与其他命令保持一致
- RESP 序列化需要正确处理数组格式

#### 2.2 命令注册和分发

**挑战描述：**
需要在命令分发器中正确注册 KEYS 命令，并确保在事务和非事务模式下都能正常工作。

**技术背景：**

- 命令分发器需要处理事务状态
- 命令需要在 `commands.rs` 中注册
- 处理函数需要添加到 `handle.rs`

**解决方案：**

在 `commands.rs` 中注册命令：

```rust
let result = match cmd_upper.as_str() {
    // ... 其他命令
    "KEYS" => handle_keys(&a, db),
    _ => Ok(serialize_resp(RespValue::Error(
        "ERR unknown command".to_string(),
    ))),
};
```

**经验教训：**

- 命令注册需要在正确的位置添加
- 命令处理函数的签名要符合规范
- 事务处理逻辑需要正确处理新命令

### 3. 启动加载挑战

#### 3.1 RDB 文件加载

**挑战描述：**
程序启动时需要从指定路径加载 RDB 文件，如果文件不存在则视为空数据库。

**技术背景：**

- RDB 文件路径由 `--dir` 和 `--dbfilename` 参数指定
- 文件不存在时应静默处理
- 解析失败时应记录错误并继续运行

**解决方案：**

```rust
// 加载RDB文件（如果存在）
{
    let rdb_path = format!("{}/{}", config.rdb_config.dir, config.rdb_config.dbfilename);
    println!("[RDB] Looking for RDB file at: {}", rdb_path);

    if let Ok(rdb_data) = std::fs::read(&rdb_path) {
        println!("[RDB] Found RDB file, size: {} bytes", rdb_data.len());

        match RdbParser::new(rdb_data).parse() {
            Ok(rdb) => {
                println!("[RDB] Successfully parsed RDB file, found {} keys", rdb.keys.len());

                // 将解析的键值对加载到数据库
                let mut db_guard = db.lock().await;
                for (key, value, expiry) in rdb.keys {
                    db_guard.data.insert(
                        key,
                        ValueWithExpiry {
                            value,
                            expiry,
                        },
                    );
                }
                drop(db_guard);
            }
            Err(e) => {
                eprintln!("[RDB] Failed to parse RDB file: {}", e);
            }
        }
    } else {
        println!("[RDB] RDB file not found, starting with empty database");
    }
}
```

**经验教训：**

- 文件操作需要优雅处理不存在的情况
- 解析错误需要记录但不影响程序启动
- 数据库锁的获取和释放要正确管理

### 4. 编译和测试挑战

#### 4.1 编译错误处理

**挑战描述：**
在实现过程中遇到了编译错误，需要快速定位和解决。

**技术背景：**

- `RedisValue` 未实现 `Debug` trait
- 未使用的导入和变量
- 类型不匹配

**解决方案：**

1. **移除不必要的 `Debug` 派生**：
   - 从 `RdbData` 结构中移除 `#[derive(Debug)]`

2. **清理未使用的导入**：
   - 移除 `std::io::Read` 导入
   - 移除 `RdbError::Io` 变体

3. **类型兼容性**：
   - 确保所有类型转换正确
   - 处理可能的溢出情况

**经验教训：**

- 编译错误要及时处理，避免积累
- 代码清理（如移除未使用的导入）有助于保持代码整洁
- 类型安全是 Rust 的核心优势，要充分利用

#### 4.2 测试验证

**挑战描述：**
需要确保实现的功能能够通过 `codecrafters submit` 的所有测试用例。

**技术背景：**

- 测试用例覆盖了各种命令和场景
- 需要处理并发客户端
- 需要正确处理各种边界情况

**解决方案：**

1. **本地测试**：
   - 运行 `cargo run` 验证程序能正常启动
   - 检查 RDB 文件加载日志

2. **提交测试**：
   - 运行 `codecrafters submit` 验证所有测试通过
   - 分析测试结果，解决失败的测试用例

**经验教训：**

- 测试是验证功能的关键手段
- 日志输出有助于调试和问题定位
- 边界情况需要特别关注

### 5. 经验总结与最佳实践

#### 5.1 技术最佳实践

1. **二进制格式解析**：
   - 使用位操作处理紧凑的编码格式
   - 严格按照规范处理字节序（大端序/小端序）
   - 实现边界检查防止缓冲区溢出

2. **命令实现**：
   - 严格遵循 Redis 协议规范
   - 保持命令处理逻辑的一致性
   - 正确处理过期键和事务状态

3. **文件操作**：
   - 优雅处理文件不存在的情况
   - 错误处理要健壮，不影响程序启动
   - 合理使用锁管理并发访问

4. **代码组织**：
   - 模块化设计，分离关注点
   - 清晰的函数职责和文档
   - 适当的错误处理和日志记录

#### 5.2 经验教训

1. **细节决定成败**：
   - 二进制格式的解析需要高度关注细节
   - 字节序、编码格式等小问题容易导致整体失败

2. **渐进式开发**：
   - 分阶段实现功能，逐步测试
   - 先实现核心解析逻辑，再集成到主程序

3. **测试驱动**：
   - 利用测试用例验证实现的正确性
   - 日志输出有助于理解程序行为

4. **错误处理**：
   - 优雅处理各种错误情况
   - 错误信息要清晰，便于调试

#### 5.3 未来改进建议

1. **性能优化**：
   - 大文件解析时考虑流式处理
   - 使用更高效的数据结构存储解析结果

2. **功能扩展**：
   - 支持更多 KEYS 模式匹配
   - 实现更多 RDB 数据类型的解析
   - 添加 RDB 文件写入功能

3. **测试覆盖**：
   - 添加单元测试和集成测试
   - 测试各种边界情况和异常输入

4. **代码质量**：
   - 进一步完善文档注释
   - 优化错误处理和日志系统
   - 提高代码的可维护性和可扩展性

### 6. 结论

通过系统化的分析和解决，我们成功实现了 RDB 文件解析和 KEYS 命令支持，通过了所有测试用例。这个过程不仅加深了对 Redis 内部机制的理解，也锻炼了处理复杂二进制格式的能力。

关键成功因素：

- 严格遵循 RDB 文件格式规范
- 模块化的代码设计
- 全面的错误处理
- 充分的测试验证

这些经验和最佳实践将为未来的 Redis 相关开发提供宝贵的参考。
