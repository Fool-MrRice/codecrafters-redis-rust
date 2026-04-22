//! 数据库存储模块 - 实现Redis数据结构、过期键管理、阻塞客户端管理等核心功能
//!
//! 包含的数据结构：
//! - StreamEntry: 流条目的结构
//! - RedisValue: Redis支持的各种数据类型
//! - ValueWithExpiry: 带过期时间的键值对
//! - DatabaseInner: 数据库内部结构
//! - BlockedClient: 阻塞客户端信息
//! - BlockedClients: 阻塞客户端管理器

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex;

/// 流条目结构
///
/// 包含：
/// - id: 流条目的唯一标识符（格式：<毫秒>-<序号>）
/// - fields: 键值对字段列表
#[derive(Clone)]
pub struct StreamEntry {
    pub id: String,
    pub fields: Vec<HashMap<String, String>>,
}

/// Redis数据类型枚举
///
/// 支持的类型：
/// - String: 字符串类型
/// - List: 列表类型
/// - Stream: 流类型
/// - Integer: 整数类型
#[derive(Clone)]
pub enum RedisValue {
    String(String),
    List(Vec<String>),
    Stream(Vec<StreamEntry>),
    Integer(i64),
}

/// 带过期时间的键值对
///
/// 包含：
/// - value: 实际存储的值
/// - expiry: 过期时间戳（毫秒），None表示永不过期
#[derive(Clone)]
pub struct ValueWithExpiry {
    pub value: RedisValue,
    pub expiry: Option<u64>,
}

/// 数据库内部结构
///
/// 包含：
/// - data: 实际存储的键值对数据
/// - blocked_clients: 阻塞客户端管理器
/// - dirty_keys: 被监视且可能被修改的键集合（用于WATCH命令）
pub struct DatabaseInner {
    pub data: HashMap<String, ValueWithExpiry>,
    pub blocked_clients: BlockedClients,
    pub dirty_keys: HashSet<String>,
}

impl DatabaseInner {
    /// 生成RDB文件用于主从复制
    ///
    /// 返回：
    /// - 包含RDB文件的RESP格式字节数组
    ///
    /// 当前实现返回一个最小化的有效RDB文件，包含：
    /// 1. Magic number: "REDIS" (5字节)
    /// 2. RDB Version: "0011" (4字节)
    /// 3. EOF标记: 0xFF (1字节)
    /// 4. CRC64校验和: 8字节（简化版本，使用全零）
    pub fn transport_binary_by_rdb(&self) -> Vec<u8> {
        let rdb_magic = b"REDIS";
        let rdb_version = b"0011";
        let rdb_eof = 0xFFu8;
        let rdb_crc: [u8; 8] = [0, 0, 0, 0, 0, 0, 0, 0];

        let mut rdb_content = Vec::new();
        rdb_content.extend_from_slice(rdb_magic);
        rdb_content.extend_from_slice(rdb_version);
        rdb_content.push(rdb_eof);
        rdb_content.extend_from_slice(&rdb_crc);

        let mut response = Vec::new();
        response.extend_from_slice(format!("${}\r\n", rdb_content.len()).as_bytes());
        response.extend_from_slice(&rdb_content);
        response
    }
}

/// 数据库类型别名 - Arc<Mutex<DatabaseInner>>
///
/// 使用Arc和Mutex确保线程安全访问
pub type Database = Arc<Mutex<DatabaseInner>>;

/// 创建新的数据库实例
///
/// 返回：
/// - 初始化的Database实例
pub fn create_database() -> Database {
    Arc::new(Mutex::new(DatabaseInner {
        data: HashMap::new(),
        blocked_clients: BlockedClients::new(),
        dirty_keys: HashSet::new(),
    }))
}

/// 获取当前时间戳（毫秒）
///
/// 返回：
/// - 从UNIX纪元开始的毫秒数
pub fn current_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

/// 检查键是否过期
///
/// 参数：
/// - expiry: 过期时间戳（毫秒），None表示永不过期
///
/// 返回：
/// - true表示已过期，false表示未过期
pub fn is_expired(expiry: &Option<u64>) -> bool {
    match expiry {
        Some(ts) => current_timestamp() > *ts,
        None => false,
    }
}

/// 阻塞客户端结构
///
/// 用于管理BLPOP、XREAD等阻塞命令的客户端
///
/// 包含：
/// - key: 被监视的键
/// - timeout: 超时时间
/// - start_time: 开始阻塞的时间戳（毫秒）
/// - last_id: 流的最后ID（用于XREAD）
/// - tx: 用于通知客户端的oneshot通道发送端
pub struct BlockedClient {
    pub key: String,
    pub timeout: Duration,
    pub start_time: u64,
    pub last_id: String,
    pub tx: tokio::sync::oneshot::Sender<Vec<u8>>,
}

/// 阻塞客户端管理器
///
/// 管理所有阻塞的客户端，支持添加、移除和清理超时客户端
///
/// 结构：
/// - clients: HashMap，键为列表名，值为阻塞的客户端列表
pub struct BlockedClients {
    pub clients: HashMap<String, Vec<BlockedClient>>,
}

impl Default for BlockedClients {
    fn default() -> Self {
        Self::new()
    }
}

impl BlockedClients {
    /// 创建新的阻塞客户端管理器
    ///
    /// 返回：
    /// - 初始化的BlockedClients实例
    pub fn new() -> Self {
        BlockedClients {
            clients: HashMap::new(),
        }
    }

    /// 添加阻塞客户端
    ///
    /// 参数：
    /// - list_name: 被监视的列表/键名
    /// - client: 阻塞客户端信息
    pub fn add_client(&mut self, list_name: String, client: BlockedClient) {
        self.clients.entry(list_name).or_default().push(client);
    }

    /// 获取并移除列表的第一个阻塞客户端
    ///
    /// 参数：
    /// - list_name: 列表/键名
    ///
    /// 返回：
    /// - Some(BlockedClient) 如果有阻塞客户端
    /// - None 如果没有阻塞客户端
    pub fn pop_client(&mut self, list_name: &str) -> Option<BlockedClient> {
        if let Some(clients) = self.clients.get_mut(list_name)
            && !clients.is_empty()
        {
            return Some(clients.remove(0));
        }
        None
    }

    /// 清理超时的阻塞客户端
    ///
    /// 功能：
    /// 1. 遍历所有阻塞客户端
    /// 2. 检查是否超时
    /// 3. 移除超时的客户端
    /// 4. 清理空的列表
    pub fn cleanup_timeout_clients(&mut self) {
        let current_time = current_timestamp();
        let mut empty_lists = Vec::new();

        for (list_name, clients) in self.clients.iter_mut() {
            clients.retain(|client| {
                if client.timeout.is_zero() {
                    true
                } else {
                    let elapsed_ms = current_time - client.start_time;
                    elapsed_ms < client.timeout.as_millis() as u64
                }
            });
            if clients.is_empty() {
                empty_lists.push(list_name.clone());
            }
        }

        for list_name in empty_lists {
            self.clients.remove(&list_name);
        }
    }
}

/// 清理过期键（惰性删除的补充）
///
/// 参数：
/// - db: 数据库引用
///
/// 功能：
/// 1. 获取数据库锁
/// 2. 遍历所有键，检查是否过期
/// 3. 删除过期的键
/// 4. 清理超时的阻塞客户端
pub async fn cleanup_expired_keys(db: &Database) {
    let mut db = db.lock().await;
    let mut keys_to_remove = Vec::new();

    for (key, entry) in db.data.iter() {
        if is_expired(&entry.expiry) {
            keys_to_remove.push(key.clone());
        }
    }

    for key in keys_to_remove {
        db.data.remove(&key);
        println!("Cleaned up expired key: {}", key);
    }

    db.blocked_clients.cleanup_timeout_clients();
}
