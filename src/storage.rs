use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[derive(Clone)]
pub enum RedisValue {
    String(String),
    List(Vec<String>),
    // 后续可以添加其他类型，如 Hash、Set 等
}

#[derive(Clone)]
pub struct ValueWithExpiry {
    pub value: RedisValue,
    pub expiry: Option<u64>, // 过期时间戳（毫秒）
}

pub struct DatabaseInner {
    pub data: HashMap<String, ValueWithExpiry>,
    pub blocked_clients: BlockedClients,
}

pub type Database = Arc<Mutex<DatabaseInner>>;

pub fn create_database() -> Database {
    Arc::new(Mutex::new(DatabaseInner {
        data: HashMap::new(),
        blocked_clients: BlockedClients::new(),
    }))
}

// 获取当前时间戳（毫秒）
pub fn current_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

// 检查键是否过期
pub fn is_expired(expiry: &Option<u64>) -> bool {
    match expiry {
        Some(ts) => current_timestamp() > *ts,
        None => false,
    }
}

// 定义阻塞客户端结构
pub struct BlockedClient {
    pub key: String,
    pub timeout: Duration,                                  // 超时时间
    pub start_time: u64,                                    // 开始阻塞的时间戳（毫秒）
    pub tx: tokio::sync::oneshot::Sender<(String, String)>, // 用于通知客户端的通道
}

// 定义阻塞客户端管理器
pub struct BlockedClients {
    pub clients: HashMap<String, Vec<BlockedClient>>, // 键: 列表名, 值: 阻塞的客户端列表
}

impl BlockedClients {
    pub fn new() -> Self {
        BlockedClients {
            clients: HashMap::new(),
        }
    }

    // 添加阻塞客户端
    pub fn add_client(&mut self, list_name: String, client: BlockedClient) {
        self.clients
            .entry(list_name)
            .or_insert(Vec::new())
            .push(client);
    }

    // 获取并移除列表的第一个阻塞客户端
    pub fn pop_client(&mut self, list_name: &str) -> Option<BlockedClient> {
        if let Some(clients) = self.clients.get_mut(list_name) {
            if !clients.is_empty() {
                return Some(clients.remove(0));
            }
        }
        None
    }

    // 清理超时的阻塞客户端
    pub fn cleanup_timeout_clients(&mut self) {
        let current_time = current_timestamp(); // 毫秒级时间戳
        let mut empty_lists = Vec::new();

        for (list_name, clients) in self.clients.iter_mut() {
            clients.retain(|client| {
                if client.timeout.is_zero() {
                    // 无限期阻塞
                    true
                } else {
                    // 检查是否超时
                    let elapsed_ms = current_time - client.start_time;
                    elapsed_ms < client.timeout.as_millis() as u64
                }
            });
            if clients.is_empty() {
                empty_lists.push(list_name.clone());
            }
        }

        // 移除空列表
        for list_name in empty_lists {
            self.clients.remove(&list_name);
        }
    }
}

// 清理过期键
pub fn cleanup_expired_keys(db: &Database) {
    let mut db = db.lock().unwrap();
    let mut keys_to_remove = Vec::new();

    // 遍历所有键，检查是否过期
    for (key, entry) in db.data.iter() {
        if is_expired(&entry.expiry) {
            keys_to_remove.push(key.clone());
        }
    }

    // 删除过期的键
    for key in keys_to_remove {
        db.data.remove(&key);
        println!("Cleaned up expired key: {}", key);
    }

    // 清理超时的阻塞客户端
    db.blocked_clients.cleanup_timeout_clients();
}
