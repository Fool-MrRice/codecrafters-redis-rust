use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Clone)]
pub struct ValueWithExpiry {
    pub value: String,
    pub expiry: Option<u64>, // 过期时间戳（毫秒）
}

pub type Database = Arc<Mutex<HashMap<String, ValueWithExpiry>>>;

pub fn create_database() -> Database {
    Arc::new(Mutex::new(HashMap::new()))
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

// 清理过期键
pub fn cleanup_expired_keys(db: &Database) {
    let mut db = db.lock().unwrap();
    let mut keys_to_remove = Vec::new();

    // 遍历所有键，检查是否过期
    for (key, entry) in db.iter() {
        if is_expired(&entry.expiry) {
            keys_to_remove.push(key.clone());
        }
    }

    // 删除过期的键
    for key in keys_to_remove {
        db.remove(&key);
        println!("Cleaned up expired key: {}", key);
    }
}
