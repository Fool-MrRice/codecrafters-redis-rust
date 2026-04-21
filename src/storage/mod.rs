// Storage模块 - 数据库和阻塞客户端管理

pub mod db_storage;

// 重新导出常用类型，方便外部使用
pub use db_storage::{
    BlockedClient, BlockedClients, Database, DatabaseInner, RedisValue, StreamEntry,
    ValueWithExpiry, cleanup_expired_keys, create_database, current_timestamp, is_expired,
};

// 导出配置模块
pub mod config;
pub use config::{Config, ReplicaofRole};

// 全局配置
use std::sync::{Arc, Mutex};
use tokio::net::tcp::OwnedWriteHalf;
use tokio::sync::Mutex as TokioMutex;

pub struct AppState {
    pub config: Arc<Mutex<config::Config>>,
    pub db: Database,
    pub replicas: Arc<Mutex<Vec<Arc<TokioMutex<OwnedWriteHalf>>>>>,
}
