//! Storage模块 - 数据库、配置和应用状态管理
//!
//! 包含：
//! - db_storage: 数据库存储和阻塞客户端管理
//! - config: 服务器配置
//! - AppState: 应用全局状态

pub mod db_storage;

/// 重新导出常用类型，方便外部使用
pub use db_storage::{
    BlockedClient, BlockedClients, Database, DatabaseInner, RedisValue, StreamEntry,
    ValueWithExpiry, cleanup_expired_keys, create_database, current_timestamp, is_expired,
};

/// 配置模块
pub mod config;
pub use config::{Config, ReplicaofRole};

use std::sync::{Arc, Mutex};
use tokio::net::tcp::OwnedWriteHalf;
use tokio::sync::Mutex as TokioMutex;
use tokio::sync::broadcast;

/// 应用全局状态
///
/// 包含：
/// - config: 服务器配置
/// - db: 数据库实例
/// - replicas: 副本连接列表
/// - wait_acks_tx: WAIT命令的广播通道（用于等待副本ACK）
pub struct AppState {
    pub config: Arc<Mutex<config::Config>>,
    pub db: Database,
    pub replicas: Arc<Mutex<Vec<Arc<TokioMutex<OwnedWriteHalf>>>>>,
    pub wait_acks_tx: Arc<Mutex<Option<broadcast::Sender<u64>>>>,
}
