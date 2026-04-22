//! Redis Clone - Rust 实现
//!
//! 一个用 Rust 实现的 Redis 克隆版本，支持基本命令和主从复制功能
//!
//! 主要功能：
//! - 字符串命令：PING, ECHO, SET, GET
//! - 列表命令：RPUSH, LPUSH, LRANGE, LLEN, LPOP, BLPOP
//! - Stream 命令：XADD, XRANGE, XREAD
//! - 事务命令：MULTI, EXEC, DISCARD, WATCH, UNWATCH
//! - 键过期机制：惰性删除 + 定期清理
//! - 主从复制：支持 PSYNC, 复制命令传播
//! - 阻塞操作：BLPOP, XREAD with BLOCK
//!
//! 模块结构：
//! - `main`: 程序入口，处理主从模式切换
//! - `handle`: 命令处理器实现
//! - `commands`: 命令分发器
//! - `blocking`: 阻塞操作支持
//! - `storage`: 数据存储和应用状态
//! - `stream_id`: Stream ID 处理
//! - `utils`: 工具函数

pub mod blocking;
pub mod commands;
pub mod handle;
pub mod rdb;
pub mod storage;
pub mod stream_id;
pub mod utils;
