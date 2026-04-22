//! RDB文件解析模块
//!
//! 提供Redis RDB文件的解析功能
//!
//! 支持的特性：
//! - RDB头部解析（Magic + Version）
//! - 元数据部分解析
//! - 数据库部分解析（键值对、过期时间）
//! - 长度编码和字符串编码

pub mod parser;

pub use parser::{RdbParser, RdbData, RdbError};
