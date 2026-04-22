//! 服务器配置模块
//!
//! 提供服务器配置和主从角色管理功能
//!
//! 命令行参数：
//! - `--port <port>`: 指定服务器监听端口
//! - `--replicaof "<master_host> <master_port>"`: 配置为从节点，指定主节点地址

/// 服务器配置
///
/// 包含服务器的所有配置信息
#[derive(Debug, Clone)]
pub struct Config {
    /// 是否静默模式（保留字段，暂未使用）
    pub is_silence: bool,
    /// 复制角色（主节点或从节点）
    pub replicaof: ReplicaofRole,
    /// 主节点的复制 ID（40 字符的字符串）
    pub master_replid: String,
    /// 主节点的复制偏移量
    pub master_repl_offset: u64,
    /// RDB存储路径和文件名
    pub rdb_config: RDBConfig,
}

/// RDB配置
///
/// 包含 RDB 文件的存储路径和文件名
#[derive(Debug, Clone)]
pub struct RDBConfig {
    /// RDB 文件存储路径
    pub dir: String,
    /// RDB 文件名
    pub dbfilename: String,
}

/// 复制角色枚举
///
/// 表示服务器在主从复制中的角色
#[derive(Debug, Clone)]
pub enum ReplicaofRole {
    /// 主节点角色
    Master,
    /// 从节点角色，包含主节点的主机和端口
    Slave(String, u16),
}

/// 配置构建器
///
/// 用于链式调用创建 Config 实例
pub struct ConfigBuilder {
    replicaof: ReplicaofRole,
    rdb_config: RDBConfig,
}

impl ConfigBuilder {
    /// 创建一个新的 ConfigBuilder
    ///
    /// 默认配置为主节点
    ///
    /// # 返回值
    /// * 新的 ConfigBuilder 实例
    pub fn new() -> Self {
        ConfigBuilder {
            replicaof: ReplicaofRole::Master,
            rdb_config: RDBConfig {
                dir: "/tmp/redis-files".to_string(),
                dbfilename: "dump.rdb".to_string(),
            },
        }
    }

    /// 设置为主节点
    ///
    /// # 返回值
    /// * 更新后的 ConfigBuilder 实例
    pub fn as_master(mut self) -> Self {
        self.replicaof = ReplicaofRole::Master;
        self
    }

    /// 设置为从节点
    ///
    /// # 参数
    /// * `host` - 主节点的主机名或 IP 地址
    /// * `port` - 主节点的端口号
    ///
    /// # 返回值
    /// * 更新后的 ConfigBuilder 实例
    pub fn as_slave(mut self, host: String, port: u16) -> Self {
        self.replicaof = ReplicaofRole::Slave(host, port);
        self
    }

    /// 从字符串设置为从节点
    ///
    /// # 参数
    /// * `replicaof` - 格式为 "<master_host> <master_port>" 的字符串
    ///
    /// # 返回值
    /// * 更新后的 ConfigBuilder 实例
    ///
    /// # Panics
    /// 如果字符串格式不正确会 panic
    pub fn as_slave_from_str(self, replicaof: &str) -> Self {
        let (host, port) = replicaof
            .split_once(" ")
            .expect("replicaof must be in the format \"MASTER_HOST MASTER_PORT\"");
        self.as_slave(host.to_string(), port.parse().unwrap())
    }

    /// 设置 RDB 配置
    ///
    /// # 参数
    /// * `dir` - RDB 文件存储路径
    /// * `dbfilename` - RDB 文件名
    ///
    /// # 返回值
    /// * 更新后的 ConfigBuilder 实例
    pub fn with_rdb_config(mut self, dir: String, dbfilename: String) -> Self {
        self.rdb_config = RDBConfig { dir, dbfilename };
        self
    }

    /// 构建最终的 Config
    ///
    /// # 返回值
    /// * 配置好的 Config 实例
    pub fn build(self) -> Config {
        Config {
            is_silence: false,
            replicaof: self.replicaof,
            // 这个是硬编码的，后续可以再真正实现
            master_replid: "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".to_string(),
            master_repl_offset: 0,
            rdb_config: self.rdb_config,
        }
    }
}

impl Default for ConfigBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// 为 Config 提供默认实现
///
/// 默认配置为主节点
impl Default for Config {
    fn default() -> Self {
        Config {
            is_silence: false,
            replicaof: ReplicaofRole::Master,
            master_replid: "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".to_string(),
            master_repl_offset: 0,
            rdb_config: RDBConfig {
                dir: "/tmp/redis-files".to_string(),
                dbfilename: "dump.rdb".to_string(),
            },
        }
    }
}
