// --replicaof "<MASTER_HOST> <MASTER_PORT>"
// 配置从节点的主节点地址和端口，并用于解析主节点的地址和端口

// 如果用户未包含该标志，请回复。--replicaofrole:master
// 如果用户包含了该标志，请用 。--replicaofrole:slave

#[derive(Debug, Clone)]
pub struct Config {
    // 是否静默
    pub is_silence: bool,
    pub replicaof: ReplicaofRole,
    // master_replid：一个40个字符的字母数字字符串
    pub master_replid: String,
    // master_repl_offset:0
    pub master_repl_offset: u64,
}

#[derive(Debug, Clone)]
pub enum ReplicaofRole {
    Master,
    Slave(String, u16),
}

// ConfigBuilder用于链式调用创建Config
pub struct ConfigBuilder {
    replicaof: ReplicaofRole,
}

impl ConfigBuilder {
    // 创建一个新的ConfigBuilder，默认配置为主节点
    pub fn new() -> Self {
        ConfigBuilder {
            replicaof: ReplicaofRole::Master,
        }
    }

    // 设置为主节点
    pub fn as_master(mut self) -> Self {
        self.replicaof = ReplicaofRole::Master;
        self
    }

    // 设置为从节点，指定主节点的地址和端口
    pub fn as_slave(mut self, host: String, port: u16) -> Self {
        self.replicaof = ReplicaofRole::Slave(host, port);
        self
    }
    pub fn as_slave_from_str(self, replicaof: &str) -> Self {
        let (host, port) = replicaof
            .split_once(" ")
            .expect("replicaof must be in the format 'MASTER_HOST MASTER_PORT'");
        self.as_slave(host.to_string(), port.parse().unwrap())
    }

    // 构建最终的Config
    pub fn build(self) -> Config {
        Config {
            is_silence: false,
            replicaof: self.replicaof,
            master_replid: "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".to_string(),
            master_repl_offset: 0,
        }
    }
}

impl Default for ConfigBuilder {
    fn default() -> Self {
        Self::new()
    }
}

// 为Config提供默认实现
impl Default for Config {
    fn default() -> Self {
        Config {
            is_silence: false,
            replicaof: ReplicaofRole::Master,
            master_replid: "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".to_string(),
            master_repl_offset: 0,
        }
    }
}
