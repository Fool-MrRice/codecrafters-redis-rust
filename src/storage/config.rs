// --replicaof "<MASTER_HOST> <MASTER_PORT>"
// 配置从节点的主节点地址和端口，并用于解析主节点的地址和端口

// 如果用户未包含该标志，请回复。--replicaofrole:master
// 如果用户包含了该标志，请用 。--replicaofrole:slave

pub struct Config {
    pub replicaof: ReplicaofRole,
}

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
            replicaof: self.replicaof,
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
            replicaof: ReplicaofRole::Master,
        }
    }
}
