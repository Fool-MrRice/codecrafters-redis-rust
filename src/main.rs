// #![allow(unused_imports)]
use clap::Parser;
use codecrafters_redis::blocking::{prepare_blpop, prepare_xread, wait_for_blocked_command};
use codecrafters_redis::commands::command_handler;
use codecrafters_redis::storage::create_database;
use codecrafters_redis::storage::{AppState, cleanup_expired_keys, config};
use codecrafters_redis::utils::case::to_uppercase;
use codecrafters_redis::utils::resp::{RespValue, deserialize_resp, serialize_resp};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::signal;
use tokio::time::Duration;

#[tokio::main]
async fn main() {
    println!("Logs from your program will appear here!");

    let cli = Cli::parse();
    let port = cli.port.unwrap_or(6379);
    let replicaof = cli.replicaof.as_deref();
    let config = {
        if let Some(replicaof) = replicaof {
            config::ConfigBuilder::new()
                .as_slave_from_str(replicaof)
                .build()
        } else {
            config::ConfigBuilder::new().as_master().build()
        }
    };
    match config.replicaof {
        config::ReplicaofRole::Master => {
            let _ = start_master_mode(port, &config).await;
        }
        config::ReplicaofRole::Slave(_, _) => {
            start_slave_mode(port, &config).await;
        }
    }
}

async fn start_slave_mode(port: u16, config: &config::Config) -> () {
    // 先启动服务器，确保它在端口上监听
    let mut config_clone = config.clone();
    config_clone.is_silence = true;
    let app_state = start_master_mode(port, &config_clone).await;

    // 然后与主节点建立连接
    let addr = match &config.replicaof {
        config::ReplicaofRole::Slave(host, port) => format!("{}:{}", host, port),
        _ => {
            eprintln!("Invalid replicaof config");
            return;
        }
    };
    let mut listener = match TcpStream::connect(&addr).await {
        Ok(stream) => stream,
        Err(e) => {
            eprintln!("连接{}主节点失败: {}", addr, e);
            return;
        }
    };
    let ping_cmd = serialize_resp(RespValue::Array(Some(vec![RespValue::BulkString(Some(
        "PING".to_string(),
    ))])));
    listener.write_all(&ping_cmd).await.unwrap();
    let mut buf = [0u8; 1024];
    let n = listener.read(&mut buf).await.unwrap();
    let resp = deserialize_resp(&buf[..n]).unwrap();

    if let RespValue::SimpleString(s) = &resp {
        if to_uppercase(s) == "PONG" {
            println!("成功收到PONG");
        } else {
            eprintln!("Invalid PONG response: {:?}", resp);
            return;
        }
    } else {
        eprintln!("Invalid PONG response: {:?}", resp);
        return;
    }

    let replconf_1 = serialize_resp(RespValue::Array(Some(vec![
        RespValue::BulkString(Some("REPLCONF".to_string())),
        RespValue::BulkString(Some("listening-port".to_string())),
        RespValue::BulkString(Some(port.to_string())),
    ])));
    listener.write_all(&replconf_1).await.unwrap();
    let mut buf = [0u8; 1024];
    let n = listener.read(&mut buf).await.unwrap();
    let resp = deserialize_resp(&buf[..n]).unwrap();

    if let RespValue::SimpleString(s) = &resp {
        if to_uppercase(s) == "OK" {
            println!("成功收到OK");
        } else {
            eprintln!("Invalid OK response: {:?}", resp);
            return;
        }
    } else {
        eprintln!("Invalid OK response: {:?}", resp);
        return;
    }
    let replconf_2 = serialize_resp(RespValue::Array(Some(vec![
        RespValue::BulkString(Some("REPLCONF".to_string())),
        RespValue::BulkString(Some("capa".to_string())),
        RespValue::BulkString(Some("psync2".to_string())),
    ])));
    listener.write_all(&replconf_2).await.unwrap();
    let mut buf = [0u8; 1024];
    let n = listener.read(&mut buf).await.unwrap();
    let resp = deserialize_resp(&buf[..n]).unwrap();

    if let RespValue::SimpleString(s) = &resp {
        if to_uppercase(s) == "OK" {
            println!("成功收到OK");
        } else {
            eprintln!("Invalid OK response: {:?}", resp);
            return;
        }
    } else {
        eprintln!("Invalid OK response: {:?}", resp);
        return;
    }
    let psync_cmd = serialize_resp(RespValue::Array(Some(vec![
        RespValue::BulkString(Some("PSYNC".to_string())),
        RespValue::BulkString(Some("?".to_string())),
        RespValue::BulkString(Some("-1".to_string())),
    ])));
    listener.write_all(&psync_cmd).await.unwrap();
    let mut buf = [0u8; 1024];
    let n = listener.read(&mut buf).await.unwrap();
    let resp = deserialize_resp(&buf[..n]).unwrap();

    if let RespValue::SimpleString(s) = &resp {
        println!("成功收到PSYNC响应: {}", s);
        let psync_info = s.split_whitespace().collect::<Vec<_>>();
        if let (Some(&first_str), Some(&id), Some(&offset)) =
            (psync_info.get(0), psync_info.get(1), psync_info.get(2))
        {
            match first_str {
                "FULLRESYNC" => {
                    println!("需要全量同步");
                    println!("全量同步ID: {}, 偏移量: {}", id, offset);
                }
                _ => {
                    println!("todo: 增量同步");
                }
            }
        } else {
            eprintln!("Invalid PSYNC response: {:?}", resp);
            return;
        }
    } else {
        eprintln!("Invalid PSYNC response: {:?}", resp);
        return;
    }

    // 读取RDB文件
    let mut buf = [0u8; 1024];
    let n = listener.read(&mut buf).await.unwrap();
    println!("收到RDB文件: {:?}", &buf[..n]);

    // 处理与主节点的连接，接收传播的命令
    let (mut read_half, _) = listener.into_split();
    let app_state_clone = Arc::clone(&app_state);

    tokio::spawn(async move {
        let mut buf = [0u8; 1024];
        loop {
            let n = match read_half.read(&mut buf).await {
                Ok(n) if n == 0 => break,
                Ok(n) => n,
                Err(e) => {
                    eprintln!("Error reading from master: {}", e);
                    break;
                }
            };
            let data = buf[..n].to_vec();

            // 处理主节点传播的命令
            let mut in_transaction = false;
            let mut command_queue: Vec<Vec<u8>> = Vec::new();
            let mut watched_keys: Vec<String> = Vec::new();
            let mut dirty = false;

            // 打印接收到的命令
            println!(
                "Received command from master: {:?}",
                String::from_utf8_lossy(&data)
            );

            match app_state_clone.db.lock() {
                Ok(mut guard) => match command_handler(
                    &data,
                    &mut guard,
                    &mut in_transaction,
                    &mut command_queue,
                    &mut watched_keys,
                    &mut dirty,
                    &app_state_clone.config,
                ) {
                    Ok(_) => {
                        // 副本不需要向主节点发送响应
                        println!("Command handled successfully");
                    }
                    Err(e) => {
                        eprintln!("Error handling command from master: {}", e);
                    }
                },
                Err(e) => {
                    eprintln!("Error locking database: {}", e);
                }
            }

            // 打印数据库内容
            if let Ok(guard) = app_state_clone.db.lock() {
                println!("Database content: {:?}", guard.data);
            }

            buf.fill(0);
        }
    });

    // 保持函数运行，不返回
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            println!("Shutting down...");
        }
    }
}

async fn start_master_mode(port: u16, config: &config::Config) -> Arc<AppState> {
    let addr = format!("127.0.0.1:{}", port);
    let listener = TcpListener::bind(addr).await.unwrap();
    let db = create_database();
    let config = config.clone();
    // 保持传入的is_silence设置，不要强制重置为false
    let app_state = AppState {
        config: Arc::new(std::sync::Mutex::new(config)),
        db: db.clone(),
        replicas: Arc::new(tokio::sync::Mutex::new(Vec::new())),
    };
    let app_state = Arc::new(app_state);

    let db_clone = Arc::clone(&app_state.db);
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_millis(100)).await;
            cleanup_expired_keys(&db_clone);
        }
    });

    let app_state_clone = Arc::clone(&app_state);
    tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            println!("accepted new connection");
            let app_state = Arc::clone(&app_state_clone);
            tokio::spawn(async move {
                let (mut read_half, write_half) = stream.into_split();
                let write_half = Arc::new(tokio::sync::Mutex::new(write_half));
                let mut buf = [0u8; 1024];
                let mut in_transaction = false;
                let mut command_queue: Vec<Vec<u8>> = Vec::new();
                let mut watched_keys: Vec<String> = Vec::new();
                let mut dirty = false;
                let mut is_replica = false;
                let mut is_change_command;

                loop {
                    // 每次读取命令前，重置is_change_command
                    is_change_command = false;
                    let n = match read_half.read(&mut buf).await {
                        Ok(n) if n == 0 => break,
                        Ok(n) => n,
                        Err(e) => {
                            eprintln!("Error reading from stream: {}", e);
                            break;
                        }
                    };
                    let data = buf[..n].to_vec();

                    if !is_replica {
                        if let Ok(resp) = deserialize_resp(&data) {
                            if let RespValue::Array(Some(a)) = resp {
                                if let Some(RespValue::BulkString(Some(cmd))) = a.get(0) {
                                    if to_uppercase(cmd) == "REPLCONF" {
                                        is_replica = true;
                                        let mut replicas = app_state.replicas.lock().await;
                                        replicas.push(Arc::clone(&write_half));
                                    }
                                }
                            }
                        }
                    }

                    let command_type = {
                        if let Ok(resp) = deserialize_resp(&data) {
                            if let RespValue::Array(Some(a)) = resp {
                                if let Some(RespValue::BulkString(Some(cmd))) = a.get(0) {
                                    let cmd_upper = to_uppercase(&cmd);
                                    if cmd_upper == "BLPOP" {
                                        "BLPOP"
                                    } else if cmd_upper == "XREAD" {
                                        "XREAD"
                                    } else {
                                        "OTHER"
                                    }
                                } else {
                                    "OTHER"
                                }
                            } else {
                                "OTHER"
                            }
                        } else {
                            "OTHER"
                        }
                    };

                    let response = match command_type {
                        "BLPOP" => {
                            is_change_command = true;
                            if let Ok(resp) = deserialize_resp(&data) {
                                if let RespValue::Array(Some(a)) = resp {
                                    let blocked_result = match app_state.db.lock() {
                                        Ok(mut guard) => prepare_blpop(&a, &mut guard).unwrap(),
                                        Err(e) => {
                                            eprintln!("Error locking database: {}", e);
                                            return;
                                        }
                                    };
                                    wait_for_blocked_command(blocked_result).await
                                } else {
                                    serialize_resp(RespValue::Error(
                                        "ERR unknown command".to_string(),
                                    ))
                                }
                            } else {
                                serialize_resp(RespValue::Error("ERR unknown command".to_string()))
                            }
                        }
                        "XREAD" => {
                            is_change_command = false;
                            if let Ok(resp) = deserialize_resp(&data) {
                                if let RespValue::Array(Some(a)) = resp {
                                    let blocked_result = match app_state.db.lock() {
                                        Ok(mut guard) => prepare_xread(&a, &mut guard).unwrap(),
                                        Err(e) => {
                                            eprintln!("Error locking database: {}", e);
                                            return;
                                        }
                                    };
                                    wait_for_blocked_command(blocked_result).await
                                } else {
                                    serialize_resp(RespValue::Error(
                                        "ERR unknown command".to_string(),
                                    ))
                                }
                            } else {
                                serialize_resp(RespValue::Error("ERR unknown command".to_string()))
                            }
                        }
                        _ => match app_state.db.lock() {
                            Ok(mut guard) => match command_handler(
                                &data,
                                &mut guard,
                                &mut in_transaction,
                                &mut command_queue,
                                &mut watched_keys,
                                &mut dirty,
                                &app_state.config,
                            ) {
                                Ok(resp) => resp,
                                Err(e) => {
                                    eprintln!("Error handling command: {}", e);
                                    serialize_resp(RespValue::Error(
                                        "ERR internal error".to_string(),
                                    ))
                                }
                            },
                            Err(e) => {
                                eprintln!("Error locking database: {}", e);
                                serialize_resp(RespValue::Error("ERR internal error".to_string()))
                            }
                        },
                    };

                    let is_slave = if let config::ReplicaofRole::Slave(_, _) =
                        app_state.config.lock().unwrap().replicaof
                    {
                        true
                    } else {
                        false
                    };

                    // 副本应该响应客户端命令，但不响应主节点的命令
                    // 只有当连接是主节点连接时，才需要保持静默
                    // 客户端连接应该正常响应
                    if !is_replica {
                        let mut write_half = write_half.lock().await;
                        if let Err(e) = write_half.write_all(&response).await {
                            eprintln!("Error writing response: {}", e);
                            break;
                        }
                    }
                    // 仅当命令是变更命令时，才需要传播到从节点
                    // 判断命令是否是变更命令，例如SET、DEL、HSET等
                    // 非变更命令，例如GET、XREAD等，不需要传播到从节点
                    if let Ok(resp) = deserialize_resp(&data) {
                        if let RespValue::Array(Some(a)) = resp {
                            if let Some(RespValue::BulkString(Some(cmd))) = a.get(0) {
                                match to_uppercase(cmd).as_str() {
                                    "SET" | "RPUSH" | "LPUSH" | "LPOP" | "XADD" | "INCR"
                                    | "BLPOP" => is_change_command = true,
                                    _ => is_change_command = false,
                                }
                            }
                        }
                    }

                    if !is_replica && is_change_command {
                        let replicas = app_state.replicas.lock().await;
                        for replica_write in replicas.iter() {
                            let mut write_half = replica_write.lock().await;
                            if let Err(e) = write_half.write_all(&data).await {
                                eprintln!("Error propagating to replica: {}", e);
                            }
                        }
                    }

                    buf.fill(0);
                }
            });
        }
    });

    app_state
}

#[derive(Parser, Debug)]
#[command(name = "rusty_redis-server", about = "A Redis Server")]
struct Cli {
    #[arg(long)]
    port: Option<u16>,
    #[arg(long)]
    replicaof: Option<String>,
}
