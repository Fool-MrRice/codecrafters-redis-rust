// #![allow(unused_imports)]
use clap::Parser;
use codecrafters_redis::blocking::{prepare_blpop, prepare_xread, wait_for_blocked_command};
use codecrafters_redis::commands;
use codecrafters_redis::storage::create_database;
use codecrafters_redis::storage::{AppState, cleanup_expired_keys, config};
use codecrafters_redis::utils::case::to_uppercase;
use codecrafters_redis::utils::resp::{RespValue, deserialize_resp, serialize_resp};
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

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
            // 保持函数运行，不返回
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {
                    println!("Shutting down...");
                }
            }
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
    listener
        .write_all(&ping_cmd)
        .await
        .map_err(|e| {
            eprintln!("写入PING命令失败: {}", e);
        })
        .unwrap();
    let mut buf = [0u8; 1024];
    let n = listener
        .read(&mut buf)
        .await
        .map_err(|e| {
            eprintln!("读取主节点响应失败: {}", e);
        })
        .unwrap();
    let (resp, _) = deserialize_resp(&buf[..n]).unwrap();

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
    let (resp, _) = deserialize_resp(&buf[..n]).unwrap();

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
    let (resp, _) = deserialize_resp(&buf[..n]).unwrap();

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
    let (resp, consumed) = deserialize_resp(&buf[..n]).unwrap();

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

                    // 检查是否有剩余数据（可能是RDB文件）
                    if consumed < n {
                        let remaining = &buf[consumed..n];
                        println!(
                            "PSYNC响应后有剩余数据: {} bytes, starts with: {:?}",
                            remaining.len(),
                            &remaining[..remaining.len().min(10)]
                        );
                    }
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

    // 处理与主节点的连接，接收传播的命令,对于特殊命令仍需有返回响应
    let (mut read_half, write_half) = listener.into_split();
    let write_half = Arc::new(tokio::sync::Mutex::new(write_half));
    let app_state_clone = Arc::clone(&app_state);

    tokio::spawn(async move {
        let mut buf = [0u8; 1024];
        let mut rdb_received = false;

        loop {
            let n: usize = match read_half.read(&mut buf).await {
                Ok(n) if n == 0 => break,
                Ok(n) => n,
                Err(e) => {
                    eprintln!("Error reading from master: {}", e);
                    break;
                }
            };
            let mut data = buf[..n].to_vec();

            println!(
                "[RECV] Received {} bytes, rdb_received={}, data preview: {:?}",
                n,
                rdb_received,
                String::from_utf8_lossy(&data[..data.len().min(50)])
            );

            // 检查是否是RDB文件
            if !rdb_received {
                println!(
                    "[RDB] Checking for RDB file, data starts with: {:?}",
                    data.get(0..5)
                );
                // 简化RDB文件检测：如果数据以$开头，假设是RDB文件
                if data.starts_with(b"$") {
                    println!("[RDB] RDB file detected (starts with $), attempting to parse length");
                    // 尝试解析RDB文件长度
                    let mut i = 1; // 跳过'$'
                    let mut length_str = String::new();
                    while i < data.len() && data[i] != b'\r' {
                        length_str.push(data[i] as char);
                        i += 1;
                    }

                    if i + 1 < data.len() && data[i] == b'\r' && data[i + 1] == b'\n' {
                        if let Ok(rdb_length) = length_str.parse::<usize>() {
                            let header_len = 1 + length_str.len() + 2; // $ + length + \r\n
                            let total_len = header_len + rdb_length;

                            println!(
                                "[RDB] RDB length parsed: {}, header_len: {}, total_len: {}, data.len(): {}",
                                rdb_length,
                                header_len,
                                total_len,
                                data.len()
                            );

                            if data.len() >= total_len {
                                println!(
                                    "[RDB] RDB file complete: {} bytes, total RDB segment: {} bytes",
                                    rdb_length, total_len
                                );
                                rdb_received = true;
                                // 跳过RDB文件，保留剩余数据
                                if data.len() > total_len {
                                    // 有剩余数据，可能是命令
                                    let remaining = data[total_len..].to_vec();
                                    println!(
                                        "[RDB] Data after RDB: {} bytes, content: {:?}",
                                        remaining.len(),
                                        String::from_utf8_lossy(&remaining)
                                    );
                                    // 用剩余数据替换原始数据
                                    data = remaining;
                                    println!(
                                        "[RDB] Replaced data with remaining commands, new data length: {}",
                                        data.len()
                                    );
                                } else {
                                    // 没有剩余数据，清空data并继续等待下一个数据包
                                    data = vec![];
                                    println!(
                                        "[RDB] No data after RDB, clearing data and continuing to next packet"
                                    );
                                    buf.fill(0);
                                    continue;
                                }
                            } else {
                                println!(
                                    "[RDB] Incomplete RDB file, need {} more bytes, waiting for more data",
                                    total_len - data.len()
                                );
                                buf.fill(0);
                                continue;
                            }
                        } else {
                            println!(
                                "[RDB] Failed to parse RDB length '{}', treating as invalid",
                                length_str
                            );
                            buf.fill(0);
                            continue;
                        }
                    } else {
                        println!("[RDB] No CRLF found after length, treating as invalid");
                        buf.fill(0);
                        continue;
                    }
                } else {
                    // 不是RDB文件，可能是主节点发送的命令
                    // 直接进入命令处理
                    println!(
                        "[RDB] Data doesn't start with $, assuming commands, setting rdb_received = true"
                    );
                    rdb_received = true;
                }
            }

            println!(
                "[CMD] After RDB check: rdb_received = {}, data length = {}",
                rdb_received,
                data.len()
            );

            // 处理命令（无论是RDB文件之后的数据，还是非RDB文件数据）
            if rdb_received && !data.is_empty() {
                // 处理主节点传播的命令
                let mut in_transaction = false;
                let mut command_queue: Vec<Vec<u8>> = Vec::new();
                let mut watched_keys: Vec<String> = Vec::new();
                let mut dirty = false;

                // 打印接收到的命令
                println!(
                    "[CMD] Received command from master: {:?}",
                    String::from_utf8_lossy(&data)
                );

                // 处理多个命令在同一个TCP段中的情况
                let mut remaining_data = data;
                println!(
                    "[CMD] Starting command processing loop, remaining_data length: {}",
                    remaining_data.len()
                );
                while !remaining_data.is_empty() {
                    println!(
                        "[CMD] Calling deserialize_resp with {} bytes",
                        remaining_data.len()
                    );
                    match deserialize_resp(&remaining_data) {
                        Ok((resp, consumed)) => {
                            println!(
                                "[CMD] deserialize_resp succeeded: type = {:?}, consumed = {}",
                                std::mem::discriminant(&resp),
                                consumed
                            );
                            // 这里的这个resp应该就是主节点发送的命令，是一个数组，第一个元素是命令名，后续元素是命令参数
                            // 判断命令名
                            let is_command_name = if let RespValue::Array(Some(array)) = resp {
                                if let Some(RespValue::BulkString(Some(s))) = array.get(0) {
                                    println!(
                                        "[CMD] Command name: {}, is REPLCONF: {}",
                                        s,
                                        s.eq("REPLCONF")
                                    );
                                    s.eq("REPLCONF")
                                } else {
                                    eprintln!("[CMD] Invalid command array format");
                                    remaining_data = remaining_data[consumed..].to_vec();
                                    continue;
                                }
                            } else {
                                // 可能是RDB文件或其他非命令数据，跳过它
                                println!("[CMD] Skipping non-command RESP value: {:?}", resp);
                                remaining_data = remaining_data[consumed..].to_vec();
                                continue;
                            };

                            // 提取单个命令的数据
                            let command_data = remaining_data[..consumed].to_vec();
                            remaining_data = remaining_data[consumed..].to_vec();

                            let mut response_result: Option<Result<Vec<u8>, String>> = None;
                            let mut guard = app_state_clone.db.lock().await;
                            response_result = Some(
                                commands::command_handler_async(
                                    &command_data,
                                    &mut guard,
                                    &mut in_transaction,
                                    &mut command_queue,
                                    &mut watched_keys,
                                    &mut dirty,
                                    &app_state_clone,
                                )
                                .await,
                            );

                            if let Some(response_result) = response_result {
                                match response_result {
                                    Ok(response) => {
                                        // 副本对于一般命令不需要向主节点发送响应
                                        // 但对于特殊命令（如REPLCONF）需要向主节点发送响应
                                        if is_command_name {
                                            println!(
                                                "[CMD] Sending REPLCONF response, length: {}",
                                                response.len()
                                            );
                                            let write_half_clone = Arc::clone(&write_half);
                                            let mut write_guard = write_half_clone.lock().await;
                                            write_guard.write_all(&response).await.unwrap();
                                            println!("[CMD] REPLCONF response sent successfully");
                                        }
                                        // 更新master_repl_offset
                                        let mut config_guard =
                                            app_state_clone.config.lock().unwrap();
                                        let old_offset = config_guard.master_repl_offset;
                                        config_guard.master_repl_offset += consumed as u64;
                                        println!(
                                            "[CMD] Updated master_repl_offset: {} -> {}",
                                            old_offset, config_guard.master_repl_offset
                                        );
                                        println!("[CMD] Command handled successfully");
                                    }
                                    Err(e) => {
                                        eprintln!(
                                            "[CMD] Error handling command from master: {}",
                                            e
                                        );
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            eprintln!("[CMD] Error deserializing command: {}", e);
                            eprintln!(
                                "[CMD] Remaining data (first 100 bytes): {:?}",
                                &remaining_data[..remaining_data.len().min(100)]
                            );
                            break;
                        }
                    }
                }

                // 打印数据库内容（仅打印键）
                let guard = app_state_clone.db.lock().await;
                let keys: Vec<&String> = guard.data.keys().collect();
                println!("[CMD] Database keys: {:?}", keys);
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
    let (wait_acks_tx, _wait_acks_rx) = tokio::sync::broadcast::channel::<u64>(100);
    let app_state = AppState {
        config: Arc::new(std::sync::Mutex::new(config)),
        db: db.clone(),
        replicas: Arc::new(std::sync::Mutex::new(Vec::new())),
        wait_acks_tx: Arc::new(Mutex::new(Some(wait_acks_tx))),
    };
    let app_state = Arc::new(app_state);

    let db_clone = Arc::clone(&app_state.db);
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_millis(100)).await;
            cleanup_expired_keys(&db_clone).await;
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

                    // 如果是副本连接，检查是否是 REPLCONF ACK
                    if is_replica {
                        if let Ok((resp, _)) = deserialize_resp(&data) {
                            if let RespValue::Array(Some(a)) = &resp {
                                if let Some(RespValue::BulkString(Some(cmd))) = a.get(0) {
                                    if cmd == "REPLCONF" {
                                        if let Some(RespValue::BulkString(Some(sub_cmd))) = a.get(1)
                                        {
                                            if sub_cmd == "ACK" {
                                                if let Some(RespValue::BulkString(Some(
                                                    offset_str,
                                                ))) = a.get(2)
                                                {
                                                    if let Ok(offset) = offset_str.parse::<u64>() {
                                                        if let Ok(mut tx_guard) =
                                                            app_state.wait_acks_tx.lock()
                                                        {
                                                            if let Some(ref mut tx) = *tx_guard {
                                                                let _ = tx.send(offset);
                                                            }
                                                        }
                                                    }
                                                }
                                                // ACK 响应不需要回复给副本，继续等待下一个命令
                                                buf.fill(0);
                                                continue;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }

                    // 先保存是否是REPLCONF命令或PSYNC命令
                    let is_repl_or_psync_cmd = if let Ok((resp, _)) = deserialize_resp(&data) {
                        if let RespValue::Array(Some(a)) = resp {
                            if let Some(RespValue::BulkString(Some(cmd))) = a.get(0) {
                                let cmd_upper = to_uppercase(cmd);
                                cmd_upper == "REPLCONF" || cmd_upper == "PSYNC"
                            } else {
                                false
                            }
                        } else {
                            false
                        }
                    } else {
                        false
                    };

                    let command_type = {
                        if let Ok((resp, _)) = deserialize_resp(&data) {
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
                            if let Ok((resp, _)) = deserialize_resp(&data) {
                                if let RespValue::Array(Some(a)) = resp {
                                    let mut guard = app_state.db.lock().await;
                                    let blocked_result = prepare_blpop(&a, &mut guard).unwrap();
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
                            if let Ok((resp, _)) = deserialize_resp(&data) {
                                if let RespValue::Array(Some(a)) = resp {
                                    let mut guard = app_state.db.lock().await;
                                    let blocked_result = prepare_xread(&a, &mut guard).unwrap();
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
                        _ => {
                            let mut response =
                                serialize_resp(RespValue::Error("ERR internal error".to_string()));
                            let mut guard = app_state.db.lock().await;
                            match commands::command_handler_async(
                                &data,
                                &mut guard,
                                &mut in_transaction,
                                &mut command_queue,
                                &mut watched_keys,
                                &mut dirty,
                                &app_state,
                            )
                            .await
                            {
                                Ok(resp) => response = resp,
                                Err(e) => {
                                    eprintln!("Error handling command: {}", e);
                                    response = serialize_resp(RespValue::Error(
                                        "ERR internal error".to_string(),
                                    ));
                                }
                            }
                            response
                        }
                    };

                    // let is_slave = if let config::ReplicaofRole::Slave(_, _) =
                    //     app_state.config.lock().unwrap().replicaof
                    // {
                    //     true
                    // } else {
                    //     false
                    // };

                    // 副本应该响应客户端命令，但不响应主节点的命令
                    // 只有当连接是主节点连接时，才需要保持静默
                    // 客户端连接应该正常响应
                    // 对于REPLCONF命令和PSYNC命令，即使连接是副本连接，也需要发送响应
                    if !is_replica || is_repl_or_psync_cmd {
                        let mut write_half = write_half.lock().await;
                        if let Err(e) = write_half.write_all(&response).await {
                            eprintln!("Error writing response: {}", e);
                            break;
                        }
                    }

                    // 处理REPLCONF命令，将连接标记为副本连接
                    if !is_replica
                        && if let Ok((resp, _)) = deserialize_resp(&data) {
                            if let RespValue::Array(Some(a)) = resp {
                                if let Some(RespValue::BulkString(Some(cmd))) = a.get(0) {
                                    to_uppercase(cmd) == "REPLCONF"
                                } else {
                                    false
                                }
                            } else {
                                false
                            }
                        } else {
                            false
                        }
                    {
                        is_replica = true;
                        let mut replicas = app_state.replicas.lock().unwrap();
                        replicas.push(Arc::clone(&write_half));
                    }

                    // 仅当命令是变更命令时，才需要传播到从节点
                    // 判断命令是否是变更命令，例如SET、DEL、HSET等
                    // 非变更命令，例如GET、XREAD等，不需要传播到从节点
                    if let Ok((resp, _)) = deserialize_resp(&data) {
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
                        // 更新master_repl_offset
                        {
                            let mut config_guard = app_state.config.lock().unwrap();
                            config_guard.master_repl_offset += data.len() as u64;
                        }

                        // 将replicas转换为Vec，避免跨await点持有MutexGuard
                        let replicas: Vec<_> = app_state.replicas.lock().unwrap().clone();
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
