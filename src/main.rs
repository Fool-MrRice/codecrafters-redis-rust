// Redis服务器实现 - 支持主从复制、事务、流数据、阻塞命令等特性
//
// 主要功能：
// - 基本命令：PING, ECHO, SET, GET, DEL, CONFIG, KEYS等
// - 事务支持：MULTI, EXEC, DISCARD, WATCH, UNWATCH
// - 流数据：XADD, XREAD, XREADGROUP, XACK
// - 阻塞命令：BLPOP, XREAD with BLOCK
// - 复制：PSYNC, REPLCONF, ACK
// - 过期键：PX/EX参数支持，自动清理过期键
// - 等待命令：WAIT

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

/// 主函数 - Redis服务器入口点
///
/// 功能：
/// 1. 解析命令行参数（端口、replicaof配置）
/// 2. 根据配置启动主节点或从节点模式
/// 3. 主节点模式：监听端口，处理客户端连接
/// 4. 从节点模式：连接主节点，接收复制数据
#[tokio::main]
async fn main() {
    println!("Logs from your program will appear here!");

    // 解析命令行参数
    let cli = Cli::parse();
    let port = cli.port.unwrap_or(6379);
    let replicaof = cli.replicaof.as_deref();

    // 根据replicaof参数构建配置
    // 如果指定了replicaof，则作为从节点；否则作为主节点
    let config = {
        if let Some(replicaof) = replicaof {
            config::ConfigBuilder::new()
                .as_slave_from_str(replicaof)
                .build()
        } else {
            config::ConfigBuilder::new().as_master().build()
        }
    };

    // 根据配置启动对应模式
    match config.replicaof {
        config::ReplicaofRole::Master => {
            // 主节点模式：启动服务器并监听端口
            let _ = start_master_mode(port, &config).await;
            // 保持函数运行，等待Ctrl+C信号
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {
                    println!("Shutting down...");
                }
            }
        }
        config::ReplicaofRole::Slave(_, _) => {
            // 从节点模式：连接主节点并开始复制
            start_slave_mode(port, &config).await;
        }
    }
}

/// 从节点模式 - 连接主节点并接收复制数据
///
/// 参数：
/// - port: 从节点监听的端口
/// - config: 配置信息，包含主节点地址
///
/// 流程：
/// 1. 启动本地服务器（静默模式，不输出日志）
/// 2. 连接主节点
/// 3. 执行复制握手：PING -> REPLCONF listening-port -> REPLCONF capa -> PSYNC
/// 4. 接收RDB文件
/// 5. 持续接收主节点传播的命令
async fn start_slave_mode(port: u16, config: &config::Config) -> () {
    // 步骤1：先启动本地服务器，确保它在端口上监听
    // 使用静默模式避免重复输出日志
    let mut config_clone = config.clone();
    config_clone.is_silence = true;
    let app_state = start_master_mode(port, &config_clone).await;

    // 步骤2：与主节点建立连接
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

    // 步骤3：执行复制握手
    // 3.1 发送PING命令，验证连接
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
        } else {
            eprintln!("Invalid PONG response: {:?}", resp);
            return;
        }
    } else {
        eprintln!("Invalid PONG response: {:?}", resp);
        return;
    }

    // 3.2 发送REPLCONF listening-port，告知主节点从节点的监听端口
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
        } else {
            eprintln!("Invalid OK response: {:?}", resp);
            return;
        }
    } else {
        eprintln!("Invalid OK response: {:?}", resp);
        return;
    }

    // 3.3 发送REPLCONF capa，告知主节点支持的复制能力（psync2）
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
        } else {
            eprintln!("Invalid OK response: {:?}", resp);
            return;
        }
    } else {
        eprintln!("Invalid OK response: {:?}", resp);
        return;
    }

    // 3.4 发送PSYNC命令，请求同步数据
    // 参数：?（表示请求复制ID），-1（表示从offset 0开始）
    let psync_cmd = serialize_resp(RespValue::Array(Some(vec![
        RespValue::BulkString(Some("PSYNC".to_string())),
        RespValue::BulkString(Some("?".to_string())),
        RespValue::BulkString(Some("-1".to_string())),
    ])));
    listener.write_all(&psync_cmd).await.unwrap();

    // 步骤4：读取PSYNC响应和RDB文件
    // PSYNC响应格式：+FULLRESYNC <replid> <offset>\r\n
    // 之后紧跟着RDB文件：$<length>\r\n<data>
    // 注意：这些数据可能在同一个TCP包中，也可能分多个包到达
    let mut buf = vec![0u8; 8192];
    let n = listener.read(&mut buf).await.unwrap();
    println!("[PSYNC] Read {} bytes after PSYNC command", n);
    println!("[PSYNC] First 100 bytes (hex): {:02x?}", &buf[..n.min(100)]);

    // 手动解析PSYNC响应，找到第一个\r\n
    // 不能使用deserialize_resp，因为它会读取整个SimpleString直到\r\n，
    // 但实际数据中FULLRESYNC响应后紧跟着RDB文件，
    // deserialize_resp可能会错误地将整个数据当作一个SimpleString
    let mut psync_end = 0;
    for i in 0..n - 1 {
        if buf[i] == b'\r' && buf[i + 1] == b'\n' {
            psync_end = i + 2;
            break;
        }
    }

    println!("[PSYNC] PSYNC response ends at byte {}", psync_end);
    let psync_response = String::from_utf8_lossy(&buf[1..psync_end - 2]); // 跳过开头的'+'和结尾的\r\n
    println!("[PSYNC] PSYNC response: {}", psync_response);

    // 验证PSYNC响应格式：FULLRESYNC <replid> <offset>
    let psync_info = psync_response.split_whitespace().collect::<Vec<_>>();
    if psync_info.len() >= 3 && psync_info[0] == "FULLRESYNC" {
        println!(
            "[PSYNC] FULLRESYNC confirmed, replication ID: {}, offset: {}",
            psync_info[1], psync_info[2]
        );
    } else {
        eprintln!("[PSYNC] Invalid PSYNC response: {}", psync_response);
        return;
    }

    // 步骤5：处理与主节点的连接，接收传播的命令
    // 对于特殊命令（如REPLCONF GETACK）需要返回响应
    let (mut read_half, write_half) = listener.into_split();
    let write_half = Arc::new(tokio::sync::Mutex::new(write_half));
    let app_state_clone = Arc::clone(&app_state);

    // 检查PSYNC响应后是否有剩余数据（RDB文件）
    // 如果有，将其作为初始数据传递给replica处理循环
    let initial_data = if n > psync_end {
        let remaining = n - psync_end;
        println!(
            "[PSYNC] Found {} bytes after PSYNC response (RDB file)",
            remaining
        );
        println!(
            "[PSYNC] RDB data starts (hex): {:02x?}",
            &buf[psync_end..n.min(psync_end + 100)]
        );
        buf[psync_end..n].to_vec()
    } else {
        println!("[PSYNC] No data after PSYNC response, will read RDB separately");
        vec![]
    };

    // 步骤6：启动replica命令处理任务
    // 该任务持续从主节点读取数据，处理RDB文件和传播的命令
    tokio::spawn(async move {
        let mut buf = [0u8; 8192]; // 增加缓冲区大小以容纳RDB文件+命令
        let mut rdb_received = false; // 标记是否已接收RDB文件
        let mut accumulated_data = initial_data; // 从PSYNC响应后的数据开始

        println!("[REPLICA] Starting replica command processing loop");
        if !accumulated_data.is_empty() {
            println!(
                "[REPLICA] Starting with {} bytes from PSYNC response",
                accumulated_data.len()
            );
            println!(
                "[REPLICA] Initial data (hex): {:02x?}",
                &accumulated_data[..accumulated_data.len().min(100)]
            );
        }

        loop {
            // 如果accumulated_data为空，需要从master读取新数据
            if accumulated_data.is_empty() {
                println!("[REPLICA] Waiting to read from master...");
                let n: usize = match read_half.read(&mut buf).await {
                    Ok(n) if n == 0 => {
                        println!("[REPLICA] Connection closed by master");
                        break;
                    }
                    Ok(n) => {
                        println!("[REPLICA] Read {} bytes from master", n);
                        n
                    }
                    Err(e) => {
                        eprintln!("[REPLICA] Error reading from master: {}", e);
                        break;
                    }
                };

                // 将新读取的数据追加到accumulated_data
                accumulated_data.extend_from_slice(&buf[..n]);
                println!(
                    "[REPLICA] Total accumulated data: {} bytes",
                    accumulated_data.len()
                );
            } else {
                println!(
                    "[REPLICA] Processing accumulated data: {} bytes",
                    accumulated_data.len()
                );
            }

            // 克隆数据用于处理，清空accumulated_data
            // 如果有未处理的数据，会在处理过程中重新填充
            let mut data = accumulated_data.clone();
            accumulated_data.clear();

            println!(
                "[RECV] Received {} bytes, rdb_received={}, data preview: {:?}",
                data.len(),
                rdb_received,
                String::from_utf8_lossy(&data[..data.len().min(50)])
            );
            println!(
                "[RECV] Full data (hex): {:02x?}",
                &data[..data.len().min(100)]
            );

            // 检查是否是RDB文件
            // RDB文件格式：$<length>\r\n<data>
            // 如果数据以$开头，说明是RDB文件
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

                    // 检查是否有完整的长度字段（$<length>\r\n）
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

                            // 检查数据是否完整
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
                                    println!("[RDB] Remaining data (hex): {:02x?}", remaining);
                                    // 用剩余数据替换原始数据，继续处理
                                    data = remaining;
                                    println!(
                                        "[RDB] Replaced data with remaining commands, new data length: {}",
                                        data.len()
                                    );
                                } else {
                                    // 没有剩余数据，清空data和buf，等待下一个数据包
                                    data = vec![];
                                    println!(
                                        "[RDB] No data after RDB, clearing buffer and waiting for next packet"
                                    );
                                    buf.fill(0);
                                    continue;
                                }
                            } else {
                                // RDB文件不完整，需要等待更多数据
                                println!(
                                    "[RDB] Incomplete RDB file, need {} more bytes, waiting for more data",
                                    total_len - data.len()
                                );
                                buf.fill(0);
                                continue;
                            }
                        } else {
                            // 解析长度失败
                            println!(
                                "[RDB] Failed to parse RDB length '{}', treating as invalid",
                                length_str
                            );
                            buf.fill(0);
                            continue;
                        }
                    } else {
                        // 没有找到CRLF
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

            // 步骤7：处理主节点传播的命令
            // 只有在RDB文件接收完成后，才开始处理命令
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
                        "[CMD] Calling deserialize_resp with {} bytes, data: {:?}",
                        remaining_data.len(),
                        String::from_utf8_lossy(&remaining_data[..remaining_data.len().min(100)])
                    );
                    println!(
                        "[CMD] Data hex: {:02x?}",
                        &remaining_data[..remaining_data.len().min(100)]
                    );
                    match deserialize_resp(&remaining_data) {
                        Ok((resp, consumed)) => {
                            println!(
                                "[CMD] deserialize_resp succeeded: resp = {:?}, consumed = {}",
                                resp, consumed
                            );
                            // 这里的这个resp应该就是主节点发送的命令，是一个数组，第一个元素是命令名，后续元素是命令参数
                            // 判断命令名
                            let (is_replconf, command_name) =
                                if let RespValue::Array(Some(array)) = &resp {
                                    if let Some(RespValue::BulkString(Some(s))) = array.get(0) {
                                        let is_replconf = s.eq_ignore_ascii_case("REPLCONF");
                                        println!(
                                            "[CMD] Command name: {}, is REPLCONF: {}",
                                            s, is_replconf
                                        );
                                        (is_replconf, s.clone())
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

                            println!("[CMD] About to handle command: {}", command_name);

                            let response = {
                                println!("[CMD] Acquiring db lock for command handling");
                                let mut guard = app_state_clone.db.lock().await;
                                println!("[CMD] Lock acquired, calling command_handler_async");
                                let result = commands::command_handler_async(
                                    &command_data,
                                    &mut guard,
                                    &mut in_transaction,
                                    &mut command_queue,
                                    &mut watched_keys,
                                    &mut dirty,
                                    &app_state_clone,
                                )
                                .await;
                                println!("[CMD] command_handler_async returned, dropping lock");
                                drop(guard);
                                println!("[CMD] Lock dropped");
                                result
                            };

                            println!("[CMD] Processing response for command: {}", command_name);
                            match response {
                                Ok(response_data) => {
                                    println!(
                                        "[CMD] Command succeeded, response length: {}",
                                        response_data.len()
                                    );
                                    // 副本对于一般命令不需要向主节点发送响应
                                    // 但对于特殊命令（如REPLCONF）需要向主节点发送响应
                                    if is_replconf {
                                        println!(
                                            "[CMD] This is REPLCONF, sending response to master, length: {}",
                                            response_data.len()
                                        );
                                        println!(
                                            "[CMD] Response data (hex): {:02x?}",
                                            &response_data[..response_data.len().min(50)]
                                        );
                                        let write_half_clone = Arc::clone(&write_half);
                                        println!("[CMD] Acquiring write lock");
                                        let mut write_guard = write_half_clone.lock().await;
                                        println!("[CMD] Write lock acquired, writing response");
                                        if let Err(e) = write_guard.write_all(&response_data).await
                                        {
                                            eprintln!(
                                                "[CMD] Failed to send REPLCONF response: {}",
                                                e
                                            );
                                        } else {
                                            println!("[CMD] REPLCONF response sent successfully");
                                        }
                                        drop(write_guard);
                                        println!("[CMD] Write lock dropped");
                                    } else {
                                        println!(
                                            "[CMD] Command {} does not require response to master",
                                            command_name
                                        );
                                    }
                                    // 更新master_repl_offset
                                    let mut config_guard = app_state_clone.config.lock().unwrap();
                                    let old_offset = config_guard.master_repl_offset;
                                    config_guard.master_repl_offset += consumed as u64;
                                    println!(
                                        "[CMD] Updated master_repl_offset: {} -> {}",
                                        old_offset, config_guard.master_repl_offset
                                    );
                                    println!("[CMD] Command handled successfully");
                                }
                                Err(e) => {
                                    eprintln!("[CMD] Error handling command from master: {}", e);
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

/// 主节点模式 - 启动Redis服务器并监听端口
///
/// 参数：
/// - port: 监听端口
/// - config: 配置信息
///
/// 返回：
/// - AppState: 应用状态，包含数据库、配置、副本连接等
///
/// 功能：
/// 1. 绑定TCP端口，监听客户端连接
/// 2. 创建数据库和配置
/// 3. 启动过期键清理任务（每100ms）
/// 4. 为每个客户端连接启动处理任务
async fn start_master_mode(port: u16, config: &config::Config) -> Arc<AppState> {
    // 绑定TCP端口
    let addr = format!("127.0.0.1:{}", port);
    let listener = TcpListener::bind(addr).await.unwrap();

    // 创建数据库和配置
    let db = create_database();
    let config = config.clone();

    // 创建WAIT命令的广播通道，用于等待副本ACK
    let (wait_acks_tx, _wait_acks_rx) = tokio::sync::broadcast::channel::<u64>(100);

    // 创建应用状态
    let app_state = AppState {
        config: Arc::new(std::sync::Mutex::new(config)),
        db: db.clone(),
        replicas: Arc::new(std::sync::Mutex::new(Vec::new())),
        wait_acks_tx: Arc::new(Mutex::new(Some(wait_acks_tx))),
    };
    let app_state = Arc::new(app_state);

    // 启动过期键清理任务（每100ms检查一次）
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
                                    drop(guard);
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
                                    drop(guard);
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
                            let mut response: Vec<u8> = vec![];
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
