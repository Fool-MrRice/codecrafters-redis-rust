// #![allow(unused_imports)]
use codecrafters_redis::commands::command_handler;
use codecrafters_redis::resp::{RespValue, deserialize_resp, serialize_resp};
use codecrafters_redis::storage::{
    BlockedClient, RedisValue, cleanup_expired_keys, create_database, is_expired,
};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::time::Duration;

fn current_timestamp() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

#[tokio::main]
async fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    // Uncomment the code below to pass the first stage
    //
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    let db = create_database();

    // 启动定期删除任务
    let db_clone = Arc::clone(&db);
    tokio::spawn(async move {
        loop {
            // 每100毫秒执行一次
            tokio::time::sleep(Duration::from_millis(100)).await;
            cleanup_expired_keys(&db_clone);
        }
    });

    loop {
        let (mut stream, _) = listener.accept().await.unwrap();
        println!("accepted new connection");
        let db = Arc::clone(&db);
        tokio::spawn(async move {
            let mut buf = [0u8; 1024];
            loop {
                let n = match stream.read(&mut buf).await {
                    Ok(n) if n == 0 => break,
                    Ok(n) => n,
                    Err(e) => {
                        eprintln!("Error reading from stream: {}", e);
                        break;
                    }
                };

                // Read the data first
                let data = buf[..n].to_vec();

                // 检测是否是 BLPOP 命令
                let is_blpop = {
                    if let Ok(resp) = deserialize_resp(&data) {
                        if let RespValue::Array(a) = resp {
                            if let Some(RespValue::BulkString(Some(cmd))) = a.get(0) {
                                let cmd_upper = cmd.to_uppercase();
                                cmd_upper == "BLPOP"
                            } else {
                                false
                            }
                        } else {
                            false
                        }
                    } else {
                        false
                    }
                };

                // Process the command with the database lock
                let response = if is_blpop {
                    // 特殊处理 BLPOP 命令
                    if let Ok(resp) = deserialize_resp(&data) {
                        if let RespValue::Array(a) = resp {
                            if let (
                                Some(RespValue::BulkString(Some(key))),
                                Some(RespValue::BulkString(Some(timeout_str))),
                            ) = (a.get(1), a.get(2))
                            {
                                let timeout = timeout_str.parse::<u64>().unwrap();

                                // 先检查列表是否有元素
                                let list_has_elements = {
                                    if let Ok(mut guard) = db.lock() {
                                        if let Some(entry) = guard.data.get(key) {
                                            if !is_expired(&entry.expiry) {
                                                if let RedisValue::List(list) = &entry.value {
                                                    !list.is_empty()
                                                } else {
                                                    false
                                                }
                                            } else {
                                                false
                                            }
                                        } else {
                                            false
                                        }
                                    } else {
                                        false
                                    }
                                };

                                if list_has_elements {
                                    // 列表有元素，正常处理
                                    match db.lock() {
                                        Ok(mut guard) => match command_handler(&data, &mut guard) {
                                            Ok(resp) => resp,
                                            Err(e) => {
                                                eprintln!("Error handling command: {}", e);
                                                b"-ERR internal error\r\n".to_vec()
                                            }
                                        },
                                        Err(e) => {
                                            eprintln!("Error locking database: {}", e);
                                            b"-ERR internal error\r\n".to_vec()
                                        }
                                    }
                                } else {
                                    // 列表为空，需要阻塞
                                    let (tx, rx) = tokio::sync::oneshot::channel();

                                    // 注册阻塞客户端
                                    {
                                        if let Ok(mut guard) = db.lock() {
                                            let blocked_client = BlockedClient {
                                                key: key.clone(),
                                                timeout,
                                                start_time: current_timestamp() / 1000,
                                                tx,
                                            };
                                            guard
                                                .blocked_clients
                                                .add_client(key.clone(), blocked_client);
                                        }
                                    }

                                    // 等待通知或超时
                                    let result = if timeout == 0 {
                                        // 无限期阻塞
                                        rx.await.ok()
                                    } else {
                                        // 有限期阻塞
                                        tokio::time::timeout(
                                            tokio::time::Duration::from_secs(timeout),
                                            rx,
                                        )
                                        .await
                                        .ok()
                                        .and_then(|r| r.ok())
                                    };

                                    // 处理结果
                                    match result {
                                        Some((list_name, element)) => {
                                            serialize_resp(RespValue::Array(vec![
                                                RespValue::BulkString(Some(list_name)),
                                                RespValue::BulkString(Some(element)),
                                            ]))
                                        }
                                        None => serialize_resp(RespValue::Array(vec![])),
                                    }
                                }
                            } else {
                                b"-ERR wrong number of arguments for 'blpop' command\r\n".to_vec()
                            }
                        } else {
                            b"-ERR unknown command\r\n".to_vec()
                        }
                    } else {
                        b"-ERR unknown command\r\n".to_vec()
                    }
                } else {
                    // 正常处理其他命令
                    match db.lock() {
                        Ok(mut guard) => {
                            // Handle the command
                            match command_handler(&data, &mut guard) {
                                Ok(resp) => resp,
                                Err(e) => {
                                    eprintln!("Error handling command: {}", e);
                                    b"-ERR internal error\r\n".to_vec()
                                }
                            }
                        }
                        Err(e) => {
                            eprintln!("Error locking database: {}", e);
                            b"-ERR internal error\r\n".to_vec()
                        }
                    }
                };

                // Write the response to the stream
                if let Err(e) = stream.write_all(&response).await {
                    eprintln!("Error writing response: {}", e);
                    break;
                }

                buf.fill(0);
            }
        });
    }
}
