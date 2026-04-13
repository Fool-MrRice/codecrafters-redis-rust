// #![allow(unused_imports)]
use codecrafters_redis::blocking::{prepare_blpop, prepare_xread, wait_for_blocked_command};
use codecrafters_redis::commands::command_handler;
use codecrafters_redis::storage::cleanup_expired_keys;
use codecrafters_redis::storage::create_database;
use codecrafters_redis::utils::resp::{RespValue, deserialize_resp};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::time::Duration;

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

                // 检测是否是 BLPOP 或 XREAD 命令
                let command_type = {
                    if let Ok(resp) = deserialize_resp(&data) {
                        if let RespValue::Array(Some(a)) = resp {
                            if let Some(RespValue::BulkString(Some(cmd))) = a.get(0) {
                                let cmd_upper = cmd.to_uppercase();
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

                // Process the command
                let response = match command_type {
                    "BLPOP" => {
                        // 处理 BLPOP 命令
                        if let Ok(resp) = deserialize_resp(&data) {
                            if let RespValue::Array(Some(a)) = resp {
                                // 准备 BLPOP 命令
                                let blocked_result = match db.lock() {
                                    Ok(mut guard) => prepare_blpop(&a, &mut guard).unwrap(),
                                    Err(e) => {
                                        eprintln!("Error locking database: {}", e);
                                        return;
                                    }
                                };

                                // 等待阻塞命令的结果
                                wait_for_blocked_command(blocked_result).await
                            } else {
                                b"-ERR unknown command\r\n".to_vec()
                            }
                        } else {
                            b"-ERR unknown command\r\n".to_vec()
                        }
                    }
                    "XREAD" => {
                        // 处理 XREAD 命令
                        if let Ok(resp) = deserialize_resp(&data) {
                            if let RespValue::Array(Some(a)) = resp {
                                // 准备 XREAD 命令
                                let blocked_result = match db.lock() {
                                    Ok(mut guard) => prepare_xread(&a, &mut guard).unwrap(),
                                    Err(e) => {
                                        eprintln!("Error locking database: {}", e);
                                        return;
                                    }
                                };

                                // 等待阻塞命令的结果
                                wait_for_blocked_command(blocked_result).await
                            } else {
                                b"-ERR unknown command\r\n".to_vec()
                            }
                        } else {
                            b"-ERR unknown command\r\n".to_vec()
                        }
                    }
                    _ => {
                        // 正常处理其他命令
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
