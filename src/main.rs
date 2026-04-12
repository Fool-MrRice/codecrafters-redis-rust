// #![allow(unused_imports)]
use codecrafters_redis::commands::command_handler;
use codecrafters_redis::storage::{cleanup_expired_keys, create_database};
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

                // Process the command with the database lock
                let response = match db.lock() {
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
