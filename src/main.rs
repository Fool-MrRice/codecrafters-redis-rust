// #![allow(unused_imports)]
mod commands;
mod resp;
mod storage;
mod utils;

use crate::commands::handle_command;
use crate::storage::{cleanup_expired_keys, create_database};
use std::io::Read;
use std::net::TcpListener;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    // Uncomment the code below to pass the first stage
    //
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    let db = create_database();

    // 启动定期删除线程
    let db_clone = Arc::clone(&db);
    thread::spawn(move || {
        loop {
            // 每100毫秒执行一次
            thread::sleep(Duration::from_millis(100));
            cleanup_expired_keys(&db_clone);
        }
    });

    loop {
        for stream in listener.incoming() {
            match stream {
                Ok(mut stream) => {
                    println!("accepted new connection");
                    let db = Arc::clone(&db);
                    thread::spawn(move || {
                        let mut buf = [0u8; 1024];
                        loop {
                            let n = match stream.read(&mut buf) {
                                Ok(n) if n == 0 => break,
                                Ok(n) => n,
                                Err(e) => {
                                    eprintln!("Error reading from stream: {}", e);
                                    break;
                                }
                            };

                            let mut db = match db.lock() {
                                Ok(guard) => guard,
                                Err(e) => {
                                    eprintln!("Error locking database: {}", e);
                                    break;
                                }
                            };

                            if let Err(e) = handle_command(&mut stream, &buf[..n], &mut db) {
                                eprintln!("Error handling command: {}", e);
                            }

                            buf.fill(0);
                        }
                    });
                }
                Err(e) => {
                    println!("error: {}", e);
                }
            }
        }
    }
}
