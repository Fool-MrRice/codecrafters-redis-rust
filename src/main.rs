// #![allow(unused_imports)]
pub mod resp;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::thread;
fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    // Uncomment the code below to pass the first stage
    //
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    loop {
        for stream in listener.incoming() {
            match stream {
                Ok(mut stream) => {
                    println!("accepted new connection");
                    thread::spawn(move || {
                        let mut buf = [0u8; 1024];
                        loop {
                            let n = stream.read(&mut buf).unwrap();
                            if n == 0 {
                                break;
                            }
                            let resp = resp::deserialize_resp(&buf[..n]).unwrap();
                            match resp {
                                resp::RespValue::Array(a) => match a.get(0) {
                                    Some(t)
                                        if t == &resp::RespValue::BulkString(Some(
                                            "PING".to_string(),
                                        )) =>
                                    {
                                        stream
                                            .write_all(
                                                resp::serialize_resp(resp::RespValue::BulkString(
                                                    Some("PONG".to_string()),
                                                ))
                                                .as_slice(),
                                            )
                                            .unwrap();
                                    }
                                    Some(t)
                                        if t == &resp::RespValue::BulkString(Some(
                                            "ECHO".to_string(),
                                        )) =>
                                    {
                                        stream
                                            .write_all(
                                                resp::serialize_resp(a.get(1).unwrap().clone())
                                                    .as_slice(),
                                            )
                                            .unwrap();
                                    }
                                    _ => {
                                        stream
                                            .write_all(
                                                resp::serialize_resp(resp::RespValue::BulkString(
                                                    Some("PONG".to_string()),
                                                ))
                                                .as_slice(),
                                            )
                                            .unwrap();
                                    }
                                },
                                _ => {
                                    stream
                                        .write_all(
                                            resp::serialize_resp(resp::RespValue::BulkString(
                                                Some("PONG".to_string()),
                                            ))
                                            .as_slice(),
                                        )
                                        .unwrap();
                                }
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
