// #![allow(unused_imports)]
pub mod resp;
use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::{Arc, Mutex};
use std::thread;
fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    // Uncomment the code below to pass the first stage
    //
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    let db = Arc::new(Mutex::new(HashMap::new()));

    loop {
        for stream in listener.incoming() {
            match stream {
                Ok(mut stream) => {
                    println!("accepted new connection");
                    let db = Arc::clone(&db);
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
                                                resp::serialize_resp(
                                                    resp::RespValue::SimpleString(
                                                        "PONG".to_string(),
                                                    ),
                                                )
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
                                    Some(t)
                                        if t == &resp::RespValue::BulkString(Some(
                                            "SET".to_string(),
                                        )) =>
                                    {
                                        if let (
                                            Some(resp::RespValue::BulkString(Some(key))),
                                            Some(resp::RespValue::BulkString(Some(value))),
                                        ) = (a.get(1), a.get(2))
                                        {
                                            let mut db = db.lock().unwrap();
                                            db.insert(key.clone(), value.clone());
                                            stream.write_all(b"+OK\r\n").unwrap();
                                        }
                                    }
                                    Some(t)
                                        if t == &resp::RespValue::BulkString(Some(
                                            "GET".to_string(),
                                        )) =>
                                    {
                                        if let Some(resp::RespValue::BulkString(Some(key))) =
                                            a.get(1)
                                        {
                                            let db = db.lock().unwrap();
                                            match db.get(key) {
                                                Some(value) => {
                                                    stream
                                                        .write_all(
                                                            resp::serialize_resp(
                                                                resp::RespValue::BulkString(Some(
                                                                    value.clone(),
                                                                )),
                                                            )
                                                            .as_slice(),
                                                        )
                                                        .unwrap();
                                                }
                                                None => {
                                                    stream
                                                        .write_all(
                                                            resp::serialize_resp(
                                                                resp::RespValue::BulkString(None),
                                                            )
                                                            .as_slice(),
                                                        )
                                                        .unwrap();
                                                }
                                            }
                                        }
                                    }
                                    _ => {
                                        continue;
                                    }
                                },
                                _ => {
                                    continue;
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
