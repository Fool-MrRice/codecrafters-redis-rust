#![allow(unused_imports)]
use std::io::{Read, Write};
use std::net::TcpListener;
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
                    let mut buf = [0u8; 1024];
                    loop {
                        let n = stream.read(&mut buf).unwrap();
                        if n == 0 {
                            break;
                        }
                        stream.write_all(b"+PONG\r\n").unwrap();
                    }
                }
                Err(e) => {
                    println!("error: {}", e);
                }
            }
        }
    }
}
