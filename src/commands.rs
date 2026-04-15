use crate::handle::{
    handle_echo, handle_get, handle_incr, handle_llen, handle_lpop, handle_lpush, handle_lrange,
    handle_rpush, handle_set, handle_type, handle_xadd, handle_xrange,
};
use crate::utils::resp::{RespValue, deserialize_resp, serialize_resp};

use crate::utils::case::to_uppercase;
use std::sync::MutexGuard;

pub fn command_handler(
    data: &[u8],
    db: &mut MutexGuard<'_, crate::storage::DatabaseInner>,
    in_transaction: &mut bool,
    command_queue: &mut Vec<Vec<u8>>,
) -> Result<Vec<u8>, String> {
    let resp = deserialize_resp(data)?;

    match resp {
        RespValue::Array(Some(a)) => {
            if let Some(RespValue::BulkString(Some(cmd))) = a.first() {
                let cmd_upper = to_uppercase(cmd);
                // 处理事务控制命令
                match cmd_upper.as_str() {
                    "MULTI" => {
                        *in_transaction = true;
                        command_queue.clear();
                        Ok(serialize_resp(RespValue::SimpleString("OK".to_string())))
                    }
                    "EXEC" => {
                        if !*in_transaction {
                            Ok(serialize_resp(RespValue::Error(
                                "ERR EXEC without MULTI".to_string(),
                            )))
                        } else {
                            *in_transaction = false;
                            // 事务执行命令响应队列
                            let mut responses = Vec::new();
                            for cmd_data in command_queue.drain(..) {
                                if let Ok(cmd_resp) =
                                    command_handler(&cmd_data, db, &mut false, &mut Vec::new())
                                {
                                    responses.push(cmd_resp);
                                } else {
                                    responses.push(serialize_resp(RespValue::Error(
                                        "ERR command execution failed".to_string(),
                                    )));
                                }
                            }
                            // 构建响应数组
                            // 这里没有想明白如何使用resp来构建响应数组，所以直接使用format!来构建
                            let mut result = Vec::new();
                            result.extend(format!("*{}\r\n", responses.len()).as_bytes());
                            for resp in responses {
                                result.extend(resp);
                            }
                            Ok(result)
                        }
                    }
                    "DISCARD" => {
                        if !*in_transaction {
                            Ok(serialize_resp(RespValue::Error(
                                "ERR DISCARD without MULTI".to_string(),
                            )))
                        } else {
                            *in_transaction = false;
                            command_queue.clear();
                            Ok(serialize_resp(RespValue::SimpleString("OK".to_string())))
                        }
                    }
                    _ => {
                        // 其他命令
                        if *in_transaction {
                            // 事务中，将命令加入队列
                            command_queue.push(data.to_vec());
                            Ok(serialize_resp(RespValue::SimpleString(
                                "QUEUED".to_string(),
                            )))
                        } else {
                            // 非事务中，立即执行命令
                            match cmd_upper.as_str() {
                                "PING" => Ok(b"+PONG\r\n".to_vec()),
                                "ECHO" => handle_echo(&a),
                                "SET" => handle_set(&a, db),
                                "GET" => handle_get(&a, db),
                                "RPUSH" => handle_rpush(&a, db),
                                "LPUSH" => handle_lpush(&a, db),
                                "LRANGE" => handle_lrange(&a, db),
                                "LLEN" => handle_llen(&a, db),
                                "LPOP" => handle_lpop(&a, db),
                                "TYPE" => handle_type(&a, db),
                                "XADD" => handle_xadd(&a, db),
                                "XRANGE" => handle_xrange(&a, db),
                                "INCR" => handle_incr(&a, db),
                                _ => Ok(b"-ERR unknown command\r\n".to_vec()),
                            }
                        }
                    }
                }
            } else {
                if *in_transaction {
                    // 事务中，将命令加入队列
                    command_queue.push(data.to_vec());
                    Ok(b"+QUEUED\r\n".to_vec())
                } else {
                    Ok(b"-ERR unknown command\r\n".to_vec())
                }
            }
        }
        _ => {
            if *in_transaction {
                // 事务中，将命令加入队列
                command_queue.push(data.to_vec());
                Ok(b"+QUEUED\r\n".to_vec())
            } else {
                Ok(b"+PONG\r\n".to_vec())
            }
        }
    }
}
