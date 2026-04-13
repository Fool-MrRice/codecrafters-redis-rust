use crate::handle::{
    handle_echo, handle_get, handle_llen, handle_lpop, handle_lpush, handle_lrange, handle_rpush,
    handle_set, handle_type, handle_xadd,
};
use crate::utils::resp::{RespValue, deserialize_resp};

use crate::utils::case::to_uppercase;
use std::sync::MutexGuard;

pub fn command_handler(
    data: &[u8],
    db: &mut MutexGuard<'_, crate::storage::DatabaseInner>,
) -> Result<Vec<u8>, String> {
    let resp = deserialize_resp(data)?;

    match resp {
        RespValue::Array(Some(a)) => {
            if let Some(RespValue::BulkString(Some(cmd))) = a.get(0) {
                let cmd_upper = to_uppercase(cmd);
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
                    _ => Ok(b"-ERR unknown command\r\n".to_vec()),
                }
            } else {
                Ok(b"-ERR unknown command\r\n".to_vec())
            }
        }
        _ => Ok(b"+PONG\r\n".to_vec()),
    }
}
