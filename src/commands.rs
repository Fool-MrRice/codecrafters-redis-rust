use crate::handle::{
    handle_echo, handle_get, handle_lrange, handle_ping, handle_rpush, handle_set,
    handle_unknown_command,
};
use crate::resp::{RespValue, deserialize_resp};
use crate::storage::ValueWithExpiry;
use crate::utils::to_uppercase;
use std::collections::HashMap;
use std::io::Write;
use std::sync::MutexGuard;

pub fn handle_command<W: Write>(
    stream: &mut W,
    data: &[u8],
    db: &mut MutexGuard<HashMap<String, ValueWithExpiry>>,
) -> Result<(), String> {
    let resp = deserialize_resp(data)?;

    match resp {
        RespValue::Array(a) => {
            if let Some(RespValue::BulkString(Some(cmd))) = a.get(0) {
                let cmd_upper = to_uppercase(cmd);
                match cmd_upper.as_str() {
                    "PING" => handle_ping(stream),
                    "ECHO" => handle_echo(stream, &a),
                    "SET" => handle_set(stream, &a, db),
                    "GET" => handle_get(stream, &a, db),
                    "RPUSH" => handle_rpush(stream, &a, db),
                    "LRANGE" => handle_lrange(stream, &a, db),
                    _ => handle_unknown_command(stream),
                }
            } else {
                handle_unknown_command(stream);
            }
        }
        _ => {
            stream.write_all(b"+PONG\r\n").map_err(|e| e.to_string())?;
        }
    }

    Ok(())
}
