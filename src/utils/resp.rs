//! RESP（Redis Serialization Protocol）协议模块
//!
//! 提供 RESP 协议的序列化和反序列化功能
//!
//! RESP 协议类型：
//! - Simple String: `+内容\r\n`
//! - Error: `-错误\r\n`
//! - Integer: `:数字\r\n`
//! - Bulk String: `$长度\r\n内容\r\n`
//! - Array: `*元素数\r\n`

/// RESP 值枚举
///
/// 表示 Redis 协议中所有可能的值类型
#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub enum RespValue {
    SimpleString(String),
    Error(String),
    Integer(i64),
    BulkString(Option<String>),   // None 表示空值（$-1\r\n）
    Array(Option<Vec<RespValue>>), // None 表示 null 数组（*-1\r\n）
}

/// 将 RespValue 序列化为 RESP 字节流
///
/// # 参数
/// * `value` - 要序列化的 RespValue
///
/// # 返回值
/// * RESP 格式的字节数组
#[allow(dead_code)]
pub fn serialize_resp(value: RespValue) -> Vec<u8> {
    match value {
        RespValue::SimpleString(s) => format!("+{}\r\n", s).into_bytes(),
        RespValue::Error(e) => format!("-{}\r\n", e).into_bytes(),
        RespValue::Integer(i) => format!(":{}\r\n", i).into_bytes(),
        RespValue::BulkString(s) => {
            if let Some(s) = s {
                format!("${}\r\n{}\r\n", s.len(), s).into_bytes()
            } else {
                // $-1\r\n 表示空值
                b"$-1\r\n".to_vec()
            }
        }
        RespValue::Array(a) => {
            match a {
                Some(elements) => {
                    let mut result = format!("*{}\r\n", elements.len()).into_bytes();
                    for item in elements {
                        result.extend(serialize_resp(item));
                    }
                    result
                }
                None => b"*-1\r\n".to_vec(), // null 数组
            }
        }
    }
}

/// 反序列化 RESP 字节流
///
/// # 参数
/// * `data` - RESP 格式的字节数据
///
/// # 返回值
/// * `Ok((RespValue, usize))` - 成功时返回解析出的值和消耗的字节数
/// * `Err(String)` - 失败时返回错误信息
#[allow(dead_code)]
pub fn deserialize_resp(data: &[u8]) -> Result<(RespValue, usize), String> {
    let input = String::from_utf8_lossy(data).to_string();

    match input.chars().next() {
        Some('+') => {
            let content = input[1..].trim_end_matches("\r\n");
            let consumed = 1 + content.len() + 2; // '+' + content + "\r\n"
            Ok((RespValue::SimpleString(content.to_string()), consumed))
        }
        Some('-') => {
            let content = input[1..].trim_end_matches("\r\n");
            let consumed = 1 + content.len() + 2; // '-' + content + "\r\n"
            Ok((RespValue::Error(content.to_string()), consumed))
        }

        Some(':') => {
            let num_str = input[1..].trim_end_matches("\r\n");
            let num = num_str
                .parse::<i64>()
                .map_err(|e| format!("Failed to parse integer: {}", e))?;
            let consumed = 1 + num_str.len() + 2; // ':' + num_str + "\r\n"
            Ok((RespValue::Integer(num), consumed))
        }
        Some('$') => {
            let mut parts = input.splitn(2, "\r\n");
            let len_part = parts.next().ok_or("Invalid bulk string format")?;
            let content_part = parts.next().ok_or("Invalid bulk string format")?;

            let len_str = len_part[1..].trim();
            if len_str == "-1" {
                let consumed = len_part.len() + 2; // len_part + "\r\n"
                Ok((RespValue::BulkString(None), consumed))
            } else {
                let len = len_str
                    .parse::<usize>()
                    .map_err(|e| format!("Failed to parse bulk string length: {}", e))?;
                let content = content_part.to_string();
                if content.len() != len {
                    return Err(format!(
                        "Bulk string length mismatch: expected {}, got {}",
                        len,
                        content.len()
                    ));
                }
                let consumed = len_part.len() + 2 + len + 2; // len_part + "\r\n" + content + "\r\n"
                Ok((RespValue::BulkString(Some(content)), consumed))
            }
        }
        Some('*') => {
            let mut lines = input.split("\r\n");
            let len_line = lines.next().ok_or("Invalid array format")?;
            let len_str = len_line[1..].trim();

            if len_str == "-1" {
                let consumed = len_line.len() + 2; // len_line + "\r\n"
                return Ok((RespValue::Array(None), consumed));
            }

            let len = len_str
                .parse::<usize>()
                .map_err(|e| format!("Failed to parse array length: {}", e))?;

            let mut elements = Vec::with_capacity(len);
            let mut consumed = len_line.len() + 2; // len_line + "\r\n"

            for _ in 0..len {
                let element_line = lines.next().ok_or("Incomplete array elements")?;
                if element_line.is_empty() {
                    consumed += 2; // "\r\n"
                    continue;
                }

                // 处理批量字符串（包含多行）
                let element_type = element_line.chars().next().unwrap();
                if element_type == '$' {
                    let len_str = element_line[1..].trim();
                    let len = len_str
                        .parse::<usize>()
                        .map_err(|e| format!("Failed to parse bulk string length: {}", e))?;

                    let content_line = lines.next().ok_or("Incomplete bulk string")?;
                    if content_line.len() != len {
                        return Err(format!(
                            "Bulk string length mismatch: expected {}, got {}",
                            len,
                            content_line.len()
                        ));
                    }

                    elements.push(RespValue::BulkString(Some(content_line.to_string())));
                    consumed += element_line.len() + 2 + content_line.len() + 2; // element_line + "\r\n" + content_line + "\r\n"
                } else {
                    // 处理其他类型
                    let element_data = element_line.as_bytes();
                    let (element, element_consumed) = deserialize_resp(element_data)?;
                    elements.push(element);
                    consumed += element_consumed;
                }
            }

            Ok((RespValue::Array(Some(elements)), consumed))
        }
        _ => Err(format!("Unknown RESP type: {}", input)),
    }
}

/// 旧版本的反序列化函数，用于向后兼容
///
/// # 参数
/// * `data` - RESP 格式的字节数据
///
/// # 返回值
/// * `Ok(RespValue)` - 成功时返回解析出的值
/// * `Err(String)` - 失败时返回错误信息
#[allow(dead_code)]
pub fn deserialize_resp_old(data: &[u8]) -> Result<RespValue, String> {
    let (resp, _) = deserialize_resp(data)?;
    Ok(resp)
}
