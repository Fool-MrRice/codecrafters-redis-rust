// RESP 类型枚举
#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub enum RespValue {
    SimpleString(String),
    Error(String),
    Integer(i64),
    BulkString(Option<String>), // None 表示空值
    Array(Vec<RespValue>),
}

// String 值 → RESP 字节流
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
                // "$-1\r\n".to_string().into_bytes()
                b"$-1\r\n".to_vec()
            }
        }
        RespValue::Array(a) => {
            if !a.is_empty() {
                let mut result = format!("*{}\r\n", a.len()).into_bytes();
                for item in a {
                    result.extend(serialize_resp(item));
                }
                result
            } else {
                // 空数组应该序列化为 *0\r\n，而不是 *-1\r\n
                b"*0\r\n".to_vec()
            }
        }
    }
}

// 反序列化：RESP 字节流 → String 值
#[allow(dead_code)]
pub fn deserialize_resp(data: &[u8]) -> Result<RespValue, String> {
    let input = String::from_utf8_lossy(data).to_string();

    match input.chars().next() {
        Some('+') => Ok(RespValue::SimpleString(input[1..].to_string())),
        Some('-') => Ok(RespValue::Error(input[1..].to_string())),
        Some(':') => {
            let num = input[1..]
                .parse::<i64>()
                .map_err(|e| format!("Failed to parse integer: {}", e))?;
            Ok(RespValue::Integer(num))
        }
        Some('$') => {
            let mut parts = input.splitn(2, "\r\n");
            let len_part = parts.next().ok_or("Invalid bulk string format")?;
            let content_part = parts.next().ok_or("Invalid bulk string format")?;

            let len_str = len_part[1..].trim();
            if len_str == "-1" {
                Ok(RespValue::BulkString(None))
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
                Ok(RespValue::BulkString(Some(content)))
            }
        }
        Some('*') => {
            let mut lines = input.split("\r\n");
            let len_line = lines.next().ok_or("Invalid array format")?;
            let len_str = len_line[1..].trim();

            if len_str == "-1" {
                return Ok(RespValue::Array(Vec::new()));
            }

            let len = len_str
                .parse::<usize>()
                .map_err(|e| format!("Failed to parse array length: {}", e))?;

            let mut elements = Vec::with_capacity(len);

            for _ in 0..len {
                let element_line = lines.next().ok_or("Incomplete array elements")?;
                if element_line.is_empty() {
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
                } else {
                    // 处理其他类型
                    let element_data = element_line.as_bytes();
                    let element = deserialize_resp(element_data)?;
                    elements.push(element);
                }
            }

            Ok(RespValue::Array(elements))
        }
        _ => Err(format!("Unknown RESP type: {}", input)),
    }
}
