// RESP 类型枚举
#[derive(Debug, PartialEq, Clone)]
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
                // "*-1\r\n".to_string().into_bytes()
                b"*-1\r\n".to_vec()
            }
        }
    }
}

// 反序列化：RESP 字节流 → String 值
#[allow(dead_code)]
pub fn deserialize_resp(data: &[u8]) -> Result<RespValue, String> {
    let input = String::from_utf8_lossy(data)
        .trim_end_matches("\r\n")
        .to_string();

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
            let parts: Vec<&str> = input.splitn(2, "\r\n").collect();
            if parts.len() < 2 {
                return Err("Invalid bulk string format".to_string());
            }

            let len_str = parts[0][1..].trim();
            if len_str == "-1" {
                Ok(RespValue::BulkString(None))
            } else {
                let len = len_str
                    .parse::<usize>()
                    .map_err(|e| format!("Failed to parse bulk string length: {}", e))?;
                let content = parts[1].to_string();
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
            let mut parts = input.splitn(2, "\r\n");
            let len_str = parts.next().unwrap_or("")[1..].trim();

            if len_str == "-1" {
                return Ok(RespValue::Array(Vec::new()));
            }

            let len = len_str
                .parse::<usize>()
                .map_err(|e| format!("Failed to parse array length: {}", e))?;

            let elements_str = parts.next().unwrap_or("");
            let mut elements = Vec::with_capacity(len);
            let mut pos = 0;

            for _ in 0..len {
                if pos >= elements_str.len() {
                    return Err("Incomplete array elements".to_string());
                }

                let start = pos;
                // 找到元素的结束位置（\r\n）
                let end = match elements_str[start..].find("\r\n") {
                    Some(idx) => start + idx + 2,
                    None => return Err("Invalid array element format".to_string()),
                };

                // 递归解析元素
                let element_data = elements_str[start..end].as_bytes();
                let element = deserialize_resp(element_data)?;
                elements.push(element);

                pos = end;
            }

            Ok(RespValue::Array(elements))
        }
        _ => Err(format!("Unknown RESP type: {}", input)),
    }
}
