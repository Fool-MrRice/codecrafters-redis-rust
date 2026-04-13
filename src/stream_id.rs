
use std::time::SystemTime;

/// 解析并处理 Redis 流条目 ID
/// 支持三种格式：
/// 1. 显式 ID: "1526919030474-0"
/// 2. 自动生成序列号: "1526919030474-*"
/// 3. 自动生成时间和序列号: "*"
pub fn process_stream_id(
    id: &str,
    current_entries: &[crate::storage::StreamEntry],
) -> String {
    match id {
        "*" => generate_full_id(current_entries),
        s if s.ends_with("-*") => generate_sequence_only(s, current_entries),
        s => validate_explicit_id(s).unwrap_or_else(|_| generate_full_id(current_entries)),
    }
}

/// 自动生成完整的 ID（时间戳和序列号）
fn generate_full_id(current_entries: &[crate::storage::StreamEntry]) -> String {
    let timestamp = current_timestamp();
    let sequence = calculate_next_sequence(timestamp, current_entries);
    format!("{}-{}", timestamp, sequence)
}

/// 自动生成仅序列号（使用指定的时间戳）
fn generate_sequence_only(id_prefix: &str, current_entries: &[crate::storage::StreamEntry]) -> String {
    // 提取时间戳部分
    let timestamp_str = id_prefix.trim_end_matches("-*");
    if let Ok(timestamp) = timestamp_str.parse::<u64>() {
        let sequence = calculate_next_sequence(timestamp, current_entries);
        format!("{}-{}", timestamp, sequence)
    } else {
        // 如果时间戳格式不正确，使用当前时间戳
        generate_full_id(current_entries)
    }
}

/// 验证显式 ID 是否合理
fn validate_explicit_id(id: &str) -> Result<String, String> {
    let parts: Vec<&str> = id.split('-').collect();
    if parts.len() != 2 {
        return Err("Invalid ID format".to_string());
    }

    let (timestamp_str, sequence_str) = (parts[0], parts[1]);
    
    // 验证时间戳
    if timestamp_str.parse::<u64>().is_ok() {
        // 验证序列号
        if sequence_str.parse::<u64>().is_ok() {
            Ok(id.to_string())
        } else {
            Err("Invalid sequence number".to_string())
        }
    } else {
        Err("Invalid timestamp".to_string())
    }
}

/// 计算下一个序列号
fn calculate_next_sequence(timestamp: u64, current_entries: &[crate::storage::StreamEntry]) -> u64 {
    // 找出相同时间戳的最大序列号
    let max_sequence = current_entries
        .iter()
        .filter_map(|entry| {
            let parts: Vec<&str> = entry.id.split('-').collect();
            if parts.len() == 2 {
                if let Ok(entry_timestamp) = parts[0].parse::<u64>() {
                    if entry_timestamp == timestamp {
                        parts[1].parse::<u64>().ok()
                    } else {
                        None
                    }
                } else {
                    None
                }
            } else {
                None
            }
        })
        .max()
        .unwrap_or(0);

    max_sequence + 1
}

/// 获取当前时间戳（毫秒）
fn current_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}
