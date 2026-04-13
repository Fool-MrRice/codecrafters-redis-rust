use std::time::SystemTime;

/// 解析并处理 Redis 流条目 ID
/// 支持三种格式：
/// 1. 显式 ID: "1526919030474-0"
/// 2. 自动生成序列号: "1526919030474-*"
/// 3. 自动生成时间和序列号: "*"
pub fn process_stream_id(
    id: &str,
    current_entries: &[crate::storage::StreamEntry],
) -> Result<String, String> {
    match id {
        "*" => Ok(generate_full_id(current_entries)),
        s if s.ends_with("-*") => Ok(generate_sequence_only(s, current_entries)),
        s => validate_explicit_id_against_last(s, current_entries),
    }
}

/// 自动生成完整的 ID（时间戳和序列号）
fn generate_full_id(current_entries: &[crate::storage::StreamEntry]) -> String {
    let timestamp = current_timestamp();
    let sequence = calculate_next_sequence(timestamp, current_entries);
    format!("{}-{}", timestamp, sequence)
}

/// 自动生成仅序列号（使用指定的时间戳）
fn generate_sequence_only(
    id_prefix: &str,
    current_entries: &[crate::storage::StreamEntry],
) -> String {
    // 提取时间戳部分
    let timestamp_str = id_prefix.trim_end_matches("-*");
    if let Ok(timestamp) = timestamp_str.parse::<u64>() {
        let sequence = calculate_next_sequence_for_partial(timestamp, current_entries);
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
        return Err("ERR Invalid ID format".to_string());
    }

    let (timestamp_str, sequence_str) = (parts[0], parts[1]);

    // 验证时间戳
    if let Ok(timestamp) = timestamp_str.parse::<u64>() {
        // 验证序列号
        if let Ok(sequence) = sequence_str.parse::<u64>() {
            // 验证 ID 是否大于 0-0
            if timestamp == 0 && sequence == 0 {
                return Err("ERR The ID specified in XADD must be greater than 0-0".to_string());
            }
            Ok(id.to_string())
        } else {
            Err("ERR Invalid sequence number".to_string())
        }
    } else {
        Err("ERR Invalid timestamp".to_string())
    }
}

/// 比较两个 ID 的大小
/// 返回 true 如果 id1 > id2
fn is_id_greater(id1: &str, id2: &str) -> bool {
    let parts1: Vec<&str> = id1.split('-').collect();
    let parts2: Vec<&str> = id2.split('-').collect();

    if parts1.len() != 2 || parts2.len() != 2 {
        return false;
    }

    if let (Ok(timestamp1), Ok(sequence1), Ok(timestamp2), Ok(sequence2)) = (
        parts1[0].parse::<u64>(),
        parts1[1].parse::<u64>(),
        parts2[0].parse::<u64>(),
        parts2[1].parse::<u64>(),
    ) {
        if timestamp1 > timestamp2 {
            return true;
        } else if timestamp1 == timestamp2 {
            return sequence1 > sequence2;
        }
    }

    false
}

/// 验证显式 ID 是否严格大于流中的最后一个 ID
pub fn validate_explicit_id_against_last(
    id: &str,
    current_entries: &[crate::storage::StreamEntry],
) -> Result<String, String> {
    // 先验证 ID 格式是否正确
    let validated_id = validate_explicit_id(id)?;

    // 如果流为空，直接返回 ID
    if current_entries.is_empty() {
        return Ok(validated_id);
    }

    // 获取最后一个条目的 ID
    let last_id = &current_entries.last().unwrap().id;

    // 比较两个 ID
    if is_id_greater(&validated_id, last_id) {
        Ok(validated_id)
    } else {
        Err(
            "ERR The ID specified in XADD is equal or smaller than the target stream top item"
                .to_string(),
        )
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

/// 计算下一个序列号（用于部分自动生成的ID格式）
fn calculate_next_sequence_for_partial(
    timestamp: u64,
    current_entries: &[crate::storage::StreamEntry],
) -> u64 {
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
        .max();

    match max_sequence {
        Some(max) => max + 1,
        None => 0,
    }
}

/// 获取当前时间戳（毫秒）
fn current_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}
