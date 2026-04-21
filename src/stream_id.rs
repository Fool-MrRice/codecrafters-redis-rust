//! Redis Stream ID 处理模块
//!
//! 提供 Stream ID 的生成、验证、比较等功能
//!
//! ID 格式：`<timestamp>-<sequence>`
//! - `timestamp`: 毫秒级时间戳
//! - `sequence`: 序列号，同一毫秒内递增
//!
//! 支持的 ID 格式：
//! 1. `*`: 自动生成完整的 ID（时间戳 + 序列号）
//! 2. `<timestamp>-*`: 使用指定时间戳，自动生成序列号
//! 3. `<timestamp>-<sequence>`: 显式指定完整 ID

use std::time::SystemTime;

/// 解析并处理 Redis 流条目 ID
///
/// # 参数
/// * `id` - 输入的 ID 字符串
/// * `current_entries` - 当前流中的所有条目
///
/// # 返回值
/// * `Ok(String)` - 处理后的有效 ID
/// * `Err(String)` - 错误信息
///
/// # 支持的格式
/// 1. `*`: 自动生成完整的 ID（时间戳 + 序列号）
/// 2. `<timestamp>-*`: 使用指定时间戳，自动生成序列号
/// 3. `<timestamp>-<sequence>`: 显式指定完整 ID
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
///
/// # 参数
/// * `current_entries` - 当前流中的所有条目
///
/// # 返回值
/// * 生成的完整 ID 字符串
fn generate_full_id(current_entries: &[crate::storage::StreamEntry]) -> String {
    let timestamp = current_timestamp();
    let sequence = calculate_next_sequence(timestamp, current_entries);
    format!("{}-{}", timestamp, sequence)
}

/// 自动生成仅序列号（使用指定的时间戳）
///
/// # 参数
/// * `id_prefix` - ID 前缀，格式为 `<timestamp>-*`
/// * `current_entries` - 当前流中的所有条目
///
/// # 返回值
/// * 生成的完整 ID 字符串
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
///
/// # 参数
/// * `id` - 要验证的 ID 字符串
///
/// # 返回值
/// * `Ok(String)` - 验证通过的 ID
/// * `Err(String)` - 错误信息
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
///
/// # 参数
/// * `id1` - 第一个 ID
/// * `id2` - 第二个 ID
///
/// # 返回值
/// * `true` - 如果 id1 > id2
/// * `false` - 否则
pub fn is_id_greater(id1: &str, id2: &str) -> bool {
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
///
/// # 参数
/// * `id` - 要验证的 ID 字符串
/// * `current_entries` - 当前流中的所有条目
///
/// # 返回值
/// * `Ok(String)` - 验证通过的 ID
/// * `Err(String)` - 错误信息
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
///
/// 对于给定的时间戳，找出相同时间戳的最大序列号并加 1
/// 如果没有相同时间戳的条目，返回 0
///
/// # 参数
/// * `timestamp` - 时间戳
/// * `current_entries` - 当前流中的所有条目
///
/// # 返回值
/// * 下一个序列号
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
        .max();

    match max_sequence {
        Some(max) => max + 1,
        None => 0,
    }
}

/// 计算下一个序列号（用于部分自动生成的 ID 格式）
///
/// 与 calculate_next_sequence 类似，但有一个例外：
/// 如果时间戳为 0 且没有相同时间戳的条目，序列号从 1 开始
///
/// # 参数
/// * `timestamp` - 时间戳
/// * `current_entries` - 当前流中的所有条目
///
/// # 返回值
/// * 下一个序列号
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
        None => {
            // 唯一的例外是时间部分为 0，在这种情况下默认序列号从 1 开始
            if timestamp == 0 { 1 } else { 0 }
        }
    }
}

/// 获取当前时间戳（毫秒）
///
/// # 返回值
/// * 从 UNIX 纪元开始的毫秒数
fn current_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

/// 检查 ID 是否在指定范围内
///
/// # 参数
/// * `id` - 要检查的 ID
/// * `start` - 起始 ID（`-` 表示最小值）
/// * `end` - 结束 ID（`+` 表示最大值）
///
/// # 返回值
/// * `true` - 如果 ID 在 [start, end] 范围内
/// * `false` - 否则
pub fn is_id_in_range(id: &str, start: &str, end: &str) -> bool {
    // 处理特殊的 start ID
    let (start_timestamp, start_sequence) = if start == "-" {
        (0, 0)
    } else {
        let start_parts: Vec<&str> = start.split('-').collect();
        let start_timestamp = start_parts[0].parse::<u64>().unwrap_or(0);
        let start_sequence = if start_parts.len() > 1 {
            start_parts[1].parse::<u64>().unwrap_or(0)
        } else {
            0
        };
        (start_timestamp, start_sequence)
    };

    // 处理特殊的 end ID
    let (end_timestamp, end_sequence) = if end == "+" {
        (u64::MAX, u64::MAX)
    } else {
        let end_parts: Vec<&str> = end.split('-').collect();
        let end_timestamp = end_parts[0].parse::<u64>().unwrap_or(u64::MAX);
        let end_sequence = if end_parts.len() > 1 {
            end_parts[1].parse::<u64>().unwrap_or(u64::MAX)
        } else {
            u64::MAX
        };
        (end_timestamp, end_sequence)
    };

    // 处理当前 ID
    let id_parts: Vec<&str> = id.split('-').collect();
    if id_parts.len() != 2 {
        return false;
    }

    let id_timestamp = id_parts[0].parse::<u64>().unwrap_or(0);
    let id_sequence = id_parts[1].parse::<u64>().unwrap_or(0);

    // 检查是否在范围内
    if id_timestamp < start_timestamp {
        return false;
    } else if id_timestamp > end_timestamp {
        return false;
    } else if id_timestamp == start_timestamp && id_sequence < start_sequence {
        return false;
    } else if id_timestamp == end_timestamp && id_sequence > end_sequence {
        return false;
    }

    true
}
