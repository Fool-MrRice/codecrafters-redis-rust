//! 字符串处理工具模块
//!
//! 提供大小写不敏感的字符串比较、转换等功能

/// 大小写不敏感的字符串比较
///
/// # 参数
/// * `a` - 第一个字符串
/// * `b` - 第二个字符串
///
/// # 返回值
/// * `true` - 如果两个字符串忽略大小写后相等
/// * `false` - 否则
#[allow(dead_code)]
pub fn case_insensitive_eq(a: &str, b: &str) -> bool {
    a.eq_ignore_ascii_case(b)
}

/// 将字符串转换为大写
///
/// # 参数
/// * `s` - 输入字符串
///
/// # 返回值
/// * 转换为大写后的字符串
pub fn to_uppercase(s: &str) -> String {
    s.to_uppercase()
}


/// 检查字符串是否以指定前缀开头（大小写不敏感）
///
/// # 参数
/// * `s` - 输入字符串
/// * `prefix` - 要检查的前缀
///
/// # 返回值
/// * `true` - 如果字符串以指定前缀开头（忽略大小写）
/// * `false` - 否则
#[allow(dead_code)]
pub fn starts_with_ignore_case(s: &str, prefix: &str) -> bool {
    s.starts_with(prefix)
}
