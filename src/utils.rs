
// 工具函数模块

/// 大小写不敏感的字符串比较
pub fn case_insensitive_eq(a: &str, b: &str) -> bool {
    a.eq_ignore_ascii_case(b)
}

/// 将字符串转换为大写
pub fn to_uppercase(s: &str) -> String {
    s.to_uppercase()
}

/// 检查字符串是否以指定前缀开头（大小写不敏感）
pub fn starts_with_ignore_case(s: &str, prefix: &str) -> bool {
    s.starts_with(prefix)
}
