//! RDB文件解析器实现
//!
//! 实现Redis RDB文件格式的完整解析
//! 参考：https://github.com/sripathikrishnan/redis-rdb-tools/wiki/Redis-RDB-Dump-File-Format

use crate::storage::RedisValue;

/// RDB解析错误类型
#[derive(Debug)]
pub enum RdbError {
    InvalidMagic(String),
    InvalidVersion(String),
    InvalidEncoding(u8),
    UnexpectedEof,
    InvalidLength,
    InvalidString,
    InvalidChecksum,
}

impl std::fmt::Display for RdbError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RdbError::InvalidMagic(s) => write!(f, "Invalid magic string: {}", s),
            RdbError::InvalidVersion(s) => write!(f, "Invalid version: {}", s),
            RdbError::InvalidEncoding(b) => write!(f, "Invalid encoding: 0x{:02x}", b),
            RdbError::UnexpectedEof => write!(f, "Unexpected end of file"),
            RdbError::InvalidLength => write!(f, "Invalid length encoding"),
            RdbError::InvalidString => write!(f, "Invalid string encoding"),
            RdbError::InvalidChecksum => write!(f, "Invalid checksum"),
        }
    }
}

impl std::error::Error for RdbError {}

/// 值类型
#[derive(Debug, Clone, Copy)]
#[repr(u8)]
enum ValueType {
    String = 0,
    List = 1,
    Set = 2,
    SortedSet = 3,
    Hash = 4,
    Zipmap = 9,
    Ziplist = 10,
    Intset = 11,
    SortedSetInZiplist = 12,
    HashmapInZiplist = 13,
    ListInQuicklist = 14,
}

impl ValueType {
    fn from_u8(value: u8) -> Option<Self> {
        match value {
            0 => Some(ValueType::String),
            1 => Some(ValueType::List),
            2 => Some(ValueType::Set),
            3 => Some(ValueType::SortedSet),
            4 => Some(ValueType::Hash),
            9 => Some(ValueType::Zipmap),
            10 => Some(ValueType::Ziplist),
            11 => Some(ValueType::Intset),
            12 => Some(ValueType::SortedSetInZiplist),
            13 => Some(ValueType::HashmapInZiplist),
            14 => Some(ValueType::ListInQuicklist),
            _ => None,
        }
    }
}

/// RDB解析结果
pub struct RdbData {
    /// 解析出的键值对：(键, 值, 过期时间毫秒)
    pub keys: Vec<(String, RedisValue, Option<u64>)>,
}

impl RdbData {
    fn new() -> Self {
        RdbData { keys: Vec::new() }
    }
}

/// RDB解析器
pub struct RdbParser {
    data: Vec<u8>,
    pos: usize,
}

impl RdbParser {
    /// 创建新的RDB解析器
    pub fn new(data: Vec<u8>) -> Self {
        RdbParser { data, pos: 0 }
    }

    /// 解析RDB文件
    pub fn parse(&mut self) -> Result<RdbData, RdbError> {
        let mut result = RdbData::new();

        // 1. 解析头部
        self.parse_header()?;

        // 2. 解析元数据部分和数据库部分
        loop {
            if self.pos >= self.data.len() {
                return Err(RdbError::UnexpectedEof);
            }

            let byte = self.peek_byte()?;

            match byte {
                // 元数据部分开始
                0xFA => {
                    self.parse_metadata()?;
                }
                // 数据库部分开始
                0xFE => {
                    self.parse_database(&mut result)?;
                }
                // 文件结束
                0xFF => {
                    self.pos += 1; // 跳过0xFF
                    // 读取CRC64校验和（8字节）
                    if self.pos + 8 <= self.data.len() {
                        self.pos += 8;
                    }
                    break;
                }
                _ => {
                    return Err(RdbError::InvalidEncoding(byte));
                }
            }
        }

        Ok(result)
    }

    /// 解析RDB头部（Magic + Version）
    fn parse_header(&mut self) -> Result<(), RdbError> {
        // 读取Magic字符串 "REDIS" (5字节)
        if self.pos + 9 > self.data.len() {
            return Err(RdbError::UnexpectedEof);
        }

        let magic = &self.data[self.pos..self.pos + 5];
        if magic != b"REDIS" {
            return Err(RdbError::InvalidMagic(
                String::from_utf8_lossy(magic).to_string(),
            ));
        }
        self.pos += 5;

        // 读取版本号 (4字节 ASCII)
        let version = &self.data[self.pos..self.pos + 4];
        let version_str = String::from_utf8_lossy(version);
        self.pos += 4;

        // 验证版本号（本挑战使用0011）
        if version_str != "0011" {
            // 不强制检查版本号，只记录
            eprintln!("Warning: RDB version is {}, expected 0011", version_str);
        }

        Ok(())
    }

    /// 解析元数据部分
    fn parse_metadata(&mut self) -> Result<(), RdbError> {
        // 跳过0xFA标记
        self.pos += 1;

        // 读取属性名（字符串编码）
        let attr_name = self.parse_string()?;

        // 读取属性值（字符串编码）
        let attr_value = self.parse_string()?;

        eprintln!("RDB Metadata: {} = {}", attr_name, attr_value);

        Ok(())
    }

    /// 解析数据库部分
    fn parse_database(&mut self, result: &mut RdbData) -> Result<(), RdbError> {
        // 跳过0xFE标记
        self.pos += 1;

        // 读取数据库索引（长度编码）
        let db_index = self.parse_length()?;
        eprintln!("RDB Database index: {:?}", db_index);

        // 检查是否有数据库选择器（0xFB标记）
        if self.peek_byte()? == 0xFB {
            // 跳过0xFB标记
            self.pos += 1;

            // 读取哈希表大小（长度编码）
            let hash_table_size = self.parse_length()?;
            eprintln!("RDB Hash table size: {:?}", hash_table_size);

            // 读取过期哈希表大小（长度编码）
            let expire_hash_table_size = self.parse_length()?;
            eprintln!("RDB Expire hash table size: {:?}", expire_hash_table_size);
        }

        // 读取键值对
        loop {
            if self.pos >= self.data.len() {
                break;
            }

            let byte = self.peek_byte()?;

            // 检查是否是下一个数据库或文件结束
            if byte == 0xFE || byte == 0xFF {
                break;
            }

            // 解析单个键值对
            self.parse_key_value_pair(result)?;
        }

        Ok(())
    }

    /// 解析单个键值对
    fn parse_key_value_pair(&mut self, result: &mut RdbData) -> Result<(), RdbError> {
        let mut expiry: Option<u64> = None;

        // 检查是否有过期时间
        let byte = self.peek_byte()?;
        match byte {
            // 毫秒级过期时间（8字节，小端序）
            0xFC => {
                self.pos += 1;
                expiry = Some(self.read_u64_le()?);
                eprintln!("RDB Key expiry (ms): {}", expiry.unwrap());
            }
            // 秒级过期时间（4字节，小端序）
            0xFD => {
                self.pos += 1;
                expiry = Some(self.read_u32_le()? as u64 * 1000); // 转换为毫秒
                eprintln!("RDB Key expiry (s converted to ms): {}", expiry.unwrap());
            }
            _ => {}
        }

        // 读取值类型
        let value_type_byte = self.read_byte()?;
        let value_type = ValueType::from_u8(value_type_byte)
            .ok_or(RdbError::InvalidEncoding(value_type_byte))?;

        // 读取键名（字符串编码）
        let key = self.parse_string()?;
        eprintln!("RDB Key: {}", key);

        // 读取值（根据值类型）
        let value = self.parse_value(value_type)?;

        // 添加到结果
        result.keys.push((key, value, expiry));

        Ok(())
    }

    /// 解析值（根据值类型）
    fn parse_value(&mut self, value_type: ValueType) -> Result<RedisValue, RdbError> {
        match value_type {
            ValueType::String => {
                let s = self.parse_string()?;
                Ok(RedisValue::String(s))
            }
            _ => {
                // 其他类型在本挑战中不涉及，返回错误
                Err(RdbError::InvalidEncoding(value_type as u8))
            }
        }
    }

    /// 解析长度编码
    fn parse_length(&mut self) -> Result<usize, RdbError> {
        let byte = self.read_byte()?;
        let encoding_type = (byte & 0xC0) >> 6; // 获取前2位

        match encoding_type {
            // 00: 长度在剩余的6位中
            0 => Ok((byte & 0x3F) as usize),
            // 01: 长度在接下来的14位中（大端序）
            1 => {
                let next_byte = self.read_byte()?;
                let length = ((byte & 0x3F) as usize) << 8 | (next_byte as usize);
                Ok(length)
            }
            // 10: 长度在接下来的4字节中（大端序）
            2 => {
                let length = self.read_u32_be()? as usize;
                Ok(length)
            }
            // 11: 特殊编码（字符串编码）
            3 => Err(RdbError::InvalidLength),
            _ => unreachable!(),
        }
    }

    /// 解析字符串编码
    fn parse_string(&mut self) -> Result<String, RdbError> {
        // 先查看长度编码类型
        let byte = self.peek_byte()?;
        let encoding_type = (byte & 0xC0) >> 6;

        if encoding_type == 3 {
            // 特殊字符串编码
            self.pos += 1; // 跳过类型字节
            let special_type = byte & 0x3F;

            match special_type {
                // 8位整数
                0 => {
                    let value = self.read_byte()? as i8;
                    Ok(value.to_string())
                }
                // 16位整数（小端序）
                1 => {
                    let value = self.read_u16_le()? as i16;
                    Ok(value.to_string())
                }
                // 32位整数（小端序）
                2 => {
                    let value = self.read_u32_le()? as i32;
                    Ok(value.to_string())
                }
                // LZF压缩（本挑战不涉及）
                3 => Err(RdbError::InvalidString),
                _ => Err(RdbError::InvalidEncoding(byte)),
            }
        } else {
            // 普通字符串
            let length = self.parse_length()?;

            if self.pos + length > self.data.len() {
                return Err(RdbError::UnexpectedEof);
            }

            let string_data = &self.data[self.pos..self.pos + length];
            let s = String::from_utf8_lossy(string_data).to_string();
            self.pos += length;

            Ok(s)
        }
    }

    /// 读取一个字节
    fn read_byte(&mut self) -> Result<u8, RdbError> {
        if self.pos >= self.data.len() {
            return Err(RdbError::UnexpectedEof);
        }
        let byte = self.data[self.pos];
        self.pos += 1;
        Ok(byte)
    }

    /// 查看下一个字节（不移动位置）
    fn peek_byte(&mut self) -> Result<u8, RdbError> {
        if self.pos >= self.data.len() {
            return Err(RdbError::UnexpectedEof);
        }
        Ok(self.data[self.pos])
    }

    /// 读取4字节无符号整数（大端序）
    fn read_u32_be(&mut self) -> Result<u32, RdbError> {
        if self.pos + 4 > self.data.len() {
            return Err(RdbError::UnexpectedEof);
        }
        let value = u32::from_be_bytes([
            self.data[self.pos],
            self.data[self.pos + 1],
            self.data[self.pos + 2],
            self.data[self.pos + 3],
        ]);
        self.pos += 4;
        Ok(value)
    }

    /// 读取4字节无符号整数（小端序）
    fn read_u32_le(&mut self) -> Result<u32, RdbError> {
        if self.pos + 4 > self.data.len() {
            return Err(RdbError::UnexpectedEof);
        }
        let value = u32::from_le_bytes([
            self.data[self.pos],
            self.data[self.pos + 1],
            self.data[self.pos + 2],
            self.data[self.pos + 3],
        ]);
        self.pos += 4;
        Ok(value)
    }

    /// 读取8字节无符号整数（小端序）
    fn read_u64_le(&mut self) -> Result<u64, RdbError> {
        if self.pos + 8 > self.data.len() {
            return Err(RdbError::UnexpectedEof);
        }
        let value = u64::from_le_bytes([
            self.data[self.pos],
            self.data[self.pos + 1],
            self.data[self.pos + 2],
            self.data[self.pos + 3],
            self.data[self.pos + 4],
            self.data[self.pos + 5],
            self.data[self.pos + 6],
            self.data[self.pos + 7],
        ]);
        self.pos += 8;
        Ok(value)
    }

    /// 读取2字节无符号整数（小端序）
    fn read_u16_le(&mut self) -> Result<u16, RdbError> {
        if self.pos + 2 > self.data.len() {
            return Err(RdbError::UnexpectedEof);
        }
        let value = u16::from_le_bytes([self.data[self.pos], self.data[self.pos + 1]]);
        self.pos += 2;
        Ok(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_length_encoding_6bit() {
        // 0x0A = 00001010, 前2位是00，长度是10
        let data = vec![0x0A];
        let mut parser = RdbParser::new(data);
        assert_eq!(parser.parse_length().unwrap(), 10);
    }

    #[test]
    fn test_length_encoding_14bit() {
        // 0x42 BC = 01000010 10111100
        // 前2位是01，长度是 000010 10111100 = 700
        let data = vec![0x42, 0xBC];
        let mut parser = RdbParser::new(data);
        assert_eq!(parser.parse_length().unwrap(), 700);
    }

    #[test]
    fn test_string_encoding() {
        // 0x0D = 13, 后面跟着13个字符 "Hello, World!"
        let mut data = vec![0x0D];
        data.extend_from_slice(b"Hello, World!");
        let mut parser = RdbParser::new(data);
        assert_eq!(parser.parse_string().unwrap(), "Hello, World!");
    }

    #[test]
    fn test_integer8_encoding() {
        // 0xC0 = 11000000, 表示8位整数，后面跟着0x7B = 123
        let data = vec![0xC0, 0x7B];
        let mut parser = RdbParser::new(data);
        assert_eq!(parser.parse_string().unwrap(), "123");
    }
}
