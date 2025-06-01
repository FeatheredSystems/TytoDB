use std::io::{Error, ErrorKind};

use base64::{engine::general_purpose, Engine};
use serde::{Deserialize, Serialize};

use crate::{database::MAX_STR_LEN, lexer_functions::Token};

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub enum AlbaTypes{
    Text(String),
    Int(i32),
    Bigint(i64),
    Float(f64),
    Bool(bool),
    Char(char),
    NanoString(String),
    SmallString(String),
    MediumString(String),
    BigString(String),
    LargeString(String),
    NanoBytes(Vec<u8>),
    SmallBytes(Vec<u8>),
    MediumBytes(Vec<u8>),
    BigSBytes(Vec<u8>),
    LargeBytes(Vec<u8>),
    NONE
}
/*
char ~ 1
string~n ~ 10
string~s ~ 100
string-m ~ 500
string-b ~ 2,000
string-l ~ 3,000
bytes~n ~ 10
bytes~s ~ 1,000
bytes-m ~ 10,000
bytes-b ~ 100,000
bytes-l ~ 1,000,000

*/

impl AlbaTypes {
    pub fn from_id(code: u8) -> Result<AlbaTypes, Error> {
        match code {
            0  => Ok(AlbaTypes::NONE),
            1  => Ok(AlbaTypes::Char('\0')),
            2  => Ok(AlbaTypes::Int(0)),
            3  => Ok(AlbaTypes::Bigint(0)),
            4  => Ok(AlbaTypes::Bool(false)),
            5  => Ok(AlbaTypes::Float(0.0)),
            6  => Ok(AlbaTypes::Text(String::new())),
            7  => Ok(AlbaTypes::NanoString(String::new())),
            8  => Ok(AlbaTypes::SmallString(String::new())),
            9  => Ok(AlbaTypes::MediumString(String::new())),
            10 => Ok(AlbaTypes::BigString(String::new())),
            11 => Ok(AlbaTypes::LargeString(String::new())),
            12 => Ok(AlbaTypes::NanoBytes(Vec::new())),
            13 => Ok(AlbaTypes::SmallBytes(Vec::new())),
            14 => Ok(AlbaTypes::MediumBytes(Vec::new())),
            15 => Ok(AlbaTypes::BigSBytes(Vec::new())),
            16 => Ok(AlbaTypes::LargeBytes(Vec::new())),
            x  => Err(Error::new(
                      ErrorKind::InvalidData,
                      format!("Unknown AlbaTypes code: {}", x)
                  )),
        }
    }
    pub fn get_id(&self) -> u8 {
        match self {
            AlbaTypes::NONE            =>  0,
            AlbaTypes::Char(_)         =>  1,
            AlbaTypes::Int(_)          =>  2,
            AlbaTypes::Bigint(_)       =>  3,
            AlbaTypes::Bool(_)         =>  4,
            AlbaTypes::Float(_)        =>  5,
            AlbaTypes::Text(_)         =>  6,
            AlbaTypes::NanoString(_)   =>  7,
            AlbaTypes::SmallString(_)  =>  8,
            AlbaTypes::MediumString(_) =>  9,
            AlbaTypes::BigString(_)    => 10,
            AlbaTypes::LargeString(_)  => 11,
            AlbaTypes::NanoBytes(_)    => 12,
            AlbaTypes::SmallBytes(_)   => 13,
            AlbaTypes::MediumBytes(_)  => 14,
            AlbaTypes::BigSBytes(_)    => 15,
            AlbaTypes::LargeBytes(_)   => 16,
        }
    }
    // pub fn get_id_from_text(keyword: &str) -> Result<u8, Error> {
    //     match keyword.to_uppercase().as_str() {
    //         "INT"             => Ok(2),
    //         "BIGINT"          => Ok(3),
    //         "BOOL"            => Ok(4),
    //         "FLOAT"           => Ok(5),
    //         "TEXT"            => Ok(6),
    //         "NANO-STRING"     => Ok(7),
    //         "SMALL-STRING"    => Ok(8),
    //         "MEDIUM-STRING"   => Ok(9),
    //         "BIG-STRING"      => Ok(10),
    //         "LARGE-STRING"    => Ok(11),
    //         "NANO-BYTES"      => Ok(12),
    //         "SMALL-BYTES"     => Ok(13),
    //         "MEDIUM-BYTES"    => Ok(14),
    //         "BIG-BYTES"       => Ok(15),
    //         "LARGE-BYTES"     => Ok(16),
    //         other => Err(Error::new(
    //             ErrorKind::InvalidInput,
    //             format!("Unknown type keyword: {}", other)
    //         )),
    //     }
    // }

}

impl AlbaTypes {
    pub fn try_from_existing(&self, i: AlbaTypes) -> Result<AlbaTypes, Error> {
        match self {
            AlbaTypes::Text(_) => {
                let text = match i {
                    AlbaTypes::Text(s) | AlbaTypes::NanoString(s) | AlbaTypes::SmallString(s) |
                    AlbaTypes::MediumString(s) | AlbaTypes::BigString(s) | AlbaTypes::LargeString(s) => s,
                    AlbaTypes::Int(n) => n.to_string(),
                    AlbaTypes::Bigint(n) => n.to_string(),
                    AlbaTypes::Float(f) => f.to_string(),
                    AlbaTypes::Bool(b) => b.to_string(),
                    AlbaTypes::Char(c) => c.to_string(),
                    AlbaTypes::NanoBytes(b) | AlbaTypes::SmallBytes(b) | AlbaTypes::MediumBytes(b) |
                    AlbaTypes::BigSBytes(b) | AlbaTypes::LargeBytes(b) => {
                        general_purpose::STANDARD.encode(&b)
                    }
                    AlbaTypes::NONE => return Err(Error::new(ErrorKind::InvalidData, "Cannot convert NONE to Text")),
                };
                Ok(AlbaTypes::Text(text))
            }
            AlbaTypes::Int(_) => {
                let int_val = match i {
                    AlbaTypes::Int(n) => n,
                    AlbaTypes::Bigint(n) => {
                        if n >= i32::MIN as i64 && n <= i32::MAX as i64 {
                            n as i32
                        } else {
                            return Err(Error::new(ErrorKind::InvalidData, "Bigint out of range for i32"));
                        }
                    }
                    AlbaTypes::Float(f) => {
                        if f.is_nan() || f.is_infinite() {
                            return Err(Error::new(ErrorKind::InvalidData, "Cannot convert NaN or infinite float to i32"));
                        }
                        f as i32
                    }
                    AlbaTypes::Bool(b) => if b { 1 } else { 0 },
                    AlbaTypes::Text(s) | AlbaTypes::NanoString(s) | AlbaTypes::SmallString(s) |
                    AlbaTypes::MediumString(s) | AlbaTypes::BigString(s) | AlbaTypes::LargeString(s) => {
                        s.parse::<i32>().map_err(|_| Error::new(ErrorKind::InvalidData, "Failed to parse string as i32"))?
                    }
                    AlbaTypes::NONE => return Err(Error::new(ErrorKind::InvalidData, "Cannot convert NONE to Int")),
                    _ => return Err(Error::new(ErrorKind::InvalidData, "Unsupported conversion to Int")),
                };
                Ok(AlbaTypes::Int(int_val))
            }
            AlbaTypes::Bigint(_) => {
                let bigint_val = match i {
                    AlbaTypes::Bigint(n) => n,
                    AlbaTypes::Int(n) => n as i64,
                    AlbaTypes::Float(f) => {
                        if f.is_nan() || f.is_infinite() {
                            return Err(Error::new(ErrorKind::InvalidData, "Cannot convert NaN or infinite float to i64"));
                        }
                        f as i64
                    }
                    AlbaTypes::Bool(b) => if b { 1 } else { 0 },
                    AlbaTypes::Text(s) | AlbaTypes::NanoString(s) | AlbaTypes::SmallString(s) |
                    AlbaTypes::MediumString(s) | AlbaTypes::BigString(s) | AlbaTypes::LargeString(s) => {
                        s.parse::<i64>().map_err(|_| Error::new(ErrorKind::InvalidData, "Failed to parse string as i64"))?
                    }
                    AlbaTypes::NONE => return Err(Error::new(ErrorKind::InvalidData, "Cannot convert NONE to Bigint")),
                    _ => return Err(Error::new(ErrorKind::InvalidData, "Unsupported conversion to Bigint")),
                };
                Ok(AlbaTypes::Bigint(bigint_val))
            }
            AlbaTypes::Float(_) => {
                let float_val = match i {
                    AlbaTypes::Float(f) => f,
                    AlbaTypes::Int(n) => n as f64,
                    AlbaTypes::Bigint(n) => n as f64,
                    AlbaTypes::Bool(b) => if b { 1.0 } else { 0.0 },
                    AlbaTypes::Text(s) | AlbaTypes::NanoString(s) | AlbaTypes::SmallString(s) |
                    AlbaTypes::MediumString(s) | AlbaTypes::BigString(s) | AlbaTypes::LargeString(s) => {
                        s.parse::<f64>().map_err(|_| Error::new(ErrorKind::InvalidData, "Failed to parse string as f64"))?
                    }
                    AlbaTypes::NONE => return Err(Error::new(ErrorKind::InvalidData, "Cannot convert NONE to Float")),
                    _ => return Err(Error::new(ErrorKind::InvalidData, "Unsupported conversion to Float")),
                };
                Ok(AlbaTypes::Float(float_val))
            }
            AlbaTypes::Bool(_) => {
                let bool_val = match i {
                    AlbaTypes::Bool(b) => b,
                    AlbaTypes::Int(n) => n != 0,
                    AlbaTypes::Bigint(n) => n != 0,
                    AlbaTypes::Float(f) => f != 0.0,
                    AlbaTypes::Text(s) | AlbaTypes::NanoString(s) | AlbaTypes::SmallString(s) |
                    AlbaTypes::MediumString(s) | AlbaTypes::BigString(s) | AlbaTypes::LargeString(s) => {
                        let trimmed = s.trim().to_lowercase();
                        match trimmed.as_str() {
                            "0" | "f" | "false" => false,
                            "1" | "t" | "true" => true,
                            _ => return Err(Error::new(ErrorKind::InvalidData, "Invalid boolean string")),
                        }
                    }
                    AlbaTypes::NONE => return Err(Error::new(ErrorKind::InvalidData, "Cannot convert NONE to Bool")),
                    _ => return Err(Error::new(ErrorKind::InvalidData, "Unsupported conversion to Bool")),
                };
                Ok(AlbaTypes::Bool(bool_val))
            }
            AlbaTypes::Char(_) => {
                let char_val = match i {
                    AlbaTypes::Char(c) => c,
                    AlbaTypes::Text(s) | AlbaTypes::NanoString(s) | AlbaTypes::SmallString(s) |
                    AlbaTypes::MediumString(s) | AlbaTypes::BigString(s) | AlbaTypes::LargeString(s) => {
                        if s.len() == 1 {
                            s.chars().next().unwrap()
                        } else {
                            return Err(Error::new(ErrorKind::InvalidData, "String must be a single character for Char"));
                        }
                    }
                    AlbaTypes::NONE => return Err(Error::new(ErrorKind::InvalidData, "Cannot convert NONE to Char")),
                    _ => return Err(Error::new(ErrorKind::InvalidData, "Unsupported conversion to Char")),
                };
                Ok(AlbaTypes::Char(char_val))
            }
            AlbaTypes::NanoString(_) => {
                let s = get_string_from_alba_type(i)?;
                Ok(AlbaTypes::NanoString(truncate_or_pad_string(s, 10)))
            }
            AlbaTypes::SmallString(_) => {
                let s = get_string_from_alba_type(i)?;
                Ok(AlbaTypes::SmallString(truncate_or_pad_string(s, 100)))
            }
            AlbaTypes::MediumString(_) => {
                let s = get_string_from_alba_type(i)?;
                Ok(AlbaTypes::MediumString(truncate_or_pad_string(s, 500)))
            }
            AlbaTypes::BigString(_) => {
                let s = get_string_from_alba_type(i)?;
                Ok(AlbaTypes::BigString(truncate_or_pad_string(s, 2000)))
            }
            AlbaTypes::LargeString(_) => {
                let s = get_string_from_alba_type(i)?;
                Ok(AlbaTypes::LargeString(truncate_or_pad_string(s, 3000)))
            }
            AlbaTypes::NanoBytes(_) => {
                let bytes = get_bytes_from_alba_type(i)?;
                Ok(AlbaTypes::NanoBytes(truncate_or_pad_bytes(bytes, 10)))
            }
            AlbaTypes::SmallBytes(_) => {
                let bytes = get_bytes_from_alba_type(i)?;
                Ok(AlbaTypes::SmallBytes(truncate_or_pad_bytes(bytes, 1000)))
            }
            AlbaTypes::MediumBytes(_) => {
                let bytes = get_bytes_from_alba_type(i)?;
                Ok(AlbaTypes::MediumBytes(truncate_or_pad_bytes(bytes, 10_000)))
            }
            AlbaTypes::BigSBytes(_) => {
                let bytes = get_bytes_from_alba_type(i)?;
                Ok(AlbaTypes::BigSBytes(truncate_or_pad_bytes(bytes, 100_000)))
            }
            AlbaTypes::LargeBytes(_) => {
                let bytes = get_bytes_from_alba_type(i)?;
                Ok(AlbaTypes::LargeBytes(truncate_or_pad_bytes(bytes, 1_000_000)))
            }
            AlbaTypes::NONE => Ok(AlbaTypes::NONE),
        }
    }
    pub fn size(&self) -> usize{
        match self {
            AlbaTypes::Bigint(_) => size_of::<i64>(),
            AlbaTypes::Int(_) => size_of::<i32>(),
            AlbaTypes::Float(_) => size_of::<f64>(),
            AlbaTypes::Bool(_) => size_of::<bool>(),
            AlbaTypes::Text(_) => MAX_STR_LEN,
            AlbaTypes::NONE => 0,
            AlbaTypes::Char(_) => size_of::<char>(),
            AlbaTypes::NanoString(_) => 10 + size_of::<usize>(),
            AlbaTypes::SmallString(_) => 100 + size_of::<usize>(),
            AlbaTypes::MediumString(_) => 500 + size_of::<usize>(),
            AlbaTypes::BigString(_) => 2_000 + size_of::<usize>(),
            AlbaTypes::LargeString(_) => 3_000 + size_of::<usize>(),
            AlbaTypes::NanoBytes(_) => 10 + size_of::<usize>(),
            AlbaTypes::SmallBytes(_) => 1000 + size_of::<usize>(),
            AlbaTypes::MediumBytes(_) => 10_000 + size_of::<usize>(),
            AlbaTypes::BigSBytes(_) => 100_000 + size_of::<usize>(),
            AlbaTypes::LargeBytes(_) => 1_000_000 + size_of::<usize>(),
        }
    }

}

fn get_string_from_alba_type(i: AlbaTypes) -> Result<String, Error> {
    match i {
        AlbaTypes::Text(s) | AlbaTypes::NanoString(s) | AlbaTypes::SmallString(s) |
        AlbaTypes::MediumString(s) | AlbaTypes::BigString(s) | AlbaTypes::LargeString(s) => Ok(s),
        AlbaTypes::Int(n) => Ok(n.to_string()),
        AlbaTypes::Bigint(n) => Ok(n.to_string()),
        AlbaTypes::Float(f) => Ok(f.to_string()),
        AlbaTypes::Bool(b) => Ok(b.to_string()),
        AlbaTypes::Char(c) => Ok(c.to_string()),
        AlbaTypes::NanoBytes(b) | AlbaTypes::SmallBytes(b) | AlbaTypes::MediumBytes(b) |
        AlbaTypes::BigSBytes(b) | AlbaTypes::LargeBytes(b) => {
            Ok(general_purpose::STANDARD.encode(&b))
        }
        AlbaTypes::NONE => Err(Error::new(ErrorKind::InvalidData, "Cannot convert NONE to string")),
    }
}

fn truncate_or_pad_string(s: String, max_len: usize) -> String {
    if s.len() > max_len {
        s[..max_len].to_string()
    } else {
        format!("{: <width$}", s, width = max_len)
    }
}

fn get_bytes_from_alba_type(i: AlbaTypes) -> Result<Vec<u8>, Error> {
    match i {
        AlbaTypes::NanoBytes(b) | AlbaTypes::SmallBytes(b) | AlbaTypes::MediumBytes(b) |
        AlbaTypes::BigSBytes(b) | AlbaTypes::LargeBytes(b) => Ok(b),
        AlbaTypes::Text(s) | AlbaTypes::NanoString(s) | AlbaTypes::SmallString(s) |
        AlbaTypes::MediumString(s) | AlbaTypes::BigString(s) | AlbaTypes::LargeString(s) => {
            general_purpose::STANDARD
                .decode(s.as_bytes())
                .map_err(|_| Error::new(ErrorKind::InvalidData, "Invalid base64 string"))
        }
        AlbaTypes::NONE => Err(Error::new(ErrorKind::InvalidData, "Cannot convert NONE to bytes")),
        _ => Err(Error::new(ErrorKind::InvalidData, "Unsupported conversion to bytes")),
    }
}

fn truncate_or_pad_bytes(b: Vec<u8>, max_len: usize) -> Vec<u8> {
    let mut bytes = b;
    if bytes.len() > max_len {
        bytes.truncate(max_len);
    } else {
        bytes.resize(max_len, 0);
    }
    bytes
}

impl TryFrom<Token> for AlbaTypes {
    type Error = &'static str;

    fn try_from(token: Token) -> Result<Self, Self::Error> {
        match token {
            Token::Bytes(b) => {
                // match size {
                //     10 => values.push(AlbaTypes::NanoBytes(blob)),
                //     1000 => values.push(AlbaTypes::SmallBytes(blob)),
                //     10_000 => values.push(AlbaTypes::MediumBytes(blob)),
                //     100_000 => values.push(AlbaTypes::BigSBytes(blob)),
                //     1_000_000 => values.push(AlbaTypes::LargeBytes(blob)),
                //     _ => unreachable!(),
                // }
                let l = b.len();
                Ok(if l <= 10{
                    AlbaTypes::NanoBytes(b)
                }else if l > 10 && l <= 1000{
                    AlbaTypes::SmallBytes(b)
                }else if l > 1000 && l <= 10000{
                    AlbaTypes::MediumBytes(b)
                }else if l > 10000 && l <= 100000{
                    AlbaTypes::BigSBytes(b)
                }else {
                    AlbaTypes::LargeBytes(b)
                })
            },
            Token::String(s) =>
                Ok(AlbaTypes::Text(s)), // moved, no clone

            Token::Int(i) if (i32::MIN as i64) <= i && i <= (i32::MAX as i64) =>
                Ok(AlbaTypes::Int(i as i32)),

            Token::Int(i) =>
                Ok(AlbaTypes::Bigint(i)),

            Token::Float(f) =>
                Ok(AlbaTypes::Float(f)),

            Token::Bool(b) =>
                Ok(AlbaTypes::Bool(b)),
            Token::Keyword(s) => match s.to_uppercase().as_str().trim() {
                "INT" => Ok(AlbaTypes::Int(0)),        // default dummy values
                "BIGINT" => Ok(AlbaTypes::Bigint(0)),
                "FLOAT" => Ok(AlbaTypes::Float(0.0)),
                "BOOL" => Ok(AlbaTypes::Bool(false)),
                "TEXT" => Ok(AlbaTypes::Text(String::new())),
                "NANO-STRING" => Ok(AlbaTypes::NanoString(String::new())),
                "SMALL-STRING" => Ok(AlbaTypes::SmallString(String::new())),
                "MEDIUM-STRING" => Ok(AlbaTypes::MediumString(String::new())),
                "BIG-STRING" => Ok(AlbaTypes::BigString(String::new())),
                "LARGE-STRING" => Ok(AlbaTypes::LargeString(String::new())),
                "NANO-BYTES" => Ok(AlbaTypes::NanoBytes(Vec::new())),
                "SMALL-BYTES" => Ok(AlbaTypes::SmallBytes(Vec::new())),
                "MEDIUM-BYTES" => Ok(AlbaTypes::MediumBytes(Vec::new())),
                "BIG-BYTES" => Ok(AlbaTypes::BigSBytes(Vec::new())),
                "LARGE-BYTES" => Ok(AlbaTypes::LargeBytes(Vec::new())),
                _ => return Err(format!("Unknown type keyword: {}", s).leak()),
            },
            _ => {
                let va = format!("Cannot convert token to AlbaTypes: unsupported token type {:#?}. Expected one of: String, Int, Float, Bool, or Keyword (for type definitions).", token);
                return Err(va.leak());
            }
        }
    }
}


