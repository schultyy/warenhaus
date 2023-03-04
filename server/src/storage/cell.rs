use serde::Serialize;
use std::io::Cursor;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

use super::{ByteString, CRC32};

const TAG_I64 : u8 = 1;
const TAG_F64 : u8 = 2;
const TAG_STR : u8 = 3;
const TAG_BOOL : u8 = 4;

#[derive(Debug, Clone, Serialize, PartialEq)]
pub enum Cell {
    Int(i64),
    Float(f64),
    String(String),
    Boolean(bool),
}

impl Cell {
    pub fn from_json_value(json_value: &serde_json::Value) -> Option<Self> {
        match json_value {
            serde_json::Value::Null => None,
            serde_json::Value::Bool(bool) => Some(Cell::Boolean(bool.to_owned())),
            serde_json::Value::Number(num) => {
                if num.is_i64() || num.is_u64() {
                    Some(Cell::Int(num.as_i64().unwrap()))
                }
                else {
                    Some(Cell::Float(num.as_f64().unwrap()))
                }
            },
            serde_json::Value::String(str) => Some(Cell::String(str.into())),
            serde_json::Value::Array(_) => None,
            serde_json::Value::Object(_) => None,
        }
    }

    pub fn to_bytes(&self) -> Result<(u32, u8, ByteString), std::io::Error> {
        let (tag_byte, value) = match self {
            Cell::Int(val) => {
                let mut value_buffer = Vec::new();
                value_buffer.write_i64::<LittleEndian>(val.to_owned())?;
                (TAG_I64, value_buffer)
            },
            Cell::Float(val) => {
                let mut value_buffer = Vec::new();
                value_buffer.write_f64::<LittleEndian>(val.to_owned())?;
                (TAG_F64, value_buffer)
            }
            Cell::String(val) => {
                (TAG_STR, val.as_bytes().to_owned())
            },
            Cell::Boolean(val) => {
                let bool_value = *val as i64;
                let mut value_buffer = Vec::new();
                value_buffer.write_i64::<LittleEndian>(bool_value.to_owned())?;
                (TAG_BOOL, value_buffer)
            }
        };

        let mut tmp = ByteString::with_capacity(1 + value.len());

        for byte in value {
            tmp.push(byte);
        }

        let checksum = CRC32.checksum(&tmp);
        Ok((checksum, tag_byte, tmp.to_vec()))
    }

    pub(crate) fn from_bytes(tag_byte: u8, data: Vec<u8>) -> Option<Cell> {
        let mut cursor = Cursor::new(data.clone());
        match tag_byte {
            TAG_I64 => {
                cursor.read_i64::<LittleEndian>()
                    .map(|val| Some(Cell::Int(val)))
                    .unwrap_or(None)
            },
            TAG_F64 => {
                cursor.read_f64::<LittleEndian>()
                    .map(|val| Some(Cell::Float(val)))
                    .unwrap_or(None)
            },
            TAG_STR => {
                String::from_utf8(data)
                    .map(|val| Some(Cell::String(val)))
                    .unwrap_or(None)
            },
            TAG_BOOL => {
                cursor.read_i64::<LittleEndian>()
                    .map(|val| Some(Cell::Boolean(val == 1)))
                    .unwrap_or(None)
            },
            _ => None
        }
    }

    pub fn as_int(&self) -> Option<&i64> {
        match self {
            Cell::Int(val) => Some(val),
            _ => None
        }
    }
}
