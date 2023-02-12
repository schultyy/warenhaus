use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::fs::File;
use std::fs::OpenOptions;
use std::io;
use std::io::Cursor;
use std::io::prelude::*;
use std::io::{BufReader, BufWriter, SeekFrom};
use std::path::Path;

use crc::{CRC_32_CKSUM, Crc};

use super::data_type::DataType;

pub const CRC32: Crc<u32> = Crc::<u32>::new(&CRC_32_CKSUM);
const TAG_I64 : u8 = 1;
const TAG_F64 : u8 = 2;
const TAG_STR : u8 = 3;
const TAG_BOOL : u8 = 4;

#[derive(Debug)]
pub enum Cell {
    Int(i64),
    Float(f64),
    String(String),
    Boolean(bool),
}



impl Cell {
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
}

type ByteString = Vec<u8>;

#[derive(Debug)]
pub struct Column {
    name: String,
    data_type: DataType,
    entries: Vec<Cell>,
    f: File,
}

impl Column {
    pub fn new(root_path: &str, name: String, data_type: DataType) -> Self {
        let root_path = Path::new(root_path);
        let file_path = root_path.join(format!("column_{}", name));
        let f = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .append(true)
            .open(file_path)
            .unwrap();

        Self {
            f,
            name,
            data_type,
            entries: vec![],
        }
    }

    pub fn name(&self) -> &str {
        self.name.as_ref()
    }

    pub fn data_type(&self) -> &DataType {
        &self.data_type
    }

    pub fn insert(&mut self, cell: Cell) -> io::Result<u64> {
        let mut f = BufWriter::new(&mut self.f);
        let (checksum, tag_byte, bytes) = cell.to_bytes()?;

        let next_byte = SeekFrom::End(0);
        let current_position = f.seek(SeekFrom::Current(0))?;

        f.seek(next_byte)?;

        f.write_u32::<LittleEndian>(checksum)?;
        f.write_u8(tag_byte)?;
        f.write_u32::<LittleEndian>(bytes.len() as u32)?;
        f.write_all(&bytes)?;

        Ok(current_position)
    }

    pub fn load(&mut self) -> io::Result<()> {
        let mut f = BufReader::new(&mut self.f);

        loop {
            let _current_position = f.seek(SeekFrom::Current(0));
            let maybe_cell = Column::process_record(&mut f);

            let cell = match maybe_cell {
                Ok(cell) => cell,
                Err(err) => {
                    match err.kind() {
                        io::ErrorKind::UnexpectedEof => {
                            break;
                        }
                        _ => return Err(err),
                    }
                }
            };
            self.entries.push(cell);
            //TODO: update index
        }
        Ok(())
    }

    fn process_record<R: Read>(f: &mut R) -> io::Result<Cell> {
        let saved_checksum = f.read_u32::<LittleEndian>()?;
        let tag_byte = f.read_u8()?;
        let val_len = f.read_u32::<LittleEndian>()?;
        let mut data = ByteString::with_capacity(val_len as usize);

        {
            f.by_ref() // <2>
                .take(val_len as u64)
                .read_to_end(&mut data)?;
        }
        debug_assert_eq!(val_len as usize, data.len() as usize);

        let checksum = CRC32.checksum(&data);
        if checksum != saved_checksum {
            panic!(
                "data corruption encountered ({:08x} != {:08x})",
                checksum, saved_checksum
            );
        }

        Ok(Cell::from_bytes(tag_byte, data).unwrap())
    }
}

