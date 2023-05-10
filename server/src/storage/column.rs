use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::fs::File;
use std::fs::OpenOptions;
use std::io;
use std::io::prelude::*;
use std::io::{BufReader, BufWriter, SeekFrom};
use std::path::Path;
use std::path::PathBuf;

use crate::storage::ByteString;
use crate::storage::CRC32;

use super::cell::Cell;
use super::data_type::DataType;


#[derive(Debug)]
pub struct Column {
    name: String,
    data_type: DataType,
    entries: Vec<Cell>,
    f: File,
}

impl Column {
    pub fn new(root_path: &PathBuf, name: String, data_type: DataType) -> Self {
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

        self.entries.push(cell);

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

    pub fn entries(&self) -> &[Cell] {
        self.entries.as_ref()
    }
}

