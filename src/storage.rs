use std::fmt::Display;

use thiserror::Error;
use tracing::{instrument, debug};

use crate::IndexParams;

#[derive(Debug, PartialEq, Clone)]
pub enum DataType {
    Int,
    Float,
    String,
    Boolean,
}

impl DataType {
    pub fn is_compatible(&self, other: &crate::Value) -> bool {
        let other : DataType = other.into();
        self == &other
    }
}

impl Display for DataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DataType::Int => write!(f, "Int"),
            DataType::Float => write!(f, "Float"),
            DataType::String => write!(f, "String"),
            DataType::Boolean => write!(f, "bool"),
        }
    }
}

#[derive(Debug)]
pub enum Cell {
    Int(i64),
    Float(f64),
    String(String),
    Boolean(bool),
}

#[derive(Debug)]
pub struct Column {
    name: String,
    data_type: DataType,
    entries: Vec<Cell>,
}

impl Column {
    pub fn new(name: String, data_type: DataType) -> Self {
        Self {
            name,
            data_type,
            entries: vec![],
        }
    }
}

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("Fields are not present in index")]
    InvalidFields(Vec<String>),
    #[error("Invalid Data Type. Expected {0}, Got {1}")]
    InvalidDataType(crate::Value, DataType),
}

#[derive(Debug)]
pub struct Storage {
    columns: Vec<Column>,
}

impl Storage {
    pub fn new() -> Self {
        Self {
            columns: vec![Column::new("url".into(), DataType::String)],
        }
    }

    #[instrument]
    pub fn index(&mut self, params: IndexParams) -> Result<(), StorageError> {
        let column_names = self.columns.iter().map(|c| c.name.to_string()).collect::<Vec<_>>();
        let invalid_fields = params.fields.iter().filter(|f| !column_names.contains(f)).map(|f| f.to_string()).collect::<Vec<_>>();

        if invalid_fields.len() > 0 {
            return Err(StorageError::InvalidFields(invalid_fields))
        }

        for (index, column_name) in params.fields.iter().enumerate() {
            let column_value = params.values.get(index).unwrap();
            let column = self.columns.iter_mut().find(|column| &column.name == column_name).unwrap();
            if column.data_type.is_compatible(column_value) {
                debug!("Store value {} for column {}", column_value, column_name);
                column.entries.push(column_value.clone().into());
            }
            else {
                return Err(StorageError::InvalidDataType(column_value.clone(), column.data_type.clone()))
            }
        }

        Ok(())
    }
}
