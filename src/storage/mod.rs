pub mod data_type;
pub mod column;

use thiserror::Error;
use tracing::{instrument, debug};

use crate::IndexParams;

use self::{data_type::DataType, column::Column};


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
        let column_names = self.columns.iter().map(|c| c.name().to_string()).collect::<Vec<_>>();
        let invalid_fields = params.fields.iter().filter(|f| !column_names.contains(f)).map(|f| f.to_string()).collect::<Vec<_>>();

        if invalid_fields.len() > 0 {
            return Err(StorageError::InvalidFields(invalid_fields))
        }

        for (index, column_name) in params.fields.iter().enumerate() {
            let column_value = params.values.get(index).unwrap();
            let column = self.columns.iter_mut().find(|column| &column.name() == column_name).unwrap();
            if column.data_type().is_compatible(column_value) {
                debug!("Store value {} for column {}", column_value, column_name);
                column.entries_mut().push(column_value.clone().into());
            }
            else {
                return Err(StorageError::InvalidDataType(column_value.clone(), column.data_type().clone()))
            }
        }

        Ok(())
    }
}
