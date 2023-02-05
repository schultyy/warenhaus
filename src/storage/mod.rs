pub mod column;
pub mod data_type;

use thiserror::Error;
use tracing::{debug, instrument};

use crate::config::SchemaConfig;
use crate::web::IndexParams;
use crate::web::Value;

use self::{column::Column, data_type::DataType};

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("Fields are not present in index")]
    InvalidFields(Vec<String>),
    #[error("Invalid Data Type. Expected {0}, Got {1}")]
    InvalidDataType(Value, DataType),
    #[error("Number of fields ({0}) does not match number of provided values ({1}).")]
    FieldCountMismatch(usize, usize),
}

#[derive(Debug)]
pub struct Storage {
    columns: Vec<Column>,
}

impl Storage {
    pub fn new(config: SchemaConfig) -> Self {

        let columns = config.columns.into_iter().map(|column_config| column_config.into()).collect::<Vec<Column>>();

        Self {
            columns
        }
    }

    #[instrument]
    fn validate_fields(&self, params: &IndexParams) -> Result<(), StorageError> {
        if self.columns.len() != params.fields.len() {
            return Err(StorageError::FieldCountMismatch(self.columns.len(), params.fields.len()))
        }

        let column_names = self
            .columns
            .iter()
            .map(|c| c.name().to_string())
            .collect::<Vec<_>>();
        let invalid_fields = params
            .fields
            .iter()
            .filter(|f| !column_names.contains(f))
            .map(|f| f.to_string())
            .collect::<Vec<_>>();

        if invalid_fields.len() > 0 {
            return Err(StorageError::InvalidFields(invalid_fields));
        }

        if params.fields.len() != params.values.len() {
            return Err(StorageError::FieldCountMismatch(
                params.fields.len(),
                params.values.len(),
            ));
        }

        Ok(())
    }

    #[instrument]
    pub fn index(&mut self, params: IndexParams) -> Result<(), StorageError> {
        self.validate_fields(&params)?;

        for (index, column_name) in params.fields.iter().enumerate() {
            let column_value = params.values.get(index).unwrap();
            let column = self
                .columns
                .iter_mut()
                .find(|column| &column.name() == column_name)
                .unwrap();
            if column.data_type().is_compatible(column_value) {
                debug!("Store value {} for column {}", column_value, column_name);
                column.entries_mut().push(column_value.clone().into());
            } else {
                return Err(StorageError::InvalidDataType(
                    column_value.clone(),
                    column.data_type().clone(),
                ));
            }
        }

        Ok(())
    }
}
