pub mod column;
pub mod data_type;

use thiserror::Error;
use tokio::sync::mpsc::Sender;
use tracing::error;
use tracing::{debug, instrument};

use crate::command::Command;
use crate::config::SchemaConfig;
use crate::storage::column::Cell;
use crate::web::IndexParams;

use self::{column::Column, data_type::DataType};

#[derive(Debug, Error)]
pub enum ContainerError {
    #[error("Fields are not present in index")]
    InvalidFields(Vec<String>),
    #[error("Invalid Data Type. Expected {0}, Got {1}")]
    InvalidDataType(serde_json::Value, DataType),
    #[error("Number of fields ({0}) does not match number of provided values ({1}).")]
    FieldCountMismatch(usize, usize),
    #[error("IO Error")]
    IoError {
        #[from]
        source: std::io::Error,
    },
}

#[derive(Debug)]
pub struct Container {
    columns: Vec<Column>,
}

impl Container {
    pub fn new(root_path: &str, config: SchemaConfig) -> Result<Self, ContainerError> {
        let mut columns = vec![];
        for column_config in config.columns.iter() {
            let mut c: Column = Column::new(root_path, column_config.name.to_string(), column_config.data_type.to_owned().into());
            c.load()?;
            columns.push(c);
        }
        Ok(Self { columns })
    }

    #[instrument]
    fn validate_fields(&self, params: &IndexParams) -> Result<(), ContainerError> {
        if self.columns.len() != params.fields.len() {
            return Err(ContainerError::FieldCountMismatch(
                self.columns.len(),
                params.fields.len(),
            ));
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
            return Err(ContainerError::InvalidFields(invalid_fields));
        }

        if params.fields.len() != params.values.len() {
            return Err(ContainerError::FieldCountMismatch(
                params.fields.len(),
                params.values.len(),
            ));
        }

        Ok(())
    }

    #[instrument]
    pub fn index(&mut self, params: IndexParams) -> Result<(), ContainerError> {
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
                if let Some(cell) = Cell::from_json_value(column_value) {
                    column.insert(cell)?;
                }
                else {
                    error!("Incompatible data type for cell: {:?}", column_value);
                }
            } else {
                return Err(ContainerError::InvalidDataType(
                    column_value.clone(),
                    column.data_type().clone(),
                ));
            }
        }

        Ok(())
    }

    pub async fn query(&self, tx: Sender<Command>) {
        let num_rows = self.columns[0].entries().len();
        for n in 0..num_rows {
            let mut row = vec!();
            for column in &self.columns {
                let cell = column.entries().get(n).unwrap();
                row.push(cell.clone());
            }
            match tx.send(Command::QueryRow { row }).await {
                Ok(()) => {},
                Err(err) => {
                    error!("SendError: {}", err);
                }
            }
        }
    }
}
