mod auto_index;
pub mod auto_index_error;
pub mod column;
pub mod cell;
pub mod data_type;
pub mod column_frame;

use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};
use crc::{CRC_32_CKSUM, Crc};
use thiserror::Error;
use tokio::sync::mpsc::Sender;
use tracing::log::warn;
use tracing::{debug, instrument};
use tracing::{error, info};

use crate::command::Command;
use crate::config::SchemaConfig;
use crate::storage::cell::Cell;
use crate::web::IndexParams;

use self::auto_index::AutoIndex;
use self::auto_index_error::AutoIndexError;
use self::column_frame::ColumnFrame;
use self::{column::Column, data_type::DataType};

pub type ByteString = Vec<u8>;
pub const CRC32: Crc<u32> = Crc::<u32>::new(&CRC_32_CKSUM);

#[derive(Debug, Error)]
pub enum ContainerError {
    #[error("Fields are not present in index")]
    InvalidFields(Vec<String>),
    #[error("Invalid Data Type. Expected {1}, Got {0}")]
    InvalidDataType(serde_json::Value, DataType),
    #[error("Number of fields ({0}) does not match number of provided values ({1}).")]
    FieldCountMismatch(usize, usize),
    #[error("IO Error")]
    IoError {
        #[from]
        source: std::io::Error,
    },
    #[error("Missing Timestamp Column")]
    MissingTimestampColumn,
    #[error("Index Error")]
    IndexError {
        #[from]
        source: AutoIndexError,
    },
}

#[derive(Debug)]
struct ColumnLayout {
    db_root_path: PathBuf,
    columns: Vec<Column>,
    column_names_ordered: Vec<(String, DataType)>,
}

impl ColumnLayout {
    fn new(db_root_path: &PathBuf) -> Self {
        Self {
            db_root_path: db_root_path.into(),
            columns: vec![],
            column_names_ordered: vec![],
        }
    }

    #[instrument(skip(self))]
    pub fn insert_column(&mut self, new_column: Column) -> Result<(), std::io::Error> {
        self.column_names_ordered.push((
            new_column.name().to_string(),
            new_column.data_type().clone(),
        ));
        self.columns.push(new_column);
        Ok(())
    }

    #[instrument(skip(self))]
    pub fn load(&mut self) -> Result<(), std::io::Error> {
        let root_path = Path::new(&self.db_root_path);
        let file_path = root_path.join("column_layout.json");

        let bytes = fs::read(file_path)?;
        let file_contents = String::from_utf8(bytes)
            .expect("Failed to load column_layout.json. Expected utf-8, got corrupted format");
        self.column_names_ordered = serde_json::from_str(&file_contents)?;
        for (column_name, data_type) in &self.column_names_ordered {
            let mut c = Column::new(
                &self.db_root_path,
                column_name.to_string(),
                data_type.to_owned(),
            );
            c.load()?;
            self.columns.push(c);
        }

        Ok(())
    }

    #[instrument(skip(self))]
    pub fn persist_layout(&self) -> Result<(), std::io::Error> {
        let json = serde_json::to_string(&self.column_names_ordered).unwrap();

        let root_path = Path::new(&self.db_root_path);
        let file_path = root_path.join("column_layout.json");

        fs::write(file_path, json)?;
        Ok(())
    }

    pub fn len(&self) -> usize {
        self.columns.len()
    }

    pub fn column_names(&self) -> Vec<String> {
        self.columns
            .iter()
            .map(|c| c.name().to_string())
            .collect::<Vec<_>>()
    }

    pub fn timestamp_column(&self) -> Option<&Column> {
        self.columns.iter().find(|c| c.name() == "timestamp")
    }

    pub fn find_column(&self, column_name: &str) -> Option<&Column> {
        self.columns
            .iter()
            .find(|column| column.name() == column_name)
    }

    #[instrument(skip(self))]
    pub fn commit(&mut self, values: Vec<(String, Cell)>) -> Result<(), ContainerError> {
        for (column_name, cell) in values {
            let db_column = self
                .columns
                .iter_mut()
                .find(|column| column.name() == column_name)
                .unwrap();
            db_column.insert(cell)?;
        }
        Ok(())
    }

    #[instrument(skip(self))]
    pub fn all_rows(&self) -> Vec<ColumnFrame> {
        let reference_length = self.columns[0].entries().len();
        let length_check_passed = self
            .columns
            .iter()
            .all(|c| c.entries().len() == reference_length);
        if !length_check_passed {
            panic!("Columns Corrupted. Not all columns contain the same number of entries");
        }

        let mut rows = vec![];

        for n in 0..reference_length {
            let mut frame = ColumnFrame::new();
            for column in &self.columns {
                let cell = column.entries().get(n).unwrap();
                frame.insert(column.name(), cell.to_owned());
            }
            rows.push(frame);
        }

        rows
    }
}


#[derive(Debug)]
pub struct Container {
    config: SchemaConfig,
    columns: ColumnLayout,
    index_counter: AutoIndex,
}

impl Container {
    #[instrument]
    pub fn new(root_path: &PathBuf, config: SchemaConfig) -> Result<Self, ContainerError> {
        let index_counter = AutoIndex::load_or_new(root_path);
        let mut column_layout = ColumnLayout::new(root_path);

        info!("Try loading column layout");
        let column_layout_load_result = column_layout.load();
        if let Err(err) = column_layout_load_result {
            if err.kind() == std::io::ErrorKind::NotFound {
                warn!("Column layout not found. Starting from scratch");
                column_layout.insert_column(Column::new(root_path, "id".into(), DataType::Int))?;
                for column_config in config.columns.iter() {
                    let mut c: Column = Column::new(
                        root_path,
                        column_config.name.to_string(),
                        column_config.data_type.to_owned().into(),
                    );
                    c.load()?;
                    column_layout.insert_column(c)?;
                }
                if config.add_timestamp_column {
                    info!(
                        add_timestamp_column = config.add_timestamp_column,
                        "Adding Timestamp Column"
                    );
                    let mut ts_column = Column::new(root_path, "timestamp".into(), DataType::Int);
                    ts_column.load()?;
                    column_layout.insert_column(ts_column)?;
                }
                info!("Persisting new column layout");
                column_layout.persist_layout()?;
            } else {
                return Err(err.into());
            }
        }

        Ok(Self {
            columns: column_layout,
            config,
            index_counter,
        })
    }

    #[instrument(skip(self))]
    fn validate_fields(&self, params: &IndexParams) -> Result<(), ContainerError> {
        let param_field_count = if self.config.add_timestamp_column {
            debug!("Validate Param Field Count. Adding Timestamp Column");
            params.fields.len() + 2 // +1 for timestamp, +1 for id
        } else {
            params.fields.len() + 1 //+1 for id
        };

        if self.columns.len() != param_field_count {
            return Err(ContainerError::FieldCountMismatch(
                self.columns.len(),
                params.fields.len(),
            ));
        }

        if self.config.add_timestamp_column {
            if params.fields.iter().find(|column_name| column_name == &"timestamp").is_some() {
                return Err(ContainerError::InvalidFields(vec!["timestamp".into()]))
            }
        }

        let column_names = self.columns.column_names();
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

    #[instrument(skip(self))]
    pub fn index(&mut self, params: IndexParams) -> Result<(), ContainerError> {
        self.validate_fields(&params)?;

        let mut to_be_inserted = vec![];

        to_be_inserted.push(("id".to_string(), Cell::Int(self.index_counter.next())));

        if self.config.add_timestamp_column {
            let timestamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs();
            if let Some(_timestamp_column) = self.columns.timestamp_column() {
                to_be_inserted.push(("timestamp".to_string(), Cell::Int(timestamp as i64)));
            } else {
                error!(
                    "Failed to insert timestamp for {:?} params. Couldn't find Column",
                    params
                );
                return Err(ContainerError::MissingTimestampColumn);
            }
        }

        for (index, column_name) in params.fields.iter().enumerate() {
            let column_value = params.values.get(index).unwrap();
            let db_column = self.columns.find_column(column_name).unwrap();
            let db_column_data_type = db_column.data_type().clone();
            if db_column.data_type().is_compatible(column_value) {
                debug!("Store value {} for column {}", column_value, column_name);
                //We assume this conversion always works because we checked in the if statement above if the type is compatible
                let cell = Cell::from_json_value(column_value).unwrap();
                to_be_inserted.push((column_name.to_owned(), cell));
            } else {
                self.rollback();
                return Err(ContainerError::InvalidDataType(
                    column_value.clone(),
                    db_column_data_type,
                ));
            }
        }

        self.commit(to_be_inserted)?;
        Ok(())
    }

    #[instrument(skip(self))]
    fn commit(&mut self, values: Vec<(String, Cell)>) -> Result<(), ContainerError> {
        self.columns.commit(values)?;
        self.index_counter.commit()?;
        Ok(())
    }

    #[instrument(skip(self))]
    fn rollback(&mut self) {
        self.index_counter.rollback();
    }

    #[instrument(skip(self))]
    pub async fn query(&self, tx: Sender<Command>) {
        for row in self.columns.all_rows() {
            match tx.send(Command::QueryRow { row }).await {
                Ok(()) => {
                    debug!("Successfully sent row");
                }
                Err(err) => {
                    error!("SendError: {}", err);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};

    use serde_json::json;

    use super::Container;
    use crate::{
        config::{ColumnConfig, DataTypeConfig, SchemaConfig},
        storage::cell::Cell,
        web::IndexParams,
    };

    pub fn initialize() {
        let _ = std::fs::remove_file("/tmp/column_url");
        let _ = std::fs::remove_file("/tmp/column_timestamp");
        let _ = std::fs::remove_file("/tmp/column_points");
        let _ = std::fs::remove_file("/tmp/column_id");
        let _ = std::fs::remove_file("/tmp/auto_index");
        let _ = std::fs::remove_file("/tmp/column_layout.json");
    }

    fn schema_config_with_timestamp() -> SchemaConfig {
        let columns = vec![ColumnConfig {
            name: "url".into(),
            data_type: DataTypeConfig::String,
        }];
        SchemaConfig {
            columns,
            add_timestamp_column: true,
        }
    }

    fn schema_config_without_timestamp() -> SchemaConfig {
        let columns = vec![ColumnConfig {
            name: "url".into(),
            data_type: DataTypeConfig::String,
        }];
        SchemaConfig {
            columns,
            add_timestamp_column: false,
        }
    }

    fn schema_config_with_timestamp_and_two_columns() -> SchemaConfig {
        let columns = vec![
            ColumnConfig {
                name: "url".into(),
                data_type: DataTypeConfig::String,
            },
            ColumnConfig {
                name: "points".into(),
                data_type: DataTypeConfig::Int,
            },
        ];
        SchemaConfig {
            columns,
            add_timestamp_column: true,
        }
    }

    #[test]
    fn insert_a_record_with_auto_timestamp_column() {
        initialize();
        let mut container = Container::new(&Path::new("/tmp").to_path_buf(), schema_config_with_timestamp()).unwrap(); 

        let params = IndexParams {
            fields: vec!["url".into()],
            values: vec![serde_json::Value::String("https://google.com".into())],
        };
        container.index(params).unwrap();

        let ts_column = container.columns.find_column("timestamp").unwrap();
        let url_column = container.columns.find_column("url").unwrap();

        assert_eq!(
            ts_column.entries().len(),
            1,
            "Timestamp not found: {:?}",
            ts_column.entries()
        );
        assert_eq!(url_column.entries().len(), 1);

        let url_cell = url_column.entries().get(0).unwrap();
        if let Cell::String(str) = url_cell {
            assert_eq!(str, "https://google.com");
        } else {
            assert!(false, "Failed to retrieve URL from column: {:?}", url_cell);
        }
    }

    #[test]
    fn insert_a_record_without_auto_timestamp_column() {
        initialize();
        let mut container =
            Container::new(&Path::new("/tmp").to_path_buf(), schema_config_without_timestamp()).unwrap();

        let params = IndexParams {
            fields: vec!["url".into()],
            values: vec![serde_json::Value::String("https://google.com".into())],
        };
        container.index(params).unwrap();

        let ts_column = container.columns.find_column("timestamp");
        let url_column = container.columns.find_column("url").unwrap();
        assert!(
            ts_column.is_none(),
            "Wasn't expecting timestamp column, yet it is present: {:?}",
            ts_column
        );
        assert_eq!(
            url_column.entries().len(),
            1,
            "was expecting one url, found more than one"
        );

        let url_cell = url_column.entries().get(0).unwrap();
        if let Cell::String(str) = url_cell {
            assert_eq!(str, "https://google.com");
        } else {
            assert!(false, "Failed to retrieve URL from column: {:?}", url_cell);
        }
    }

    #[test]
    fn fail_on_null_value() {
        initialize();
        let mut container =
            Container::new(&Path::new("/tmp").to_path_buf(), schema_config_without_timestamp()).unwrap();

        let params = IndexParams {
            fields: vec!["url".into()],
            values: vec![serde_json::Value::Null],
        };
        let result = container.index(params);
        assert!(
            result.is_err(),
            "Was expecting error on insert. Got {:?}",
            result
        );

        let url_column = container.columns.find_column("url").unwrap();
        assert_eq!(
            url_column.entries().len(),
            0,
            "was expecting no url, found: {:?}",
            url_column.entries()
        );
    }

    #[test]
    fn reject_insert_when_data_type_is_incompatible() {
        initialize();
        let mut container =
            Container::new(&Path::new("/tmp").to_path_buf(), schema_config_without_timestamp()).unwrap();
        let params = IndexParams {
            fields: vec!["url".into()],
            values: vec![json!(2342)],
        };

        let result = container.index(params);
        assert!(
            result.is_err(),
            "Was expecting error on insert. Got {:?}",
            result
        );

        let url_column = container.columns.find_column("url").unwrap();
        assert_eq!(
            url_column.entries().len(),
            0,
            "was expecting no url, found: {:?}",
            url_column.entries()
        );
    }

    #[test]
    fn reject_insert_for_all_cells_when_one_cell_fails() {
        initialize();
        let mut container = Container::new(
            &Path::new("/tmp").to_path_buf(),
            schema_config_with_timestamp_and_two_columns(),
        )
        .unwrap();
        let params = IndexParams {
            fields: vec!["url".into(), "points".into()],
            values: vec!["https://google.com".into(), serde_json::Value::Null],
        };

        let result = container.index(params);
        assert!(
            result.is_err(),
            "Was expecting error on insert. Got {:?}",
            result
        );

        let url_column = container.columns.find_column("url").unwrap();
        assert_eq!(
            url_column.entries().len(),
            0,
            "was expecting no url, found: {:?}",
            url_column.entries()
        );

        let points_column = container.columns.find_column("points").unwrap();
        assert_eq!(
            points_column.entries().len(),
            0,
            "was expecting no points, found: {:?}",
            points_column.entries()
        );

        let timestamp_column = container.columns.find_column("timestamp").unwrap();
        assert_eq!(
            timestamp_column.entries().len(),
            0,
            "was expecting no timestamp, found: {:?}",
            timestamp_column.entries()
        );
    }

    #[test]
    fn rejected_insert_rolls_back_auto_index() {
        initialize();
        let mut container = Container::new(
            &Path::new("/tmp").to_path_buf(),
            schema_config_with_timestamp_and_two_columns(),
        )
        .unwrap();
        let params = IndexParams {
            fields: vec!["url".into(), "points".into()],
            values: vec!["https://google.com".into(), serde_json::Value::Null],
        };

        assert_eq!(container.index_counter.counter(), 0);

        let result = container.index(params);
        assert!(
            result.is_err(),
            "Was expecting error on insert. Got {:?}",
            result
        );
        assert_eq!(container.index_counter.counter(), 0);

        let id_column = container.columns.find_column("id").unwrap();
        assert_eq!(
            id_column.entries().len(),
            0,
            "Was expecting zero entries in id column"
        );
    }

    #[test]
    fn successful_insert_increases_counter() {
        initialize();
        let mut container = Container::new(
            &Path::new("/tmp").to_path_buf(),
            schema_config_with_timestamp_and_two_columns(),
        )
        .unwrap();
        let params = IndexParams {
            fields: vec!["url".into(), "points".into()],
            values: vec!["https://google.com".into(), 54.into()],
        };

        assert_eq!(container.index_counter.counter(), 0);

        let result = container.index(params);

        assert!(result.is_ok(), "Expected Insert to be successful");

        let id_column = container.columns.find_column("id").unwrap();

        let inserted_value = id_column.entries().first().unwrap();
        assert_eq!(inserted_value, &Cell::Int(1));

        //Index starts counting at 0, therefore we expect the next id to be 1
        assert_eq!(
            container.index_counter.counter(),
            1,
            "Expected Index Counter to have increased after commit"
        );
    }

    #[test]
    fn reject_timestamp_value_when_autotimestamp_is_on() {
        initialize();
        let mut container = Container::new(
            &Path::new("/tmp").to_path_buf(),
            schema_config_with_timestamp_and_two_columns(),
        )
        .unwrap();
        let params = IndexParams {
            fields: vec!["url".into(), "timestamp".into()],
            values: vec!["https://google.com".into(), 54.into()],
        };

        let result = container.index(params);

        assert!(result.is_err(), "Expected Insert to fail");
    }
}
