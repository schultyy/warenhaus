use std::{fs::File, io::Read, path::Path};

use serde::Deserialize;
use tracing::{instrument, info};

#[derive(Deserialize, Clone, Debug)]
pub enum DataTypeConfig {
    Int,
    Float,
    String,
    Boolean,
}

#[derive(Deserialize, Debug)]
pub struct SchemaConfig {
    pub columns: Vec<ColumnConfig>,
    ///Indicates wheter there should be an automatically generated timestamp column
    pub add_timestamp_column: bool
}

#[derive(Deserialize, Clone, Debug)]
pub struct ColumnConfig {
    pub name: String,
    pub data_type: DataTypeConfig,
}

#[derive(Debug)]
pub struct Configurator {
    root_path: String,
}

impl Configurator {
    #[instrument]
    pub fn new(root_path: &str) -> Self {
        Self {
            root_path: root_path.into(),
        }
    }

    #[instrument]
    pub fn load(&self) -> Result<SchemaConfig, std::io::Error> {
        let root_path = Path::new(&self.root_path);
        let schema_json_path = root_path.join("schema.json");
        let mut file = File::open(schema_json_path)?;
        let mut data = String::new();
        file.read_to_string(&mut data).unwrap();
        let data: SchemaConfig = serde_json::from_str(&data)?;
        info!("Loaded configuration: {:?}", data);
        Ok(data)
    }
}
