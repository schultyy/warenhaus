use std::{fs::File, io::Read, path::Path};

use serde::Deserialize;
use tracing::instrument;

#[derive(Deserialize, Clone)]
pub enum DataTypeConfig {
    Int,
    Float,
    String,
    Boolean,
}

#[derive(Deserialize)]
pub struct SchemaConfig {
    pub columns: Vec<ColumnConfig>,
}

#[derive(Deserialize, Clone)]
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
    pub fn new() -> Self {
        Self {
            root_path: ".".into(),
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
        Ok(data)
    }
}
