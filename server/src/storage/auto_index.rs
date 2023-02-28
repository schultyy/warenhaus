use std::{fs, path::Path};

use serde::{Serialize, Deserialize};
use tracing::error;

use super::auto_index_error::AutoIndexError;

#[derive(Debug, Serialize, Deserialize)]
pub struct AutoIndex {
    counter: i64,
    #[serde(skip_serializing, skip_deserializing)]
    file_path: String,
}

impl AutoIndex {
    pub fn load_or_new(root_path: &str) -> Self {
        let root_path = Path::new(root_path);
        let file_path = root_path.join("auto_index");

        match fs::read_to_string(file_path.clone()) {
            Ok(str) => match serde_json::from_str::<Self>(&str) {
                Ok(mut auto_index) => {
                    auto_index.file_path = file_path.to_str().unwrap().to_string();
                    return auto_index;
                }
                Err(serde_err) => {
                    error!("Error while deserializing auto index: {}", serde_err);
                }
            },
            Err(err) => {
                error!("Failed to load auto index: {}", err);
            }
        }

        Self {
            counter: 0,
            file_path: file_path.to_str().unwrap().to_string(),
        }
    }

    pub fn next(&mut self) -> i64 {
        self.counter += 1;
        return self.counter;
    }

    pub fn rollback(&mut self) {
        self.counter -= 1;
    }

    pub fn commit(&self) -> Result<(), AutoIndexError> {
        let j = serde_json::to_string(self)?;
        fs::write(&self.file_path, j)?;
        Ok(())
    }

    pub fn counter(&self) -> i64 {
        self.counter
    }
}
