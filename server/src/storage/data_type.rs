use std::fmt::Display;

use crate::config::DataTypeConfig;

use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub enum DataType {
    Int,
    Float,
    String,
    Boolean,
}

impl DataType {
    pub fn is_compatible(&self, other: &Value) -> bool {
        match self {
            DataType::Int => other.is_i64(),
            DataType::Float => other.is_f64(),
            DataType::String => other.is_string(),
            DataType::Boolean => other.is_boolean(),
        }
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

impl From<DataTypeConfig> for DataType {
    fn from(value: DataTypeConfig) -> Self {
        match value {
            DataTypeConfig::Int => DataType::Int,
            DataTypeConfig::Float => DataType::Float, 
            DataTypeConfig::String => DataType::String,
            DataTypeConfig::Boolean => DataType::Boolean,
        }
    }
}
