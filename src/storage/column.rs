use super::data_type::DataType;


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

    pub fn name(&self) -> &str {
        self.name.as_ref()
    }

    pub fn data_type(&self) -> &DataType {
        &self.data_type
    }

    pub fn entries_mut(&mut self) -> &mut Vec<Cell> {
        &mut self.entries
    }
}
