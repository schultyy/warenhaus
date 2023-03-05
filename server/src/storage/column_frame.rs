use std::collections::HashMap;

use serde::Serialize;

use super::cell::Cell;

#[derive(Serialize, Debug, Clone)]
pub struct ColumnFrame {
    column_names: Vec<String>,
    column_values: Vec<Cell>,
}

impl ColumnFrame {
    pub fn new() -> Self {
        Self {
            column_names: vec![],
            column_values: vec![],
        }
    }

    pub fn insert(&mut self, column_name: &str, cell: Cell) {
        self.column_names.push(column_name.to_owned());
        self.column_values.push(cell);
    }

    pub fn get(&self, column_name: &str) -> Option<&Cell> {
        if let Some(index) = self.column_names.iter().position(|c| c == column_name) {
            let cell = self.column_values.get(index).expect("Encountered ColumnFrame. Internal Index Mismatch");
            Some(cell)
        }
        else {
            None
        }
    }

    pub fn to_view_object(&self) -> HashMap<String, Cell> {
        let mut map = HashMap::new();
        for i in 0..self.column_names.len() {
            let column = self.column_names[i].to_owned();
            let column_value = self.column_values[i].to_owned();
            map.insert(column, column_value);
        }

        map
    }
}
