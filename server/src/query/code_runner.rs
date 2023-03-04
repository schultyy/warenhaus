use std::{fs::File, io::Write, path::Path};

use anyhow::{anyhow, Result};
use tracing::{debug, error, log::warn};
use wasmtime::*;

use crate::{
    query::AssemblyScriptCompiler,
    storage::{column::Cell, ColumnFrame},
};

use super::wasm_error::WasmError;

#[derive(Debug)]
pub struct CodeRunner {
    compiled_query_storage_path: String,
    asm_script_compiler_path: String,
}

impl CodeRunner {
    pub fn find_asm_script_compiler_path() -> Result<String, WasmError> {
        match std::env::var("ASM_SCRIPT_COMPILER_PATH") {
            Ok(path) => Ok(path),
            Err(_) => Err(WasmError::CompilerNotFound),
        }
    }

    pub fn new(compiled_query_storage_path: String) -> Result<Self, WasmError> {
        let asm_script_compiler_path = CodeRunner::find_asm_script_compiler_path()?;
        Ok(Self {
            compiled_query_storage_path,
            asm_script_compiler_path,
        })
    }

    pub fn compile_and_store(&self, asm_script_code: &str, name: &str) -> Result<(), WasmError> {
        let compiler = AssemblyScriptCompiler::new(self.asm_script_compiler_path.to_string());
        let compiled_wat = match compiler.compile_to_wat(&asm_script_code) {
            Ok(compiled) => compiled,
            Err(err) => {
                error!("Failed to compile {}: {}", name, err);
                return Err(WasmError::CompilerError(err.to_string()));
            }
        };

        let mut compiled_file_path = Path::new(&self.compiled_query_storage_path).join(name);
        compiled_file_path.set_extension("wat");
        let mut file = File::create(compiled_file_path)?;
        file.write_all(compiled_wat.as_bytes())?;

        Ok(())
    }

    ///runs a specific query for a single database row
    ///Returns: boolean indicating if the row should be included in the result set
    #[tracing::instrument]
    pub fn execute_map(&self, function_name: &str, row: ColumnFrame) -> Result<bool> {
        let base_path = Path::new(&self.compiled_query_storage_path);
        let filename = base_path.join(format!("{}.wat", function_name));

        debug!("Loading wasm file {:?}", filename);
        let engine = Engine::default();
        let module = Module::from_file(&engine, filename)?;
        let mut store = Store::new(&engine, ());

        let _log_func = Func::wrap(&mut store, |_caller: Caller<'_, ()>| {
            println!("Logging");
        });

        // let imports = [log_func.into()];
        let imports = [];
        let instance = Instance::new(&mut store, &module, &imports)?;

        let run = instance.get_typed_func::<i32, i32>(&mut store, "run")?;

        let id_cell = row.get("id").ok_or_else(||anyhow!("Expected ID - found None"))?;
        let timestamp_cell = row.get("timestamp").ok_or_else(||anyhow!("Expected timestamp - Found None"))?;
    
        let id = id_cell.as_int().ok_or_else(|| anyhow!("Invalid Type for ID Cell: Was expecting i64"))?;
        let timestamp = timestamp_cell.as_int().ok_or_else(|| anyhow!("Invalid Type for ID Cell: Was expecting i64"))?;

        debug!("Calling function {} with {}", function_name, id);
        let should_be_included = run.call(&mut store, (*timestamp as i32).to_owned())? != 0;
        debug!("Call returned: {}", should_be_included);
        Ok(should_be_included)
    }
}
