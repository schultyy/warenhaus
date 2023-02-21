use std::{path::Path, fs::File, io::Write};

use tracing::{error, debug};
use wasmtime::*;
use anyhow::Result;

use crate::{wasm_error::WasmError, AssemblyScriptCompiler};

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
            },
        };

        let mut compiled_file_path = Path::new(&self.compiled_query_storage_path).join(name);
        compiled_file_path.set_extension("wat");
        let mut file = File::create(compiled_file_path)?;
        file.write_all(compiled_wat.as_bytes())?;

        Ok(())
    }

    #[tracing::instrument]
    pub fn execute_map(&self, function_name: &str) -> Result<()> {

        let base_path = Path::new(&self.compiled_query_storage_path);
        let filename = base_path.join(format!("{}.wat", function_name));

        debug!("Loading wasm file {:?}", filename);
        let engine = Engine::default();
        // let module = Module
        let module = Module::from_file(&engine, filename)?;
        let mut store = Store::new(&engine, ());

        let log_func = Func::wrap(&mut store, |_caller: Caller<'_, ()>| {
            println!("Logging");
        });

        let imports = [log_func.into()];
        let instance = Instance::new(&mut store, &module, &imports)?;

        let run = instance.get_typed_func::<(), ()>(&mut store, "run")?;

        run.call(&mut store, ())?;

        Ok(())
    }
}
