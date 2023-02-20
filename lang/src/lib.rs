use std::{fs::File, io::Write, path::Path};
use tracing::{debug, error, info};

use anyhow::Result;
use thiserror::Error;
use wasmtime::*;

#[derive(Debug, Error)]
pub enum WasmError {
    #[error("Code is invalid")]
    InvalidCode,
    #[error("Assembly Script Compiler not Found")]
    CompilerNotFound,
    #[error("Compiler Error: {0}")]
    CompilerError(String),
    #[error("IO Error")]
    Io {
        #[from]
        source: std::io::Error,
    },
}

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

#[derive(Error, Debug)]
pub enum AssemblyCompilationError {
    #[error("Failed to compile script. Reason: {0}")]
    CompilationError(String),
    #[error("IO Error")]
    Io {
        #[from]
        source: std::io::Error
    }
}

#[derive(Debug)]
pub struct AssemblyScriptCompiler {
    asm_script_compiler_path: String,
}

impl AssemblyScriptCompiler {
    pub fn new(asm_script_compiler_path: String) -> Self {
        Self {
            asm_script_compiler_path,
        }
    }

    #[tracing::instrument]
    pub fn compile_to_wat(&self, code: &str) -> Result<String, AssemblyCompilationError> {
        let mut temp_file = tempfile::Builder::new()
            .prefix("assemblyscript")
            .suffix(".ts")
            .tempfile()?;

        temp_file.write_all(code.as_bytes())?;

        let file_path = temp_file.path();

        let asc_result = std::process::Command::new(&self.asm_script_compiler_path)
            .arg(&file_path)
            .output()?;
        info!("Compilation Status: {}", asc_result.status);
        if asc_result.status.code().unwrap_or_default() != 0 {
            let stderr = String::from_utf8_lossy(&asc_result.stderr);
            error!("STDERR: {}", stderr);
            return Err(AssemblyCompilationError::CompilationError(stderr.to_string()))
        } 

        let stdout = String::from_utf8_lossy(&asc_result.stdout);
        Ok(stdout.to_string())
    }
}

