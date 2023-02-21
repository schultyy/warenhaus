use std::io::Write;
use tracing::{error, info};

use thiserror::Error;

pub mod code_runner;
pub mod wasm_error;

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

