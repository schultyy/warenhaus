use thiserror::Error;

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
