use thiserror::Error;

#[derive(Error, Debug)]
pub enum AutoIndexError {
    #[error("JSON Error")]
    Json {
        #[from]
        source: serde_json::Error,
    },
    #[error("IO Error")]
    Io {
        #[from]
        source: std::io::Error,
    },
}
