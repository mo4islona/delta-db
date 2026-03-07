use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("schema error: {0}")]
    Schema(String),

    #[error("storage error: {0}")]
    Storage(String),

    #[error("reducer error: {0}")]
    Reducer(String),

    #[error("rollback error: {0}")]
    Rollback(String),

    #[error("serialization error: {0}")]
    Serialization(#[from] rmp_serde::encode::Error),

    #[error("deserialization error: {0}")]
    Deserialization(#[from] rmp_serde::decode::Error),

    #[error("invalid operation: {0}")]
    InvalidOperation(String),
}

pub type Result<T> = std::result::Result<T, Error>;
