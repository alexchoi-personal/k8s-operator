use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Kubernetes API error: {0}")]
    Kube(#[from] kube::Error),

    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("Missing field: {0}")]
    MissingField(&'static str),

    #[error("Invalid configuration: {0}")]
    InvalidConfig(String),

    #[error("Resource not found: {0}")]
    NotFound(String),

    #[error("{0}")]
    Other(String),
}

pub type Result<T> = std::result::Result<T, Error>;
