use thiserror::Error;

/// Repository 层统一错误定义。
#[derive(Debug, Error)]
pub enum RepositoryError {
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),
    #[error("unknown persisted job state: {state}")]
    UnknownJobState { state: String },
}
