//! 任务状态持久化抽象与具体实现。

mod error;
mod job_repository;
mod sqlite;

pub use error::RepositoryError;
pub use job_repository::{JobRepository, JobStateRecord, PersistedJobState};
pub use sqlite::SqliteJobRepository;
