use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};

use crate::manager::status::AnyJob;

use super::RepositoryError;

/// 持久化层使用的任务状态表示，避免直接暴露类型状态机到存储层。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PersistedJobState {
    Init,
    Syncing,
    Verifying,
    Publishing,
    Success,
    Failed,
    Paused,
}

impl PersistedJobState {
    /// 返回适合存储在数据库中的稳定状态名。
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Init => "Init",
            Self::Syncing => "Syncing",
            Self::Verifying => "Verifying",
            Self::Publishing => "Publishing",
            Self::Success => "Success",
            Self::Failed => "Failed",
            Self::Paused => "Paused",
        }
    }

    /// 从数据库中的状态名恢复为 repository 层状态枚举。
    pub fn from_str(state: &str) -> Result<Self, RepositoryError> {
        match state {
            "Init" => Ok(Self::Init),
            "Syncing" => Ok(Self::Syncing),
            "Verifying" => Ok(Self::Verifying),
            "Publishing" => Ok(Self::Publishing),
            "Success" => Ok(Self::Success),
            "Failed" => Ok(Self::Failed),
            "Paused" => Ok(Self::Paused),
            _ => Err(RepositoryError::UnknownJobState {
                state: state.to_string(),
            }),
        }
    }
}

/// 任务状态快照，作为 Manager 与 repository 之间的稳定数据边界。
#[derive(Debug, Clone)]
pub struct JobStateRecord {
    pub mirror_id: Arc<str>,
    pub state: PersistedJobState,
    pub updated_at: DateTime<Utc>,
    pub last_error: Option<String>,
    pub last_finished_at: Option<DateTime<Utc>>,
}

impl JobStateRecord {
    /// 基于当前内存态任务构造一个可持久化快照。
    pub fn from_job(job: &AnyJob) -> Self {
        let (mirror_id, state) = match job {
            AnyJob::Init(job) => (job.mirror_id.clone(), PersistedJobState::Init),
            AnyJob::Syncing(job) => (job.mirror_id.clone(), PersistedJobState::Syncing),
            AnyJob::Verifying(job) => (job.mirror_id.clone(), PersistedJobState::Verifying),
            AnyJob::Publishing(job) => (job.mirror_id.clone(), PersistedJobState::Publishing),
            AnyJob::Success(job) => (job.mirror_id.clone(), PersistedJobState::Success),
            AnyJob::Failed(job) => (job.mirror_id.clone(), PersistedJobState::Failed),
            AnyJob::Paused(job) => (job.mirror_id.clone(), PersistedJobState::Paused),
        };

        Self {
            mirror_id,
            state,
            updated_at: Utc::now(),
            last_error: None,
            last_finished_at: None,
        }
    }
}

/// 任务状态持久化抽象接口。
#[async_trait]
pub trait JobRepository: Send + Sync {
    /// 插入或更新一个任务状态快照。
    async fn upsert_job_state(&self, record: &JobStateRecord) -> Result<(), RepositoryError>;

    /// 读取指定任务的最新状态快照。
    async fn get_job_state(&self, mirror_id: &str) -> Result<Option<JobStateRecord>, RepositoryError>;

    /// 列出所有任务的最新状态快照。
    async fn list_job_states(&self) -> Result<Vec<JobStateRecord>, RepositoryError>;
}
