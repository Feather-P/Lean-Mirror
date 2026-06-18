use thiserror::Error;

/// 管理器在调度、状态转换与通道交互过程中可能返回的错误。
#[derive(Debug, Error)]
pub enum ManagerError {
    #[error("{channel_name} channel is closed")]
    ChannelClosed { channel_name: String },
    #[error("{job_mirror_id} Job not found")]
    JobNotFound { job_mirror_id: String },
    #[error("invalid transition: {from} to {to}")]
    InvalidTransition { from: String, to: String }
}
