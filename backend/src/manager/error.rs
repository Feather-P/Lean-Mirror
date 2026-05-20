use thiserror::Error;

#[derive(Debug, Error)]
pub enum ManagerError {
    #[error("{channel_name} channel is closed")]
    ChannelClosed { channel_name: &'static str },
    #[error("{job_mirror_id} Job not found")]
    JobNotFound { job_mirror_id: &'static str }
}