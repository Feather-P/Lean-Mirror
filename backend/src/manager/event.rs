use chrono::{DateTime, Utc};

/// 控制生命周期的消息事件
#[derive(Debug, Clone)]
pub enum LifeCycleEvent {
    Shutdown
}

/// 上级控制接口发送的事件消息
#[derive(Debug, Clone)]
pub enum ControlEvent {
    SyncNow { mirror_id: String },
    Pause { mirror_id: String },
    Resume { mirror_id: String },
}

/// 发送给 Worker 的指令
#[derive(Debug, Clone)]
pub enum WorkerCommand {
    Run { mirror_id: String },
}

/// Worker发送的事件消息
#[derive(Debug, Clone)]
pub enum WorkerEvent {
    SyncStarted {
        mirror_id: String,
        started_at: DateTime<Utc>,
    },
    SyncSucceeded {
        mirror_id: String,
        finished_at: DateTime<Utc>,
    },
    SyncFailed {
        mirror_id: String,
        finished_at: DateTime<Utc>,
        error: String,
    },
    VerifyStarted {
        mirror_id: String,
        started_at: DateTime<Utc>,
    },
    VerifySucceeded {
        mirror_id: String,
        finished_at: DateTime<Utc>,
    },
    VerifyFailed {
        mirror_id: String,
        finished_at: DateTime<Utc>,
    },
    PublishStarted {
        mirror_id: String,
        started_at: DateTime<Utc>,
    },
    PublishSucceeded {
        mirror_id: String,
        finished_at: DateTime<Utc>,
    },
    PublishFailed {
        mirror_id: String,
        finished_at: DateTime<Utc>,
        error: String,
    },
}
