use std::future::Future;
use std::path::PathBuf;
use std::sync::Arc;

use crate::provider::{ProviderKind, SyncArtifact, SyncError};
use crate::publisher::{PublishError, PublishReceipt, PublisherKind};
use crate::verifier::{VerifyError, VerifyReport, VerifierKind};

/// worker 所服务的阶段类型。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StageKind {
    Sync,
    Verify,
    Publish,
}

/// 供 worker 执行阶段时消费的统一上下文。
#[derive(Debug, Clone)]
pub struct StageContext {
    pub mirror_id: Arc<str>,
    pub stage: StageKind,
    pub workspace_dir: PathBuf,
    pub scratch_dir: PathBuf,
    pub attempt: u32,
}

/// 同步阶段结果载荷。
#[derive(Debug, Clone)]
pub struct SyncStagePayload {
    pub provider_kind: ProviderKind,
    pub artifact: SyncArtifact,
}

/// 校验阶段结果载荷。
#[derive(Debug, Clone)]
pub struct VerifyStagePayload {
    pub verifier_kind: VerifierKind,
    pub report: VerifyReport,
}

/// 发布阶段结果载荷。
#[derive(Debug, Clone)]
pub struct PublishStagePayload {
    pub publisher_kind: PublisherKind,
    pub receipt: PublishReceipt,
}

/// 单阶段成功后的结构化载荷。
#[derive(Debug, Clone)]
pub enum StageSuccessPayload {
    Sync(SyncStagePayload),
    Verify(VerifyStagePayload),
    Publish(PublishStagePayload),
}

/// 单阶段失败后的结构化载荷。
#[derive(Debug, Clone)]
pub enum StageFailurePayload {
    Sync(SyncError),
    Verify(VerifyError),
    Publish(PublishError),
}

/// worker 对 manager 的统一回报结果。
#[derive(Debug, Clone)]
pub enum StageOutcome {
    Started {
        mirror_id: Arc<str>,
        stage: StageKind,
    },
    Succeeded {
        mirror_id: Arc<str>,
        stage: StageKind,
        payload: StageSuccessPayload,
    },
    Failed {
        mirror_id: Arc<str>,
        stage: StageKind,
        payload: StageFailurePayload,
    },
}

/// worker 运行框架级错误。
#[derive(Debug, Clone)]
pub struct StageWorkerError {
    pub message: String,
}

/// Manager 可调度的单阶段 worker 抽象。
pub trait StageWorker: Send + Sync {
    type RunFuture<'a>: Future<Output = Result<StageOutcome, StageWorkerError>> + Send + 'a
    where
        Self: 'a;

    /// 返回该 worker 负责的阶段。
    fn stage_kind(&self) -> StageKind;

    /// 执行一个明确阶段，并向 manager 回传统一结果。
    fn execute<'a>(&'a self, ctx: &'a StageContext) -> Self::RunFuture<'a>;
}

/// 可按阶段查询 worker 的注册表抽象。
pub trait WorkerRegistry: Send + Sync {
    type Worker<'a>: StageWorker + 'a
    where
        Self: 'a;

    /// 根据阶段类型返回对应 worker。
    fn worker_for(&self, stage: StageKind) -> Option<&Self::Worker<'_>>;
}

/// 负责把 manager 的调度请求派发给具体 worker 的抽象。
pub trait StageDispatcher: Send + Sync {
    type DispatchFuture<'a>: Future<Output = Result<(), StageWorkerError>> + Send + 'a
    where
        Self: 'a;

    /// 派发一个阶段执行请求。
    fn dispatch<'a>(&'a self, ctx: &'a StageContext) -> Self::DispatchFuture<'a>;
}
