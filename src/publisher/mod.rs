use std::future::Future;
use std::path::PathBuf;
use std::sync::Arc;

use crate::provider::SyncArtifact;
use crate::verifier::VerifyReport;

/// publisher 类型标识。
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PublisherKind(pub Arc<str>);

/// 发布阶段所需的只读上下文。
#[derive(Debug, Clone)]
pub struct PublishContext {
    pub mirror_id: Arc<str>,
    pub publisher_kind: PublisherKind,
    pub target_dir: PathBuf,
    pub artifact: SyncArtifact,
    pub verify_report: VerifyReport,
    pub attempt: u32,
}

/// 发布成功后的回执。
#[derive(Debug, Clone)]
pub struct PublishReceipt {
    pub mirror_id: Arc<str>,
    pub publisher_kind: PublisherKind,
    pub published_revision: Option<String>,
    pub destination: PathBuf,
}

/// 发布阶段的结构化错误。
#[derive(Debug, Clone)]
pub struct PublishError {
    pub message: String,
}

/// 单次发布执行结果。
pub type PublishResult = Result<PublishReceipt, PublishError>;

/// 发布能力抽象。
pub trait Publisher: Send + Sync {
    type PublishFuture<'a>: Future<Output = PublishResult> + Send + 'a
    where
        Self: 'a;

    /// 返回 publisher 的稳定标识。
    fn kind(&self) -> &PublisherKind;

    /// 发布一份已通过校验的同步产物。
    fn publish<'a>(&'a self, ctx: &'a PublishContext) -> Self::PublishFuture<'a>;
}
