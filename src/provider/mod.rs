pub mod git;
pub mod rsync;

use std::future::Future;
use std::path::PathBuf;
use std::sync::Arc;

/// provider 类型标识。
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ProviderKind(pub Arc<str>);

/// 同步阶段所需的只读上下文。
#[derive(Debug, Clone)]
pub struct SyncContext {
    pub mirror_id: Arc<str>,
    pub provider_kind: ProviderKind,
    pub workspace_dir: PathBuf,
    pub scratch_dir: PathBuf,
    pub attempt: u32,
}

/// 同步阶段产物。
#[derive(Debug, Clone)]
pub struct SyncArtifact {
    pub mirror_id: Arc<str>,
    pub provider_kind: ProviderKind,
    pub snapshot_dir: PathBuf,
    pub revision: Option<String>,
}

/// 同步阶段的结构化错误。
#[derive(Debug, Clone)]
pub struct SyncError {
    pub message: String,
    pub retryable: bool,
}

/// 单次同步执行结果。
pub type SyncResult = Result<SyncArtifact, SyncError>;

/// 同步能力抽象。
pub trait SyncProvider: Send + Sync {
    type SyncFuture<'a>: Future<Output = SyncResult> + Send + 'a
    where
        Self: 'a;

    /// 返回 provider 的稳定标识。
    fn kind(&self) -> &ProviderKind;

    /// 执行一次同步阶段，产出供后续校验使用的产物。
    fn sync<'a>(&'a self, ctx: &'a SyncContext) -> Self::SyncFuture<'a>;
}
