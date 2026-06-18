use std::future::Future;
use std::sync::Arc;

use crate::provider::SyncArtifact;

/// verifier 类型标识。
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct VerifierKind(pub Arc<str>);

/// 校验结论。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VerifyVerdict {
    Passed,
    Failed,
}

/// 校验阶段所需的只读上下文。
#[derive(Debug, Clone)]
pub struct VerifyContext {
    pub mirror_id: Arc<str>,
    pub verifier_kind: VerifierKind,
    pub artifact: SyncArtifact,
    pub attempt: u32,
}

/// 校验报告。
#[derive(Debug, Clone)]
pub struct VerifyReport {
    pub mirror_id: Arc<str>,
    pub verifier_kind: VerifierKind,
    pub verdict: VerifyVerdict,
    pub warnings: Vec<String>,
}

/// 校验阶段的结构化错误。
#[derive(Debug, Clone)]
pub struct VerifyError {
    pub message: String,
    pub retryable: bool,
}

/// 单次校验执行结果。
pub type VerifyResult = Result<VerifyReport, VerifyError>;

/// 校验能力抽象。
pub trait Verifier: Send + Sync {
    type VerifyFuture<'a>: Future<Output = VerifyResult> + Send + 'a
    where
        Self: 'a;

    /// 返回 verifier 的稳定标识。
    fn kind(&self) -> &VerifierKind;

    /// 对同步阶段产物进行校验。
    fn verify<'a>(&'a self, ctx: &'a VerifyContext) -> Self::VerifyFuture<'a>;
}
