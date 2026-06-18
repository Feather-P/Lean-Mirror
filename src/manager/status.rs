use std::sync::Arc;

/// 表示任务正处于同步阶段的业务状态标记。
#[derive(Debug, Clone)]
pub struct Syncing;
/// 表示任务正处于校验阶段的业务状态标记。
#[derive(Debug, Clone)]
pub struct Verifying;
/// 表示任务正处于发布阶段的业务状态标记。
#[derive(Debug, Clone)]
pub struct Publishing;
/// 表示任务已成功完成一次完整处理流程的业务状态标记。
#[derive(Debug, Clone)]
pub struct Success;
/// 表示任务在处理流程中失败的业务状态标记。
#[derive(Debug, Clone)]
pub struct Failed;

/// 表示任务刚创建完成、尚未进入过任何处理流程的初始状态。
#[derive(Debug, Clone)]
pub struct Init;
/// 表示任务已被人工或系统暂停的运行状态标记。
#[derive(Debug, Clone)]
pub struct Paused;

/// 带有类型状态的任务实体。
///
/// `St` 用于在编译期表达任务当前所处的单层状态。
#[derive(Debug, Clone)]
pub struct Job<St> {
    /// 任务对应的镜像标识。
    pub mirror_id: Arc<str>,
    /// 任务当前携带的类型状态。
    pub state: St,
}

/// 擦除具体类型状态后的任务枚举，便于在运行时统一存储和分发。
#[derive(Debug, Clone)]
pub enum AnyJob {
    Init(Job<Init>),
    Syncing(Job<Syncing>),
    Verifying(Job<Verifying>),
    Publishing(Job<Publishing>),
    Success(Job<Success>),
    Failed(Job<Failed>),
    Paused(Job<Paused>),
}

/// 状态转换时可能产生的副作用描述。
#[derive(Debug, Clone)]
pub enum Effect {
    /// 将任务加入调度队列
    QueueEnqueue { mirror_id: Arc<str> },
    /// 将任务从调度队列中移除
    QueueRemove { mirror_id: Arc<str> },
    /// 启动同步执行器
    Sync { mirror_id: Arc<str> },
    /// 启动验证执行器
    Verify { mirror_id: Arc<str> },
    /// 启动发布执行器
    Publish { mirror_id: Arc<str> },
}

/// 一次状态转换的结果描述。
///
/// 包含下一个任务状态，以及需要由管理器执行的副作用列表。
#[derive(Debug, Clone)]
pub struct TransitionPlan<NextSt> {
    /// 转换后的任务状态。
    pub next: Job<NextSt>,
    /// 为完成该转换需要执行的副作用。
    pub effects: Vec<Effect>,
}

/// # 任务状态机转移计划
///
/// 任务状态机的转移应消耗转移计划
impl<NextSt> TransitionPlan<NextSt> {
    /// 基于下一个状态创建一个空副作用列表的转换计划。
    pub fn new(next: Job<NextSt>) -> Self {
        Self {
            next,
            effects: Vec::<Effect>::new(),
        }
    }

    /// 追加一个副作用，并返回更新后的转换计划。
    pub fn with_effect(mut self, effect: Effect) -> Self {
        self.effects.push(effect);
        self
    }

    /// 追加多个副作用，并返回更新后的转换计划。
    pub fn with_effects(mut self, effects: impl IntoIterator<Item = Effect>) -> Self {
        self.effects.extend(effects);
        self
    }
}

impl AnyJob {
    /// 返回当前任务状态对应的人类可读名称。
    pub fn state_name(&self) -> &'static str {
        match self {
            AnyJob::Init(_) => "Init",
            AnyJob::Syncing(_) => "Syncing",
            AnyJob::Verifying(_) => "Verifying",
            AnyJob::Publishing(_) => "Publishing",
            AnyJob::Success(_) => "Success",
            AnyJob::Failed(_) => "Failed",
            AnyJob::Paused(_) => "Paused",
        }
    }
}

impl From<Job<Init>> for AnyJob {
    fn from(job: Job<Init>) -> Self {
        AnyJob::Init(job)
    }
}

impl From<Job<Syncing>> for AnyJob {
    fn from(job: Job<Syncing>) -> Self {
        AnyJob::Syncing(job)
    }
}

impl From<Job<Verifying>> for AnyJob {
    fn from(job: Job<Verifying>) -> Self {
        AnyJob::Verifying(job)
    }
}

impl From<Job<Publishing>> for AnyJob {
    fn from(job: Job<Publishing>) -> Self {
        AnyJob::Publishing(job)
    }
}

impl From<Job<Success>> for AnyJob {
    fn from(job: Job<Success>) -> Self {
        AnyJob::Success(job)
    }
}

impl From<Job<Failed>> for AnyJob {
    fn from(job: Job<Failed>) -> Self {
        AnyJob::Failed(job)
    }
}

impl From<Job<Paused>> for AnyJob {
    fn from(job: Job<Paused>) -> Self {
        AnyJob::Paused(job)
    }
}

/// 表示任务支持被暂停到 [`Paused`](backend/src/manager/status.rs:32) 状态。
pub trait Suspendable {
    /// 生成一次暂停转换计划。
    fn pause(self) -> TransitionPlan<Paused>;
}

/// 表示任务支持转移到失败态。
pub trait Failable {
    /// 生成一次失败转换计划。
    fn fail(self) -> TransitionPlan<Failed>;
}

impl Job<Init> {
    /// 将任务注册到调度队列。
    ///
    /// 状态保持为 [`Init`](src/manager/status.rs)，排队事实由队列本身表达。
    pub fn register(self) -> TransitionPlan<Init> {
        let mirror_id = self.mirror_id;

        let next = Job::<Init> {
            mirror_id: mirror_id.clone(),
            state: Init,
        };

        TransitionPlan::new(next).with_effect(Effect::QueueEnqueue { mirror_id })
    }
}

impl Suspendable for Job<Init> {
    /// 将任务从初始态切换为暂停状态。
    ///
    /// 如果任务当前在队列中，副作用会将其移出队列。
    fn pause(self) -> TransitionPlan<Paused> {
        let mirror_id = self.mirror_id;

        let next = Job::<Paused> {
            mirror_id: mirror_id.clone(),
            state: Paused,
        };
        let effects = vec![Effect::QueueRemove {
            mirror_id: mirror_id.clone(),
        }];

        TransitionPlan::new(next).with_effects(effects)
    }
}

impl Suspendable for Job<Success> {
    /// 将任务从成功态切换为暂停状态。
    fn pause(self) -> TransitionPlan<Paused> {
        let mirror_id = self.mirror_id;

        let next = Job::<Paused> {
            mirror_id: mirror_id.clone(),
            state: Paused,
        };

        TransitionPlan::new(next).with_effect(Effect::QueueRemove { mirror_id })
    }
}

impl Suspendable for Job<Failed> {
    /// 将任务从失败态切换为暂停状态。
    fn pause(self) -> TransitionPlan<Paused> {
        let mirror_id = self.mirror_id;

        let next = Job::<Paused> {
            mirror_id: mirror_id.clone(),
            state: Paused,
        };

        TransitionPlan::new(next).with_effect(Effect::QueueRemove { mirror_id })
    }
}

impl Job<Init> {
    /// 将初始态任务推进到同步阶段。
    pub fn sync(self) -> TransitionPlan<Syncing> {
        let mirror_id = self.mirror_id;

        let next = Job::<Syncing> {
            mirror_id: mirror_id.clone(),
            state: Syncing,
        };
        let effects = vec![Effect::Sync {
            mirror_id: mirror_id.clone(),
        }];

        TransitionPlan::new(next).with_effects(effects)
    }
}

impl Job<Success> {
    /// 将成功态任务重新推进到同步阶段。
    pub fn sync(self) -> TransitionPlan<Syncing> {
        let mirror_id = self.mirror_id;

        let next = Job::<Syncing> {
            mirror_id: mirror_id.clone(),
            state: Syncing,
        };
        let effects = vec![Effect::Sync {
            mirror_id: mirror_id.clone(),
        }];

        TransitionPlan::new(next).with_effects(effects)
    }
}

impl Job<Failed> {
    /// 将失败态任务重新推进到同步阶段。
    pub fn sync(self) -> TransitionPlan<Syncing> {
        let mirror_id = self.mirror_id;

        let next = Job::<Syncing> {
            mirror_id: mirror_id.clone(),
            state: Syncing,
        };
        let effects = vec![Effect::Sync {
            mirror_id: mirror_id.clone(),
        }];

        TransitionPlan::new(next).with_effects(effects)
    }
}

impl Job<Syncing> {
    /// 将任务从同步中状态推进到校验中状态。
    ///
    /// 会记录最新状态到持久化存储。
    pub fn verify(self) -> TransitionPlan<Verifying> {
        let mirror_id = self.mirror_id;

        let next = Job::<Verifying> {
            mirror_id: mirror_id.clone(),
            state: Verifying,
        };
        let effects = vec![Effect::Verify {
            mirror_id: mirror_id.clone(),
        }];

        TransitionPlan::new(next).with_effects(effects)
    }
}

impl Job<Verifying> {
    /// 将任务从校验中状态推进到发布中状态。
    ///
    /// 会记录最新状态到持久化存储。
    pub fn publish(self) -> TransitionPlan<Publishing> {
        let mirror_id = self.mirror_id;

        let next = Job::<Publishing> {
            mirror_id: mirror_id.clone(),
            state: Publishing,
        };
        let effects = vec![Effect::Publish {
            mirror_id: mirror_id.clone(),
        }];

        TransitionPlan::new(next).with_effects(effects)
    }
}

impl Job<Publishing> {
    /// 将任务从发布中状态推进到成功状态。
    pub fn succeed(self) -> TransitionPlan<Success> {
        let mirror_id = self.mirror_id;

        let next = Job::<Success> {
            mirror_id: mirror_id.clone(),
            state: Success,
        };

        TransitionPlan::new(next)
    }
}

impl Failable for Job<Syncing> {
    /// 将任务从同步中状态转移到失败状态。
    ///
    /// 会记录最新状态到持久化存储。
    fn fail(self) -> TransitionPlan<Failed> {
        let mirror_id = self.mirror_id;

        let next = Job::<Failed> {
            mirror_id: mirror_id.clone(),
            state: Failed,
        };

        TransitionPlan::new(next)
    }
}

impl Failable for Job<Verifying> {
    /// 将任务从校验中状态转移到失败状态。
    fn fail(self) -> TransitionPlan<Failed> {
        let mirror_id = self.mirror_id;

        let next = Job::<Failed> {
            mirror_id: mirror_id.clone(),
            state: Failed,
        };

        TransitionPlan::new(next)
    }
}

impl Failable for Job<Publishing> {
    /// 将任务从发布中状态转移到失败状态。
    fn fail(self) -> TransitionPlan<Failed> {
        let mirror_id = self.mirror_id;

        let next = Job::<Failed> {
            mirror_id: mirror_id.clone(),
            state: Failed,
        };

        TransitionPlan::new(next)
    }
}

impl Job<Success> {
    /// 将成功态任务重新加入队列，但保持成功状态不变。
    pub fn enqueue(self) -> TransitionPlan<Success> {
        let mirror_id = self.mirror_id;

        let next = Job::<Success> {
            mirror_id: mirror_id.clone(),
            state: Success,
        };
        let effects = vec![Effect::QueueEnqueue {
            mirror_id: mirror_id,
        }];

        TransitionPlan::new(next).with_effects(effects)
    }
}

impl Job<Failed> {
    /// 将失败态任务重新加入队列，但保持失败状态不变。
    pub fn enqueue(self) -> TransitionPlan<Failed> {
        let mirror_id = self.mirror_id;

        let next = Job::<Failed> {
            mirror_id: mirror_id.clone(),
            state: Failed,
        };
        let effects = vec![Effect::QueueEnqueue {
            mirror_id: mirror_id,
        }];

        TransitionPlan::new(next).with_effects(effects)
    }
}

impl Job<Paused> {
    /// 将任务从暂停状态恢复到初始态，并重新加入队列。
    ///
    /// 会将任务重新加入队列
    pub fn resume(self) -> TransitionPlan<Init> {
        let mirror_id = self.mirror_id;

        let next = Job::<Init> {
            mirror_id: mirror_id.clone(),
            state: Init,
        };
        let effects = vec![Effect::QueueEnqueue {
            mirror_id: mirror_id.clone(),
        }];

        TransitionPlan::new(next).with_effects(effects)
    }
}
