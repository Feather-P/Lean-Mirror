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

/// 表示任务已存在，但尚未进入调度队列的运行状态标记。
#[derive(Debug, Clone)]
pub struct Idle;
/// 表示任务已登记到调度队列、等待执行的运行状态标记。
#[derive(Debug, Clone)]
pub struct Pending;
/// 表示任务当前正在执行某个业务阶段的运行状态。
#[derive(Debug, Clone)]
pub struct Running<Business> {
    /// 当前运行阶段对应的业务状态标记。
    pub business_status: Business,
}
/// 表示任务已被人工或系统暂停的运行状态标记。
#[derive(Debug, Clone)]
pub struct Paused;

/// 带有类型状态的任务实体。
///
/// `RunSt` 用于在编译期表达任务当前所处的运行/业务状态。
#[derive(Debug, Clone)]
pub struct Job<RunSt> {
    /// 任务对应的镜像标识。
    pub mirror_id: Arc<str>,
    /// 任务当前携带的类型状态。
    pub running_status: RunSt,
}

/// 为所有实现了 [`Default`](backend/src/manager/status.rs:38) 的业务状态自动提供默认运行态。
impl<Biz: Default> Default for Running<Biz> {
    fn default() -> Self {
        Self {
            business_status: Biz::default(),
        }
    }
}

/// 擦除具体类型状态后的任务枚举，便于在运行时统一存储和分发。
#[derive(Debug, Clone)]
pub enum AnyJob {
    Idle(Job<Idle>),
    Pending(Job<Pending>),
    Syncing(Job<Running<Syncing>>),
    Verifying(Job<Running<Verifying>>),
    Publishing(Job<Running<Publishing>>),
    Success(Job<Running<Success>>),
    Failed(Job<Running<Failed>>),
    Paused(Job<Paused>),
}

/// 状态转换时可能产生的副作用描述。
#[derive(Debug, Clone)]
pub enum Effect {
    /// 将任务加入调度队列
    QueueEnqueue { mirror_id: Arc<str> },
    /// 将任务从调度队列中移除
    QueueRemove { mirror_id: Arc<str> },
    /// 持久化任务状态
    Persist { mirror_id: Arc<str> },
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
            AnyJob::Idle(_) => "Idle",
            AnyJob::Pending(_) => "Pending",
            AnyJob::Syncing(_) => "Syncing",
            AnyJob::Verifying(_) => "Verifying",
            AnyJob::Publishing(_) => "Publishing",
            AnyJob::Success(_) => "Success",
            AnyJob::Failed(_) => "Failed",
            AnyJob::Paused(_) => "Paused",
        }
    }
}

impl From<Job<Idle>> for AnyJob {
    fn from(job: Job<Idle>) -> Self {
        AnyJob::Idle(job)
    }
}

impl From<Job<Pending>> for AnyJob {
    fn from(job: Job<Pending>) -> Self {
        AnyJob::Pending(job)
    }
}

impl From<Job<Running<Syncing>>> for AnyJob {
    fn from(job: Job<Running<Syncing>>) -> Self {
        AnyJob::Syncing(job)
    }
}

impl From<Job<Running<Verifying>>> for AnyJob {
    fn from(job: Job<Running<Verifying>>) -> Self {
        AnyJob::Verifying(job)
    }
}

impl From<Job<Running<Publishing>>> for AnyJob {
    fn from(job: Job<Running<Publishing>>) -> Self {
        AnyJob::Publishing(job)
    }
}

impl From<Job<Running<Success>>> for AnyJob {
    fn from(job: Job<Running<Success>>) -> Self {
        AnyJob::Success(job)
    }
}

impl From<Job<Running<Failed>>> for AnyJob {
    fn from(job: Job<Running<Failed>>) -> Self {
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
    fn fail(self) -> TransitionPlan<Running<Failed>>;
}

impl Job<Idle> {
    /// 将任务从空闲状态注册进任务队列。
    ///
    /// 会记录最新状态到持久化存储。
    pub fn register(self) -> TransitionPlan<Pending> {
        let mirror_id = self.mirror_id;

        let next = Job::<Pending> {
            mirror_id: mirror_id.clone(),
            running_status: Pending,
        };
        let effects = vec![Effect::Persist {
            mirror_id: mirror_id,
        }];

        TransitionPlan::new(next).with_effects(effects)
    }
}

// 为 Job<Pending> 实现可暂停
impl Suspendable for Job<Pending> {
    /// 将任务从入队状态切换为暂停状态。
    ///
    /// 会将任务移出队列，并记录最新状态到持久化存储。
    fn pause(self) -> TransitionPlan<Paused> {
        let mirror_id = self.mirror_id;

        let next = Job::<Paused> {
            mirror_id: mirror_id.clone(),
            running_status: Paused,
        };
        let effects = vec![
            Effect::QueueRemove {
                mirror_id: mirror_id.clone(),
            },
            Effect::Persist {
                mirror_id: mirror_id,
            },
        ];

        TransitionPlan::new(next).with_effects(effects)
    }
}

impl Suspendable for Job<Idle> {
    /// 将任务从已登但没入队状态切换为暂停状态。
    ///
    /// 记录最新状态到持久化存储。
    fn pause(self) -> TransitionPlan<Paused> {
        let mirror_id = self.mirror_id;

        let next = Job::<Paused> {
            mirror_id: mirror_id.clone(),
            running_status: Paused,
        };
        let effects = vec![
            Effect::Persist {
                mirror_id: mirror_id,
            },
        ];

        TransitionPlan::new(next).with_effects(effects)
    }
}

impl Job<Pending> {
    /// 将任务从任务队列里移出，进入执行器执行
    ///
    /// 并持久化任务状态
    pub fn sync(self) -> TransitionPlan<Running<Syncing>> {
        let mirror_id = self.mirror_id;

        let next = Job::<Running<Syncing>> {
            mirror_id: mirror_id.clone(),
            running_status: Running {
                business_status: Syncing,
            },
        };
        let effects = vec![
            Effect::Sync {
                mirror_id: mirror_id.clone(),
            },
            Effect::Persist { mirror_id },
        ];

        TransitionPlan::new(next).with_effects(effects)
    }
}

impl Job<Running<Syncing>> {
    /// 将任务从同步中状态推进到校验中状态。
    ///
    /// 会记录最新状态到持久化存储。
    pub fn verify(self) -> TransitionPlan<Running<Verifying>> {
        let mirror_id = self.mirror_id;

        let next = Job::<Running<Verifying>> {
            mirror_id: mirror_id.clone(),
            running_status: Running {
                business_status: Verifying,
            },
        };
        let effects = vec![
            Effect::Verify {
                mirror_id: mirror_id.clone(),
            },
            Effect::Persist {
                mirror_id: mirror_id,
            },
        ];

        TransitionPlan::new(next).with_effects(effects)
    }
}

impl Job<Running<Verifying>> {
    /// 将任务从校验中状态推进到发布中状态。
    ///
    /// 会记录最新状态到持久化存储。
    pub fn publish(self) -> TransitionPlan<Running<Publishing>> {
        let mirror_id = self.mirror_id;

        let next = Job::<Running<Publishing>> {
            mirror_id: mirror_id.clone(),
            running_status: Running {
                business_status: Publishing,
            },
        };
        let effects = vec![
            Effect::Publish {
                mirror_id: mirror_id.clone(),
            },
            Effect::Persist {
                mirror_id: mirror_id,
            },
        ];

        TransitionPlan::new(next).with_effects(effects)
    }
}

impl Job<Running<Publishing>> {
    /// 将任务从发布中状态推进到成功状态。
    ///
    /// 会记录最新状态到持久化存储。
    pub fn succeed(self) -> TransitionPlan<Running<Success>> {
        let mirror_id = self.mirror_id;

        let next = Job::<Running<Success>> {
            mirror_id: mirror_id.clone(),
            running_status: Running {
                business_status: Success,
            },
        };
        let effects = vec![Effect::Persist {
            mirror_id: mirror_id,
        }];

        TransitionPlan::new(next).with_effects(effects)
    }
}

impl Failable for Job<Running<Syncing>> {
    /// 将任务从同步中状态转移到失败状态。
    ///
    /// 会记录最新状态到持久化存储。
    fn fail(self) -> TransitionPlan<Running<Failed>> {
        let mirror_id = self.mirror_id;

        let next = Job::<Running<Failed>> {
            mirror_id: mirror_id.clone(),
            running_status: Running {
                business_status: Failed,
            },
        };
        let effects = vec![Effect::Persist {
            mirror_id: mirror_id,
        }];

        TransitionPlan::new(next).with_effects(effects)
    }
}

impl Failable for Job<Running<Verifying>> {
    /// 将任务从校验中状态转移到失败状态。
    ///
    /// 会记录最新状态到持久化存储。
    fn fail(self) -> TransitionPlan<Running<Failed>> {
        let mirror_id = self.mirror_id;

        let next = Job::<Running<Failed>> {
            mirror_id: mirror_id.clone(),
            running_status: Running {
                business_status: Failed,
            },
        };
        let effects = vec![Effect::Persist {
            mirror_id: mirror_id,
        }];

        TransitionPlan::new(next).with_effects(effects)
    }
}

impl Failable for Job<Running<Publishing>> {
    /// 将任务从发布中状态转移到失败状态。
    ///
    /// 会记录最新状态到持久化存储。
    fn fail(self) -> TransitionPlan<Running<Failed>> {
        let mirror_id = self.mirror_id;

        let next = Job::<Running<Failed>> {
            mirror_id: mirror_id.clone(),
            running_status: Running {
                business_status: Failed,
            },
        };
        let effects = vec![Effect::Persist {
            mirror_id: mirror_id,
        }];

        TransitionPlan::new(next).with_effects(effects)
    }
}

impl Job<Running<Success>> {
    /// 将任务从成功状态回到空闲状态。
    ///
    /// 会将任务重新加入队列，等待下一次调度。
    pub fn idle(self) -> TransitionPlan<Idle> {
        let mirror_id = self.mirror_id;

        let next = Job::<Idle> {
            mirror_id: mirror_id.clone(),
            running_status: Idle,
        };
        let effects = vec![Effect::QueueEnqueue {
            mirror_id: mirror_id,
        }];

        TransitionPlan::new(next).with_effects(effects)
    }
}

impl Job<Running<Failed>> {
    /// 将任务从失败状态回到空闲状态。
    ///
    /// 会将任务重新加入队列，等待下一次调度。
    pub fn idle(self) -> TransitionPlan<Idle> {
        let mirror_id = self.mirror_id;

        let next = Job::<Idle> {
            mirror_id: mirror_id.clone(),
            running_status: Idle,
        };
        let effects = vec![Effect::QueueEnqueue {
            mirror_id: mirror_id,
        }];

        TransitionPlan::new(next).with_effects(effects)
    }
}

impl Job<Paused> {
    /// 将任务从暂停状态恢复到运行态（成功分支起点）。
    ///
    /// 会将任务重新加入队列，并记录最新状态到持久化存储。
    pub fn resume(self) -> TransitionPlan<Running<Success>> {
        let mirror_id = self.mirror_id;

        let next = Job::<Running<Success>> {
            mirror_id: mirror_id.clone(),
            running_status: Running {
                business_status: Success,
            },
        };
        let effects = vec![
            Effect::QueueEnqueue {
                mirror_id: mirror_id.clone(),
            },
            Effect::Persist {
                mirror_id: mirror_id,
            },
        ];

        TransitionPlan::new(next).with_effects(effects)
    }
}

