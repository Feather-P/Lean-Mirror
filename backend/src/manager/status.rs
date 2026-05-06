use std::sync::Arc;

// 业务进度状态
#[derive(Debug, Clone)]
pub struct Syncing;
#[derive(Debug, Clone)]
pub struct Verifying;
#[derive(Debug, Clone)]
pub struct Publishing;
#[derive(Debug, Clone)]
pub struct Success;
#[derive(Debug, Clone)]
pub struct Failed;

// 调度运行状态
#[derive(Debug, Clone)]
pub struct Idle;
#[derive(Debug, Clone)]
pub struct Running<Business> {
    pub business_status: Business,
}
#[derive(Debug, Clone)]
pub struct Paused;

#[derive(Debug, Clone)]
pub struct Job<RunSt> {
    pub mirror_id: Arc<str>,
    pub running_status: RunSt,
}

// 为所有带有 Default 的 Biz 自动实现default
impl<Biz: Default> Default for Running<Biz> {
    fn default() -> Self {
        Self {
            business_status: Biz::default(),
        }
    }
}

#[derive(Debug, Clone)]
pub enum AnyJob {
    Idle(Job<Idle>),
    Syncing(Job<Running<Syncing>>),
    Verifying(Job<Running<Verifying>>),
    Publishing(Job<Running<Publishing>>),
    Success(Job<Running<Success>>),
    Failed(Job<Running<Failed>>),
    Paused(Job<Paused>),
}

#[derive(Debug, Clone)]
pub enum Effect {
    /// 将任务加入调度队列
    QueueEnqueue { mirror_id: Arc<str> },
    /// 将任务从调度队列中移除
    QueueRemove { mirror_id: Arc<str> },
    /// 立刻触发 Worker 执行
    DispatchRunNow { mirror_id: Arc<str> },
    /// 持久化任务状态
    Persist { mirror_id: Arc<str> },
}

#[derive(Debug, Clone)]
pub struct TransitionPlan<NextSt> {
    pub next: Job<NextSt>,
    pub effects: Vec<Effect>,
}

impl<NextSt> TransitionPlan<NextSt> {
    pub fn new(next: Job<NextSt>) -> Self {
        Self {
            next,
            effects: Vec::<Effect>::new(),
        }
    }

    pub fn with_effect(mut self, effect: Effect) -> Self {
        self.effects.push(effect);
        self
    }

    pub fn with_effects(mut self, effects: impl IntoIterator<Item = Effect>) -> Self {
        self.effects.extend(effects);
        self
    }
}

impl AnyJob {
    pub fn state_name(&self) -> &'static str {
        match self {
            AnyJob::Idle(_) => "Idle",
            AnyJob::Syncing(_) => "Syncing",
            AnyJob::Verifying(_) => "Verifying",
            AnyJob::Publishing(_) => "Publishing",
            AnyJob::Success(_) => "Success",
            AnyJob::Failed(_) => "Failed",
            AnyJob::Paused(_) => "Paused",
        }
    }
}

pub trait Suspendable {
    fn pause(self) -> TransitionPlan<Paused>;
}

pub trait Failable {
    fn fail(self) -> TransitionPlan<Running<Failed>>;
}

impl Job<Idle> {
    /// 将任务从空闲状态推进到同步中状态。
    ///
    /// 会记录最新状态到持久化存储。
    pub fn sync(self) -> TransitionPlan<Running<Syncing>> {
        let mirror_id = self.mirror_id;

        let next = Job::<Running<Syncing>> {
            mirror_id: mirror_id.clone(),
            running_status: Running {
                business_status: Syncing,
            },
        };
        let effects = vec![Effect::Persist {
            mirror_id: mirror_id,
        }];

        TransitionPlan::new(next).with_effects(effects)
    }
}

// 为 Job<Idle> 实现可暂停
impl Suspendable for Job<Idle> {
    /// 将任务从空闲状态切换为暂停状态。
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
        let effects = vec![Effect::Persist {
            mirror_id: mirror_id,
        }];

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
        let effects = vec![Effect::Persist {
            mirror_id: mirror_id,
        }];

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
