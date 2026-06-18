use chrono::Utc;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::{Instant, sleep_until};

use crate::manager::event::LifeCycleEvent;
use crate::manager::status::{Failable, Suspendable};

use super::error::ManagerError;
use super::event::{ControlEvent, WorkerCommand, WorkerEvent};
use super::queue::JobQueue;
use super::status::AnyJob;
use super::status::{Effect, Job, TransitionPlan};

/// 负责协调任务状态机、调度队列与 worker 事件的中心管理器。
pub struct Manager {
    jobs: HashMap<Arc<str>, AnyJob>,
    queue: JobQueue,
    lifecycle_event_rx: mpsc::Receiver<LifeCycleEvent>,
    control_event_rx: mpsc::Receiver<ControlEvent>,
    worker_event_rx: mpsc::Receiver<WorkerEvent>,
    worker_cmd_tx: mpsc::Sender<WorkerCommand>,
}

impl Manager {
    /// 初始化 [`Manager`](backend/src/manager/manager.rs:18)，并绑定所需的事件通道。
    pub fn init(
        lifecycle_event_rx: mpsc::Receiver<LifeCycleEvent>,
        control_event_rx: mpsc::Receiver<ControlEvent>,
        worker_event_rx: mpsc::Receiver<WorkerEvent>,
        worker_cmd_tx: mpsc::Sender<WorkerCommand>,
    ) -> Self {
        // 这里要从配置文件和数据库装载所有的任务列表
        Self {
            jobs: HashMap::new(),
            queue: JobQueue::new(),
            lifecycle_event_rx,
            control_event_rx,
            worker_event_rx,
            worker_cmd_tx,
        }
    }

    /// # 开始运行 Manager 管理器的事件循环
    ///
    /// 该异步函数启动一个使用 `tokio::select!` 的事件循环
    /// ## 运行逻辑
    /// 该函数将会利用队列的 `peek_time()` 方法获取任务队列顶部的事件，设置下一个 tick 的时间
    pub async fn run_event_loop(mut self) -> Result<(), ManagerError> {
        loop {
            let next_tick_time = self.queue.peek_time().map(to_instant);

            tokio::select! {
                maybe_lifecycle_event = self.lifecycle_event_rx.recv() => {
                    let Some(lifecycle_event) = maybe_lifecycle_event
                    else {
                        return Err(ManagerError::ChannelClosed {
                            channel_name: "lifecycle event".into()
                        })
                    };

                    todo!("调用优雅关机逻辑，让所有线程干完事后自行退出，这里用channel去通知各个线程，
                    如果超时就强制退出，这个实现应该单独封装在一个函数里")
                }
                maybe_control_event = self.control_event_rx.recv() => {
                    let Some(control_event) = maybe_control_event
                    else {
                        return Err(ManagerError::ChannelClosed {
                            channel_name: "control event".into()
                        })
                    };


                    self.handle_control_event(control_event).await?;
                }
                maybe_worker_event = self.worker_event_rx.recv() => {
                    let Some(worker_event) = maybe_worker_event
                    else {
                        return Err(ManagerError::ChannelClosed {
                            channel_name: "worker event".into()
                        })
                    };

                    self.handle_worker_event(worker_event).await?;
                }
                _ = async {
                    if let Some(deadline) = next_tick_time {
                        sleep_until(deadline).await;
                    }
                }, if next_tick_time.is_some() => {
                    self.tick().await?;
                }
            }
        }

        Ok(())
    }

    /// 处理上游网页或命令行前端发来的控制事件。
    ///
    /// 该方法会依据事件类型推进任务状态机，并在需要时调整调度队列。
    async fn handle_control_event(
        &mut self,
        control_event: ControlEvent,
    ) -> Result<(), ManagerError> {
        match control_event {
            ControlEvent::SyncNow { mirror_id } => {
                todo!(
                    "仅对在Success和Failed状态下的任务进行，
                    因为这个时候他们并没有进行同步
                    将指定的任务从等待队列中移除，并直接插队到执行线程池上去"
                );
            }
            ControlEvent::Pause { mirror_id } => {
                let Some(any_job) = self.jobs.remove(mirror_id.as_str()) else {
                    return Err(ManagerError::JobNotFound {
                        job_mirror_id: mirror_id.into(),
                    });
                };

                match any_job {
                    // 这里是目前分支处理的baseline，其他分支也按此处方式处理
                    AnyJob::Paused(job) => {
                        self.jobs.insert(mirror_id.into(), job.into());
                        Ok(())
                    }
                    AnyJob::Pending(job) => self.apply_plan(job.pause()).await,
                    AnyJob::Idle(_) => {
                        return Err(ManagerError::InvalidTransition {
                            from: "Idle".into(),
                            to: "Paused".into(),
                        });
                    }
                    AnyJob::Syncing(job) => todo!(),
                    AnyJob::Verifying(job) => todo!(),
                    AnyJob::Publishing(job) => todo!(),
                    AnyJob::Success(job) => todo!(),
                    AnyJob::Failed(job) => todo!(),
                }
            }
            ControlEvent::Resume { mirror_id } => {
                todo!("仅对paused状态的任务有效，把任务标记为Success，然后再入队")
            }
        }
    }

    /// 处理 worker 上报的事件，并据此推进任务状态机。
    ///
    /// 当事件与当前任务状态不匹配时，会返回 [`ManagerError::InvalidTransition`](backend/src/manager/error.rs:10)。
    async fn handle_worker_event(&mut self, worker_event: WorkerEvent) -> Result<(), ManagerError> {
        let mirror_id = match &worker_event {
            WorkerEvent::SyncSucceeded { mirror_id, .. }
            | WorkerEvent::SyncFailed { mirror_id, .. }
            | WorkerEvent::VerifySucceeded { mirror_id, .. }
            | WorkerEvent::VerifyFailed { mirror_id, .. }
            | WorkerEvent::PublishSucceeded { mirror_id, .. }
            | WorkerEvent::PublishFailed { mirror_id, .. } => mirror_id.clone(),
        };

        let Some(any_job) = self.jobs.remove(mirror_id.as_str()) else {
            return Err(ManagerError::JobNotFound {
                job_mirror_id: mirror_id,
            });
        };

        match (any_job, worker_event) {
            (AnyJob::Syncing(job), WorkerEvent::SyncSucceeded { .. }) => self.apply_plan(job.verify()).await,
            (AnyJob::Verifying(job), WorkerEvent::VerifySucceeded { .. }) => self.apply_plan(job.publish()).await,
            (AnyJob::Publishing(job), WorkerEvent::PublishSucceeded { .. }) => self.apply_plan(job.succeed()).await,

            (AnyJob::Syncing(job), WorkerEvent::SyncFailed { .. }) => self.apply_plan(job.fail()).await,
            (AnyJob::Verifying(job), WorkerEvent::VerifyFailed { .. }) => self.apply_plan(job.fail()).await,
            (AnyJob::Publishing(job), WorkerEvent::PublishFailed { .. }) => self.apply_plan(job.fail()).await,

            (job, event) => {
                let from = job.state_name().to_string();
                self.jobs.insert(mirror_id.into(), job);
                Err(ManagerError::InvalidTransition {
                    from,
                    to: format!("worker_event::{event:?}"),
                })
            }
        }
    }

    /// 处理队列中所有已到期的任务
    ///
    /// 该函数不断检查队列顶部的任务时间，如果该时间早于或等于当前时间，
    /// 则将其出队并在线程池进行分发。
    async fn tick(&mut self) -> Result<(), ManagerError> {
        while let Some(time) = self.queue.peek_time() {
            if time > Utc::now() {
                break;
            }

            let Some((mirror_id, _)) = self.queue.dequeue() else {
                break;
            };

            todo!("这里需要实现具体的分发逻辑，最好是后面再统一线程池spawn或者用tokio的")
        }

        Ok(())
    }

    /// 应用一次状态机转换计划。
    ///
    /// 该方法会先写回新的任务状态，再顺序执行转换附带的副作用。
    pub async fn apply_plan<NextSt>(
        &mut self,
        plan: TransitionPlan<NextSt>,
    ) -> Result<(), ManagerError>
    where
        Job<NextSt>: Into<AnyJob>,
    {   
        //TODO 这里有问题，合理的流程应该是：manager中状态机发生转换
        //     然后spawn异步一个新的worker线程去执行effect内的内容
        //     effect执行完毕之后，通过channel向manager汇报，
        //.    manager收到汇报之后，落库，然后发生下一次状态机转换，spawn一个新的worker线程，重复
        let key = plan.next.mirror_id.clone();
        let new_job = plan.next.into();
        self.jobs.insert(key, new_job);
        match do_effects(plan.effects).await {
            Ok(_) => {
                return Ok(());
            }
            Err(e) => return Err(e),
        };
    }
}

/// 将 [`chrono::DateTime<Utc>`](backend/src/manager/manager.rs:236) 转换为 [`tokio::time::Instant`](backend/src/manager/manager.rs:236)。
fn to_instant(time_utc: chrono::DateTime<Utc>) -> Instant {
    let now_utc = Utc::now();
    let now_instant = Instant::now();

    // 如果出现传入的时间已经超过现在时间的情况下，不加偏移
    let diff = (time_utc - now_utc).to_std().unwrap_or(Duration::ZERO);

    now_instant + diff
}

/// 顺序执行状态转换过程中产生的副作用列表。
async fn do_effects(effects: Vec<Effect>) -> Result<(), ManagerError> {
    todo!()
}
