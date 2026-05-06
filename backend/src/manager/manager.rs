use chrono::Utc;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::sync::mpsc;
use tokio::time::{Instant, sleep_until};

use crate::manager::event::LifeCycleEvent;

use super::event::{ControlEvent, WorkerCommand, WorkerEvent};
use super::queue::JobQueue;
use super::status::AnyJob;

pub struct Manager {
    jobs: HashMap<Arc<str>, AnyJob>,
    queue: JobQueue,
    lifecycle_event_rx: mpsc::Receiver<LifeCycleEvent>,
    control_event_rx: mpsc::Receiver<ControlEvent>,
    worker_event_rx: mpsc::Receiver<WorkerEvent>,
    worker_cmd_tx: mpsc::Sender<WorkerCommand>,
}

impl Manager {
    pub fn new(
        lifecycle_event_rx: mpsc::Receiver<LifeCycleEvent>,
        control_event_rx: mpsc::Receiver<ControlEvent>,
        worker_event_rx: mpsc::Receiver<WorkerEvent>,
        worker_cmd_tx: mpsc::Sender<WorkerCommand>,
    ) -> Self {
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
                            channel_name: "lifecycle event"
                        })
                    };

                    todo!("调用优雅关机逻辑，让所有线程干完事后自行退出，这里用channel去通知各个线程，
                    如果超时就强制退出，这个实现应该单独封装在一个函数里")
                }
                maybe_control_event = self.control_event_rx.recv() => {
                    let Some(control_event) = maybe_control_event
                    else {
                        return Err(ManagerError::ChannelClosed {
                            channel_name: "control event"
                        })
                    };


                    self.handle_control_event(control_event)?;
                }
                maybe_worker_event = self.worker_event_rx.recv() => {
                    let Some(worker_event) = maybe_worker_event
                    else {
                        return Err(ManagerError::ChannelClosed {
                            channel_name: "worker event"
                        })
                    };

                    self.handle_worker_event(worker_event);
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

    /// 处理上游网页或命令行前端发来的业务控制事件，更新任务状态机，重新安排任务队列，
    fn handle_control_event(
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
                todo!(
                    "对除了Paused之外的状态都有效，让这个Worker在完成本轮worker同步之后进入Pause状态，
                    以后就不入队了"
                )
            }
            ControlEvent::Resume { mirror_id } => {
                todo!(
                    "仅对paused状态的任务有效，把任务标记为Success，然后再入队"
                )
            }
        }
    }

    /// 处理 'worker' 上报的事件信息，并根据信息更新任务状态机、队列，和持久化
    fn handle_worker_event(
        &mut self,
        worker_event: WorkerEvent,
    ) -> Result<(), ManagerError> {
        todo!("还没有实现")
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
}

#[derive(Debug, Error)]
pub enum ManagerError {
    #[error("{channel_name} channel is closed")]
    ChannelClosed { channel_name: &'static str },
}

/// 将 'chrono::DateTime<Utc>' 时间转换为 'tokio::time::instant
fn to_instant(time_utc: chrono::DateTime<Utc>) -> Instant {
    let now_utc = Utc::now();
    let now_instant = Instant::now();

    // 如果出现传入的时间已经超过现在时间的情况下，不加偏移
    let diff = (time_utc - now_utc).to_std().unwrap_or(Duration::ZERO);

    now_instant + diff
}
