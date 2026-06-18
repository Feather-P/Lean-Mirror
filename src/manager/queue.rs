use chrono::{DateTime, Utc};
use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

/// 队列内部使用的调度项，按计划执行时间与任务标识排序。
#[derive(Debug, Clone, Eq, PartialEq)]
struct ScheduledJob {
    mirror_id: Arc<str>,
    time: DateTime<Utc>,
}

impl Ord for ScheduledJob {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.time
            .cmp(&other.time)
            .then_with(|| self.mirror_id.cmp(&other.mirror_id))
    }
}

impl PartialOrd for ScheduledJob {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// 基于时间排序的任务调度队列。
///
/// 内部使用 [`BTreeSet`](backend/src/manager/queue.rs:27) 维护时间顺序，
/// 使用 [`HashMap`](backend/src/manager/queue.rs:28) 支持按 `mirror_id` 快速覆盖与删除。
#[derive(Debug, Default)]
pub struct JobQueue {
    time_index: BTreeSet<ScheduledJob>,
    state_map: HashMap<Arc<str>, DateTime<Utc>>,
}

impl JobQueue {
    /// 创建一个空的 [`JobQueue`](backend/src/manager/queue.rs:26)。
    pub fn new() -> Self {
        Self::default()
    }

    /// 安排一个任务在指定时间执行。
    ///
    /// 如果同一个 `mirror_id` 已存在旧计划，则会先覆盖旧计划再插入新计划。
    pub fn schedule(&mut self, mirror_id: String, time: DateTime<Utc>) {
        let mirror_id: Arc<str> = Arc::from(mirror_id);

        if let Some(&old_time) = self.state_map.get(mirror_id.as_ref()) {
            let old_job = ScheduledJob {
                mirror_id: mirror_id.clone(),
                time: old_time,
            };
            self.time_index.remove(&old_job);
        }

        self.time_index.insert(ScheduledJob {
            mirror_id: mirror_id.clone(),
            time,
        });
        self.state_map.insert(mirror_id, time);
    }

    /// 从队列中移除指定任务。
    ///
    /// 如果任务不存在，则该操作为空操作。
    pub fn remove(&mut self, mirror_id: &str) {
        if let Some(&old_time) = self.state_map.get(mirror_id) {
            let arc_id: Arc<str> = Arc::from(mirror_id);

            self.time_index.remove(&ScheduledJob {
                mirror_id: arc_id,
                time: old_time,
            });
            self.state_map.remove(mirror_id);
        }
    }

    /// 获取任务队列中下一个任务的规划执行时间
    ///
    /// 如果队列已空，则会返回 'None'
    pub fn peek_time(&self) -> Option<DateTime<Utc>> {
        self.time_index
            .iter()
            .next()
            .map(|scheduled| scheduled.time)
    }

    /// 弹出最早到期的任务。
    pub fn dequeue(&mut self) -> Option<(Arc<str>, DateTime<Utc>)> {
        let scheduled = self.time_index.iter().next()?.clone();
        self.time_index.remove(&scheduled);
        self.state_map.remove(scheduled.mirror_id.as_ref());

        Some((scheduled.mirror_id, scheduled.time))
    }

    /// 返回当前队列中的任务数量。
    pub fn len(&self) -> usize {
        self.time_index.len()
    }

    /// 判断队列是否为空。
    pub fn is_empty(&self) -> bool {
        self.time_index.is_empty()
    }

    /// 判断指定任务是否已在队列中登记。
    pub fn contains(&self, mirror_id: &str) -> bool {
        self.state_map.contains_key(mirror_id)
    }
}
