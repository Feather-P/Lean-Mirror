use chrono::{DateTime, Utc};
use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

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

#[derive(Debug, Default)]
pub struct JobQueue {
    time_index: BTreeSet<ScheduledJob>,
    state_map: HashMap<Arc<str>, DateTime<Utc>>,
}

impl JobQueue {
    pub fn new() -> Self {
        Self::default()
    }

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

    pub fn peek_time(&self) -> Option<DateTime<Utc>> {
        self.time_index.iter().next().map(|scheduled| scheduled.time)
    }

    pub fn dequeue(&mut self) -> Option<(Arc<str>, DateTime<Utc>)> {
        let scheduled = self.time_index.iter().next()?.clone();
        self.time_index.remove(&scheduled);
        self.state_map.remove(scheduled.mirror_id.as_ref());

        Some((scheduled.mirror_id, scheduled.time))
    }

    pub fn len(&self) -> usize {
        self.time_index.len()
    }

    pub fn is_empty(&self) -> bool {
        self.time_index.is_empty()
    }

    pub fn contains(&self, mirror_id: &str) -> bool {
        self.state_map.contains_key(mirror_id)
    }
}
