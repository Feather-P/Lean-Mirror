use async_trait::async_trait;
use sqlx::{Row, SqlitePool};

use super::{JobRepository, JobStateRecord, PersistedJobState, RepositoryError};

/// 基于 SQLite + `sqlx` 的任务状态仓储实现。
#[derive(Debug, Clone)]
pub struct SqliteJobRepository {
    pool: SqlitePool,
}

impl SqliteJobRepository {
    /// 创建 SQLite repository，并由调用方决定是否立即执行初始化。
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    /// 建立数据库连接。
    pub async fn connect(database_url: &str) -> Result<Self, RepositoryError> {
        let pool = SqlitePool::connect(database_url).await?;
        Ok(Self::new(pool))
    }

    /// 初始化任务状态表。
    pub async fn migrate(&self) -> Result<(), RepositoryError> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS job_states (
                mirror_id TEXT PRIMARY KEY,
                state TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                last_error TEXT NULL,
                last_finished_at TEXT NULL
            )
            "#,
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// 暴露底层连接池，便于其他组件组合事务或复用连接。
    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }
}

#[async_trait]
impl JobRepository for SqliteJobRepository {
    async fn upsert_job_state(&self, record: &JobStateRecord) -> Result<(), RepositoryError> {
        sqlx::query(
            r#"
            INSERT INTO job_states (
                mirror_id,
                state,
                updated_at,
                last_error,
                last_finished_at
            )
            VALUES (?1, ?2, ?3, ?4, ?5)
            ON CONFLICT(mirror_id) DO UPDATE SET
                state = excluded.state,
                updated_at = excluded.updated_at,
                last_error = excluded.last_error,
                last_finished_at = excluded.last_finished_at
            "#,
        )
        .bind(record.mirror_id.as_ref())
        .bind(record.state.as_str())
        .bind(record.updated_at)
        .bind(record.last_error.as_deref())
        .bind(record.last_finished_at)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn get_job_state(&self, mirror_id: &str) -> Result<Option<JobStateRecord>, RepositoryError> {
        let maybe_row = sqlx::query(
            r#"
            SELECT mirror_id, state, updated_at, last_error, last_finished_at
            FROM job_states
            WHERE mirror_id = ?1
            "#,
        )
        .bind(mirror_id)
        .fetch_optional(&self.pool)
        .await?;

        maybe_row.map(map_row_to_record).transpose()
    }

    async fn list_job_states(&self) -> Result<Vec<JobStateRecord>, RepositoryError> {
        let rows = sqlx::query(
            r#"
            SELECT mirror_id, state, updated_at, last_error, last_finished_at
            FROM job_states
            ORDER BY mirror_id ASC
            "#,
        )
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter().map(map_row_to_record).collect()
    }
}

fn map_row_to_record(row: sqlx::sqlite::SqliteRow) -> Result<JobStateRecord, RepositoryError> {
    let state_name: String = row.try_get("state")?;

    Ok(JobStateRecord {
        mirror_id: row.try_get::<String, _>("mirror_id")?.into(),
        state: PersistedJobState::from_str(&state_name)?,
        updated_at: row.try_get("updated_at")?,
        last_error: row.try_get("last_error")?,
        last_finished_at: row.try_get("last_finished_at")?,
    })
}
