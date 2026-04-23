use chrono::Duration;
use chrono_tz::Tz;
use serde::{Deserialize, Serialize};
use serde_with::{DurationMilliSeconds, DurationSeconds, serde_as};
use std::fs;
use std::io;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use thiserror::Error;
use toml;

const CONFIG_VERSION: u32 = 1;

#[derive(Deserialize, Serialize, Debug)]
#[serde(rename_all = "kebab-case")]
pub struct AppConfig {
    pub version: u32,
    pub path: PathConfig,
    pub time: TimeConfig,
    pub database: DbConfig,
    pub api: ApiConfig,
    pub manager: ManagerConfig,
    pub worker: WorkerConfig,
    pub shutdown: ShutdownConfig,
    pub publish: PublishConfig,
    #[serde(default)]
    pub mirrors: Vec<MirrorConfig>,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct PathConfig {
    pub data_dir: PathBuf,
    pub pub_dir: PathBuf,
    pub log_dir: PathBuf,
    pub tmp_dir: PathBuf,
    pub run_dir: PathBuf,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct TimeConfig {
    pub timezone: Tz,
}

#[serde_as]
#[derive(Deserialize, Serialize, Debug)]
pub struct RetryConfig {
    pub max_attempts: u32,
    #[serde(rename = "first-backoff-secs")]
    #[serde_as(as = "DurationSeconds<i64>")]
    pub first_backoff_dur: Duration,
    #[serde(rename = "max-backoff-secs")]
    #[serde_as(as = "DurationSeconds<i64>")]
    pub max_backoff_dur: Duration,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct PublishConfig {
    pub driver: PublishDriver,
    pub keep_generations: u32,
    #[serde(default)]
    pub symlink: Option<SymlinkPublishConfig>,
    #[serde(default)]
    pub btrfs: Option<BtrfsPublishConfig>,
}

impl PublishConfig {
    fn validate(&self) -> Result<(), ConfigError> {
        match self.driver {
            PublishDriver::Symlink => {
                if self.symlink.is_none() {
                    return Err(ConfigError::InvalidField {
                        field: "publish.symlink",
                        reason: "required when driver is 'symlink'".to_string(),
                    });
                }
            }
            PublishDriver::Btrfs => {
                if self.btrfs.is_none() {
                    return Err(ConfigError::InvalidField {
                        field: "publish.btrfs",
                        reason: "required when driver is 'btrfs'".to_string(),
                    });
                }
            }
        }
        Ok(())
    }
}

#[derive(Deserialize, Serialize, Debug, Default)]
pub struct SymlinkPublishConfig {
    pub snapshot_dir: PathBuf,
}

#[derive(Deserialize, Serialize, Debug, Default)]
pub struct BtrfsPublishConfig {
    pub subvolume_path: PathBuf,
}

#[derive(Deserialize, Serialize, Debug)]
pub enum PublishDriver {
    Symlink,
    Btrfs,
}

#[serde_as]
#[derive(Deserialize, Serialize, Debug)]
pub struct DbConfig {
    pub sqlite_path: PathBuf,
    #[serde(rename = "busy-timeout-ms")]
    #[serde_as(as = "DurationMilliSeconds<i64>")]
    pub busy_timeout: Duration,
    #[serde(default)]
    pub journal_mode: DbJournalMode,
    #[serde(default)]
    pub synchronous: DbSynchronous,
}

impl DbConfig {
    fn validate(&self) -> Result<(), ConfigError> {
        if self.busy_timeout <= Duration::zero() {
            return Err(ConfigError::InvalidField {
                field: "database.busy-timeout-ms",
                reason: "must be positive".to_string(),
            });
        }
        Ok(())
    }
}

#[derive(Deserialize, Serialize, Debug)]
#[serde(rename_all = "lowercase")]
pub enum DbJournalMode {
    Delete,
    Truncate,
    Persist,
    Memory,
    Wal,
    Off,
}

impl Default for DbJournalMode {
    fn default() -> Self {
        Self::Wal
    }
}

#[derive(Deserialize, Serialize, Debug)]
#[serde(rename_all = "lowercase")]
pub enum DbSynchronous {
    Off,
    Normal,
    Full,
    Extra,
}

impl Default for DbSynchronous {
    fn default() -> Self {
        Self::Normal
    }
}

#[serde_as]
#[derive(Deserialize, Serialize, Debug)]
pub struct ApiConfig {
    pub bind: SocketAddr,
    #[serde_as(as = "DurationSeconds<i64>")]
    #[serde(rename = "request-timeout-secs")]
    pub request_timeout: Duration,
    pub access_log: bool,
}

impl ApiConfig {
    fn validate(&self) -> Result<(), ConfigError> {
        if self.request_timeout <= Duration::zero() {
            return Err(ConfigError::InvalidField {
                field: "api.request-timeout-secs",
                reason: "must be positive".to_string(),
            });
        }
        Ok(())
    }
}

#[derive(Deserialize, Serialize, Debug)]
pub struct ManagerConfig {
    pub readonly_mode: bool,
    pub config_reload_watch: bool,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct WorkerConfig {
    pub worker_num: u64,
}

#[serde_as]
#[derive(Deserialize, Serialize, Debug)]
pub struct ShutdownConfig {
    #[serde_as(as = "DurationSeconds<i64>")]
    #[serde(rename = "grace-period-secs")]
    pub grace_period: Duration,
    #[serde_as(as = "DurationSeconds<i64>")]
    #[serde(rename = "force-kill-after-secs")]
    pub force_kill_limit: Duration,
    #[serde(rename = "publish-is-non-interruptible")]
    pub publish_non_stopable: bool,
}

impl ShutdownConfig {
    fn validate(&self) -> Result<(), ConfigError> {
        if self.force_kill_limit <= Duration::zero() {
            return Err(ConfigError::InvalidField {
                field: "shutdown.force-kill-after-secs",
                reason: "must be positive".to_string(),
            });
        }
        if self.grace_period <= Duration::zero() {
            return Err(ConfigError::InvalidField {
                field: "shutdown.grace-period-secs",
                reason: "must be positive".to_string(),
            });
        }
        if self.force_kill_limit < self.grace_period {
            return Err(ConfigError::InvalidField {
                field: "shutdown.grace-period-secs AND shutdown.force-kill-after-secs",
                reason: "grace-period must longer than force-kill".to_string(),
            });
        }
        Ok(())
    }
}

#[serde_as]
#[derive(Serialize, Deserialize, Debug)]
pub struct MirrorConfig {
    pub id: String,
    #[serde(rename = "interval-secs")]
    #[serde_as(as = "DurationSeconds<i64>")]
    pub interval: Duration,
    #[serde(default)]
    pub keep_generations: Option<u32>,
    pub provider: Provider,
    pub provider_config: ProviderConfig,
}

impl MirrorConfig {
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.id.trim().is_empty() {
            return Err(ConfigError::InvalidField {
                field: "mirrors.id",
                reason: "must not be empty".to_string(),
            });
        }

        if self.interval <= Duration::zero() {
            return Err(ConfigError::InvalidField {
                field: "mirrors.interval-secs",
                reason: "must be positive".to_string(),
            });
        }

        self.provider_config.validate(&self.provider)?;
        Ok(())
    }
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "lowercase")]
pub enum Provider {
    Git,
    Rsync,
    Http,
    Exec,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ProviderConfig {
    pub source: String,
    pub args: Vec<String>,
}

impl ProviderConfig {
    fn validate(&self, provider: &Provider) -> Result<(), ConfigError> {
        if self.source.trim().is_empty() {
            return Err(ConfigError::InvalidField {
                field: "mirrors.provider-config.source",
                reason: "must not be empty".to_string(),
            });
        }

        if self.args.iter().any(|arg| arg.trim().is_empty()) {
            return Err(ConfigError::InvalidField {
                field: "mirrors.provider-config.args",
                reason: "must not contain empty argument".to_string(),
            });
        }

        match provider {
            Provider::Git => {
                let src = self.source.trim();
                let valid = src.starts_with("http://")
                    || src.starts_with("https://")
                    || src.starts_with("ssh://")
                    || src.starts_with("git@")
                    || src.starts_with("file://")
                    || src.starts_with('/')
                    || src.starts_with("./")
                    || src.starts_with("../");

                if !valid {
                    return Err(ConfigError::InvalidField {
                        field: "mirrors.provider-config.source",
                        reason: "invalid git source".to_string(),
                    });
                }
            }
            Provider::Rsync => {
                let src = self.source.trim();
                let valid = src.starts_with("rsync://") || src.contains(":/") || src.contains("::");
                if !valid {
                    return Err(ConfigError::InvalidField {
                        field: "mirrors.provider-config.source",
                        reason: "invalid rsync source".to_string(),
                    });
                }
            }
            Provider::Http => {
                let src = self.source.trim();
                if !(src.starts_with("http://") || src.starts_with("https://")) {
                    return Err(ConfigError::InvalidField {
                        field: "mirrors.provider-config.source",
                        reason: "http provider requires http:// or https:// source".to_string(),
                    });
                }
            }
            Provider::Exec => {
                let src = self.source.trim();
                if src.contains('\n') || src.contains('\r') {
                    return Err(ConfigError::InvalidField {
                        field: "mirrors.provider-config.source",
                        reason: "exec source must be a single-line command".to_string(),
                    });
                }
            }
        }

        Ok(())
    }
}

pub struct ConfigLoadOptions {
    config_path: PathBuf,
}

impl ConfigLoadOptions {
    pub fn new(config_path: PathBuf) -> Self {
        Self { config_path }
    }
}

fn load_raw_config(path: &Path) -> Result<AppConfig, ConfigError> {
    if path.as_os_str().is_empty() {
        return Err(ConfigError::SourceUnavailable(
            "empty config path".to_string(),
        ));
    }

    if !path.exists() {
        return Err(ConfigError::SourceUnavailable(format!(
            "config file not found: {}",
            path.display()
        )));
    }

    let content = fs::read_to_string(path)?;
    let parsed = toml::from_str::<AppConfig>(&content).map_err(ConfigError::ParseToml)?;
    Ok(parsed)
}

fn apply_env_overrides(mut _cfg: AppConfig) -> Result<AppConfig, ConfigError> {
    // TODO: 把环境变量确定下来之后用这个方法读取并覆盖，目前直接透明传递
    Ok(_cfg)
}

pub fn load_config(opts: &ConfigLoadOptions) -> Result<AppConfig, ConfigError> {
    let raw = load_raw_config(&opts.config_path)?;
    validate_config_version(raw.version)?;
    let env_applied = apply_env_overrides(raw)?;
    let resolved = apply_inheritance(env_applied)?;
    validate_config(&resolved)?;
    Ok(resolved)
}

fn validate_config_version(version: u32) -> Result<(), ConfigError> {
    if version != CONFIG_VERSION {
        return Err(ConfigError::ConfigVersionMismatch {
            expected: CONFIG_VERSION,
            actual: version,
        });
    }

    Ok(())
}

fn validate_config(cfg: &AppConfig) -> Result<(), ConfigError> {
    cfg.publish.validate()?;
    cfg.shutdown.validate()?;
    cfg.api.validate()?;
    cfg.database.validate()?;

    for mirror in &cfg.mirrors {
        mirror.validate()?;
    }

    Ok(())
}

fn apply_inheritance(mut cfg: AppConfig) -> Result<AppConfig, ConfigError> {
    for mirror in &mut cfg.mirrors {
        if mirror.keep_generations.is_none() {
            mirror.keep_generations = Some(cfg.publish.keep_generations);
        }
    }

    Ok(cfg)
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("config source unavailable: {0}")]
    SourceUnavailable(String),
    #[error("failed to parse TOML config: {0}")]
    ParseToml(toml::de::Error),
    #[error("invalid config field '{field}': {reason}")]
    InvalidField { field: &'static str, reason: String },
    #[error("config version mismatch: expected {expected}, got {actual}")]
    ConfigVersionMismatch { expected: u32, actual: u32 },
    #[error("i/o error when loading config: {0}")]
    Io(#[from] io::Error),
}
