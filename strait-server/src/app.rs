use std::{
    collections::BTreeMap,
    fs, io,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::{Duration as StdDuration, Instant},
};

use tracing::info;

use crate::{
    artifacts::ArtifactStore, auth::hash_password, config::Config, db::Database, git,
    runner::RunnerClient, runner_auth::RunnerSigner, scheduler, web,
};

#[derive(Clone)]
pub struct AppState {
    pub(crate) config: Arc<Config>,
    pub(crate) db: Database,
    pub(crate) artifacts: ArtifactStore,
    pub(crate) runner_client: RunnerClient,
    pub(crate) runner_signer: RunnerSigner,
    pub(crate) config_path: Arc<PathBuf>,
    pub(crate) server_bin: Arc<PathBuf>,
    pub(crate) login_attempts: Arc<Mutex<BTreeMap<String, LoginWindow>>>,
}

#[derive(Clone)]
pub(crate) struct LoginWindow {
    pub(crate) started_at: Instant,
    pub(crate) count: u64,
}

pub(crate) fn build_state(
    config_path: PathBuf,
    server_bin: PathBuf,
) -> Result<Arc<AppState>, Box<dyn std::error::Error>> {
    let config = Arc::new(Config::load_from_path(&config_path)?);
    fs::create_dir_all(&config.data_dir)?;
    fs::create_dir_all(&config.repos_dir)?;
    if let Some(parent) = Path::new(&config.database.path).parent() {
        fs::create_dir_all(parent)?;
    }

    let db = Database::open(&config.database.path)?;
    db.cleanup_expired_sessions()?;
    let admin_hash = hash_password(&config.auth.bootstrap_admin.password)?;
    db.ensure_user(&config.auth.bootstrap_admin.username, &admin_hash, "admin")?;
    let runner_signer = RunnerSigner::load_or_generate(&config.runner_auth)?;

    Ok(Arc::new(AppState {
        artifacts: ArtifactStore::new(&config.data_dir)?,
        config,
        db,
        runner_client: RunnerClient::new(runner_signer.clone()),
        runner_signer,
        config_path: Arc::new(config_path),
        server_bin: Arc::new(server_bin),
        login_attempts: Arc::new(Mutex::new(BTreeMap::new())),
    }))
}

pub(crate) async fn serve(state: Arc<AppState>) -> Result<(), Box<dyn std::error::Error>> {
    let address: SocketAddr = state.config.server.listen.parse()?;
    scheduler::spawn(Arc::clone(&state));
    let app = web::build_router(state);
    let listener = tokio::net::TcpListener::bind(address).await?;
    info!(listen = %address, "strait-server listening");
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    Ok(())
}

pub(crate) fn hook_post_receive(
    state: Arc<AppState>,
    repo_id: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let refs = git::read_push_refs(&mut io::stdin())?;
    let event_key = git::event_key(repo_id, &refs);
    state.db.create_push_event(repo_id, &event_key, &refs)?;
    Ok(())
}

pub(crate) fn reconcile_hooks(state: Arc<AppState>) -> Result<(), Box<dyn std::error::Error>> {
    for repo in state.db.list_repos()? {
        git::install_post_receive_hook(
            Path::new(&repo.bare_path),
            state.server_bin.as_path(),
            state.config_path.as_path(),
            &repo.id,
        )?;
    }
    Ok(())
}

pub(crate) fn seed_user(
    state: Arc<AppState>,
    username: &str,
    password: &str,
    role: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let password_hash = hash_password(password)?;
    state.db.create_user(username, &password_hash, role)?;
    Ok(())
}

impl AppState {
    pub(crate) fn allow_login_attempt(&self, username: &str) -> bool {
        let mut windows = self
            .login_attempts
            .lock()
            .expect("login attempt mutex poisoned");
        let now = Instant::now();
        let window = windows.entry(username.to_string()).or_insert(LoginWindow {
            started_at: now,
            count: 0,
        });
        if now.duration_since(window.started_at) >= StdDuration::from_secs(60) {
            window.started_at = now;
            window.count = 0;
        }
        if window.count >= self.config.auth.login_rate_limit_per_minute {
            return false;
        }
        window.count += 1;
        true
    }
}

pub(crate) fn init_tracing() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "strait_server=info,axum=info".into()),
        )
        .json()
        .flatten_event(true)
        .init();
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut terminate =
            signal(SignalKind::terminate()).expect("failed to install SIGTERM handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {},
            _ = terminate.recv() => {},
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

pub(crate) struct Cli {
    pub(crate) config_path: PathBuf,
    pub(crate) command: Command,
}

pub(crate) enum Command {
    Serve,
    HookPostReceive {
        repo_id: String,
    },
    AdminReconcileHooks,
    AdminSeedUser {
        username: String,
        password: String,
        role: String,
    },
    AdminRunnerKeyShow,
    AdminRunnerKeyRotate,
}

impl Cli {
    pub(crate) fn from_env() -> Result<Self, Box<dyn std::error::Error>> {
        let args = std::env::args().skip(1).collect::<Vec<_>>();
        let mut config_path = PathBuf::from("server.toml");
        if args.is_empty() {
            return Ok(Self {
                config_path,
                command: Command::Serve,
            });
        }
        let mut index = 0;
        let command = match args.get(index).map(String::as_str) {
            Some("serve") => {
                index += 1;
                while index < args.len() {
                    if args[index] == "--config" {
                        index += 1;
                        config_path = PathBuf::from(args.get(index).ok_or("missing config path")?);
                    }
                    index += 1;
                }
                Command::Serve
            }
            Some("hook") if args.get(index + 1).map(String::as_str) == Some("post-receive") => {
                index += 2;
                let mut repo_id = None;
                while index < args.len() {
                    match args[index].as_str() {
                        "--repo-id" => {
                            index += 1;
                            repo_id = args.get(index).cloned();
                        }
                        "--config" => {
                            index += 1;
                            config_path =
                                PathBuf::from(args.get(index).ok_or("missing config path")?);
                        }
                        _ => {}
                    }
                    index += 1;
                }
                Command::HookPostReceive {
                    repo_id: repo_id.ok_or("missing --repo-id")?,
                }
            }
            Some("admin") if args.get(index + 1).map(String::as_str) == Some("reconcile-hooks") => {
                index += 2;
                while index < args.len() {
                    if args[index] == "--config" {
                        index += 1;
                        config_path = PathBuf::from(args.get(index).ok_or("missing config path")?);
                    }
                    index += 1;
                }
                Command::AdminReconcileHooks
            }
            Some("admin") if args.get(index + 1).map(String::as_str) == Some("seed-user") => {
                index += 2;
                let mut username = None;
                let mut password = None;
                let mut role = Some("developer".to_string());
                while index < args.len() {
                    match args[index].as_str() {
                        "--username" => {
                            index += 1;
                            username = args.get(index).cloned();
                        }
                        "--password" => {
                            index += 1;
                            password = args.get(index).cloned();
                        }
                        "--role" => {
                            index += 1;
                            role = args.get(index).cloned();
                        }
                        "--config" => {
                            index += 1;
                            config_path =
                                PathBuf::from(args.get(index).ok_or("missing config path")?);
                        }
                        _ => {}
                    }
                    index += 1;
                }
                Command::AdminSeedUser {
                    username: username.ok_or("missing --username")?,
                    password: password.ok_or("missing --password")?,
                    role: role.unwrap_or_else(|| "developer".to_string()),
                }
            }
            Some("admin") if args.get(index + 1).map(String::as_str) == Some("runner-key") => {
                index += 2;
                let action = args.get(index).map(String::as_str).unwrap_or("show");
                index += 1;
                while index < args.len() {
                    match args[index].as_str() {
                        "--config" => {
                            index += 1;
                            config_path =
                                PathBuf::from(args.get(index).ok_or("missing config path")?);
                        }
                        _ => {}
                    }
                    index += 1;
                }
                match action {
                    "show" => Command::AdminRunnerKeyShow,
                    "rotate" => Command::AdminRunnerKeyRotate,
                    _ => return Err(format!("unknown runner-key action: {action}").into()),
                }
            }
            _ => Command::Serve,
        };
        Ok(Self {
            config_path,
            command,
        })
    }
}
