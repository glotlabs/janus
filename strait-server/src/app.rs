use std::{
    collections::BTreeMap,
    fs,
    io::{self, Read, Write},
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::{Duration as StdDuration, Instant},
};

use tracing::info;

use crate::{
    artifacts::ArtifactStore, auth::hash_password, config::Config, db::Database, git,
    models::UserRole, runner::RunnerClient, runner_auth::RunnerSigner, scheduler, web,
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
    prepare_storage(&config)?;

    let db = Database::open(&config.database.path)?;
    db.cleanup_expired_sessions()?;
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

fn prepare_storage(config: &Config) -> Result<(), Box<dyn std::error::Error>> {
    fs::create_dir_all(&config.data_dir)?;
    fs::create_dir_all(&config.repos_dir)?;
    if let Some(parent) = Path::new(&config.database.path).parent() {
        fs::create_dir_all(parent)?;
    }
    Ok(())
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
    let role = UserRole::parse(role).ok_or_else(|| format!("unknown user role: {role}"))?;
    state.db.create_user(username, &password_hash, role)?;
    Ok(())
}

pub(crate) fn bootstrap_admin(
    config_path: &Path,
    username: &str,
    password: impl AsRef<str>,
) -> Result<(), Box<dyn std::error::Error>> {
    let password = password.as_ref();
    let config = Config::load_from_path(config_path)?;
    prepare_storage(&config)?;
    let db = Database::open(&config.database.path)?;
    if db.user_count()? > 0 {
        return Err("bootstrap-admin can only run when no users exist".into());
    }
    validate_bootstrap_admin(username, password)?;
    let password_hash = hash_password(password)?;
    db.create_user(username.trim(), &password_hash, UserRole::Admin)?;
    println!("bootstrapped admin user {}", username.trim());
    Ok(())
}

pub(crate) fn password_from_source(
    source: &BootstrapPasswordSource,
) -> Result<String, Box<dyn std::error::Error>> {
    match source {
        BootstrapPasswordSource::Prompt => prompt_confirmed_password(),
        BootstrapPasswordSource::Stdin => {
            let mut password = String::new();
            io::stdin().read_to_string(&mut password)?;
            Ok(password.trim_end_matches(['\r', '\n']).to_string())
        }
    }
}

pub(crate) fn prompt_confirmed_password() -> Result<String, Box<dyn std::error::Error>> {
    let password = prompt_hidden_password("Password: ")?;
    let confirmation = prompt_hidden_password("Confirm password: ")?;
    if password != confirmation {
        return Err("passwords do not match".into());
    }
    Ok(password)
}

#[cfg(unix)]
pub(crate) fn prompt_hidden_password(prompt: &str) -> Result<String, Box<dyn std::error::Error>> {
    let fd = libc::STDIN_FILENO;
    let mut original = std::mem::MaybeUninit::<libc::termios>::uninit();
    if unsafe { libc::tcgetattr(fd, original.as_mut_ptr()) } != 0 {
        return Err(
            "password prompt requires a terminal; use --password-stdin for non-interactive use"
                .into(),
        );
    }
    let original = unsafe { original.assume_init() };
    let mut hidden = original;
    hidden.c_lflag &= !libc::ECHO;

    eprint!("{prompt}");
    io::stderr().flush()?;
    if unsafe { libc::tcsetattr(fd, libc::TCSANOW, &hidden) } != 0 {
        return Err("failed to disable terminal echo".into());
    }

    let mut password = String::new();
    let read_result = io::stdin().read_line(&mut password);
    let restore_result = unsafe { libc::tcsetattr(fd, libc::TCSANOW, &original) };
    eprintln!();

    read_result?;
    if restore_result != 0 {
        return Err("failed to restore terminal echo".into());
    }
    Ok(password.trim_end_matches(['\r', '\n']).to_string())
}

#[cfg(not(unix))]
pub(crate) fn prompt_hidden_password(_prompt: &str) -> Result<String, Box<dyn std::error::Error>> {
    Err(
        "hidden password prompt is not supported on this platform; use --password-stdin instead"
            .into(),
    )
}

fn validate_bootstrap_admin(username: &str, password: &str) -> Result<(), String> {
    let username = username.trim();
    if username.len() < 3 {
        return Err("username must be at least 3 characters".to_string());
    }
    if username.chars().count() > 64 {
        return Err("username must be 64 characters or fewer".to_string());
    }
    if !username
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-' | '.'))
    {
        return Err("username contains invalid characters".to_string());
    }
    if password.len() < 8 {
        return Err("password must be at least 8 characters".to_string());
    }
    Ok(())
}

fn set_bootstrap_password_source(
    current: &mut BootstrapPasswordSource,
    next: BootstrapPasswordSource,
) -> Result<(), Box<dyn std::error::Error>> {
    if !matches!(current, BootstrapPasswordSource::Prompt) {
        return Err("only one bootstrap password source may be provided".into());
    }
    *current = next;
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
    AdminBootstrapAdmin {
        username: String,
        password_source: BootstrapPasswordSource,
    },
    AdminRunnerKeyShow {
        format: crate::runner_auth::RunnerKeyShowFormat,
    },
    AdminRunnerKeyInit,
    AdminRunnerKeyRotate,
}

#[derive(Clone)]
pub(crate) enum BootstrapPasswordSource {
    Prompt,
    Stdin,
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
                let mut role = Some("admin".to_string());
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
                    role: role.unwrap_or_else(|| "admin".to_string()),
                }
            }
            Some("admin") if args.get(index + 1).map(String::as_str) == Some("bootstrap-admin") => {
                index += 2;
                let mut username = None;
                let mut password_source = BootstrapPasswordSource::Prompt;
                while index < args.len() {
                    match args[index].as_str() {
                        "--username" => {
                            index += 1;
                            username = args.get(index).cloned();
                        }
                        "--password-stdin" => {
                            set_bootstrap_password_source(
                                &mut password_source,
                                BootstrapPasswordSource::Stdin,
                            )?;
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
                Command::AdminBootstrapAdmin {
                    username: username.ok_or("missing --username")?,
                    password_source,
                }
            }
            Some("admin") if args.get(index + 1).map(String::as_str) == Some("runner-key") => {
                index += 2;
                let action = args.get(index).map(String::as_str).unwrap_or("show");
                index += 1;
                let mut format = crate::runner_auth::RunnerKeyShowFormat::Text;
                while index < args.len() {
                    match args[index].as_str() {
                        "--config" => {
                            index += 1;
                            config_path =
                                PathBuf::from(args.get(index).ok_or("missing config path")?);
                        }
                        "--format" => {
                            index += 1;
                            format = match args.get(index).map(String::as_str) {
                                Some("text") => crate::runner_auth::RunnerKeyShowFormat::Text,
                                Some("toml") => crate::runner_auth::RunnerKeyShowFormat::Toml,
                                Some(value) => {
                                    return Err(
                                        format!("unknown runner-key format: {value}").into()
                                    );
                                }
                                None => return Err("missing runner-key format".into()),
                            };
                        }
                        _ => {}
                    }
                    index += 1;
                }
                match action {
                    "init" => Command::AdminRunnerKeyInit,
                    "show" => Command::AdminRunnerKeyShow { format },
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
