mod app;
mod artifacts;
mod auth;
mod config;
mod db;
mod git;
mod models;
mod runner;
mod runner_auth;
mod scheduler;
mod schema_diff;
mod state_machine;
mod web;

use std::env;

use app::{
    Cli, Command, bootstrap_admin, build_state, hook_post_receive, init_tracing,
    password_from_source, reconcile_hooks, seed_user, serve,
};
use runner_auth::{init_runner_key, rotate_runner_key, show_runner_key};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_tracing();

    let cli = Cli::from_env()?;
    match &cli.command {
        Command::AdminRunnerKeyInit => return init_runner_key(&cli.config_path),
        Command::AdminRunnerKeyShow { format } => {
            return show_runner_key(&cli.config_path, *format);
        }
        Command::AdminRunnerKeyRotate => {
            return rotate_runner_key(&cli.config_path);
        }
        Command::AdminBootstrapAdmin {
            username,
            password_source,
        } => {
            let password = password_from_source(password_source)?;
            return bootstrap_admin(&cli.config_path, username, password);
        }
        _ => {}
    }

    let state = build_state(cli.config_path.clone(), env::current_exe()?)?;

    match cli.command {
        Command::Serve => serve(state).await,
        Command::HookPostReceive { repo_id } => hook_post_receive(state, &repo_id),
        Command::AdminReconcileHooks => reconcile_hooks(state),
        Command::AdminSeedUser {
            username,
            password,
            role,
        } => seed_user(state, &username, &password, &role),
        Command::AdminBootstrapAdmin { .. } => unreachable!(),
        Command::AdminRunnerKeyInit
        | Command::AdminRunnerKeyShow { .. }
        | Command::AdminRunnerKeyRotate => {
            unreachable!()
        }
    }
}
