use std::sync::{Arc, Mutex};

use chrono::Utc;
use rusqlite::{Connection, OptionalExtension, params};
use uuid::Uuid;

use crate::models::{
    JobRun, JobRunArtifact, JobRunDetail, PipelineRun, PipelineSnapshot, PushEvent, PushEventRef,
    Repo, Runner, User, Workflow,
};

#[derive(Clone)]
pub struct Database {
    conn: Arc<Mutex<Connection>>,
}

impl Database {
    pub fn open(path: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let conn = Connection::open(path)?;
        conn.execute_batch(
            r#"
            PRAGMA foreign_keys = ON;

            CREATE TABLE IF NOT EXISTS users (
                id TEXT PRIMARY KEY,
                username TEXT NOT NULL UNIQUE,
                password_hash TEXT NOT NULL,
                role TEXT NOT NULL,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS sessions (
                id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                expires_at TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS repos (
                id TEXT PRIMARY KEY,
                owner_id TEXT NOT NULL,
                name TEXT NOT NULL,
                normalized_name TEXT NOT NULL,
                bare_path TEXT NOT NULL,
                default_branch TEXT NOT NULL,
                created_at TEXT NOT NULL,
                UNIQUE(owner_id, normalized_name),
                FOREIGN KEY(owner_id) REFERENCES users(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS runners (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL UNIQUE,
                base_url TEXT NOT NULL,
                token_encrypted_or_local_ref TEXT NOT NULL,
                enabled INTEGER NOT NULL,
                last_health_state TEXT NOT NULL,
                last_seen_at TEXT,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS runner_jobs (
                id TEXT PRIMARY KEY,
                runner_id TEXT NOT NULL,
                job_name TEXT NOT NULL,
                definition_json TEXT NOT NULL,
                last_refreshed_at TEXT NOT NULL,
                UNIQUE(runner_id, job_name),
                FOREIGN KEY(runner_id) REFERENCES runners(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS workflows (
                id TEXT PRIMARY KEY,
                repo_id TEXT NOT NULL,
                name TEXT NOT NULL,
                enabled INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY(repo_id) REFERENCES repos(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS workflow_versions (
                id TEXT PRIMARY KEY,
                workflow_id TEXT NOT NULL,
                version INTEGER NOT NULL,
                trigger_json TEXT NOT NULL,
                definition_json TEXT NOT NULL,
                created_at TEXT NOT NULL,
                UNIQUE(workflow_id, version),
                FOREIGN KEY(workflow_id) REFERENCES workflows(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS push_events (
                id TEXT PRIMARY KEY,
                repo_id TEXT NOT NULL,
                received_at TEXT NOT NULL,
                event_key TEXT NOT NULL UNIQUE,
                processed_at TEXT,
                FOREIGN KEY(repo_id) REFERENCES repos(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS push_event_refs (
                id TEXT PRIMARY KEY,
                push_event_id TEXT NOT NULL,
                old_rev TEXT NOT NULL,
                new_rev TEXT NOT NULL,
                ref_name TEXT NOT NULL,
                FOREIGN KEY(push_event_id) REFERENCES push_events(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS pipeline_runs (
                id TEXT PRIMARY KEY,
                repo_id TEXT NOT NULL,
                workflow_id TEXT NOT NULL,
                workflow_version_id TEXT NOT NULL,
                trigger_type TEXT NOT NULL,
                trigger_ref TEXT,
                commit_sha TEXT,
                status TEXT NOT NULL,
                started_at TEXT NOT NULL,
                finished_at TEXT,
                FOREIGN KEY(repo_id) REFERENCES repos(id) ON DELETE CASCADE,
                FOREIGN KEY(workflow_id) REFERENCES workflows(id) ON DELETE CASCADE,
                FOREIGN KEY(workflow_version_id) REFERENCES workflow_versions(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS job_runs (
                id TEXT PRIMARY KEY,
                pipeline_run_id TEXT NOT NULL,
                job_id TEXT NOT NULL,
                job_name TEXT NOT NULL,
                runner_id TEXT NOT NULL,
                runner_job_name TEXT NOT NULL,
                runner_run_id TEXT,
                status TEXT NOT NULL,
                allow_failure INTEGER NOT NULL,
                started_at TEXT,
                finished_at TEXT,
                FOREIGN KEY(pipeline_run_id) REFERENCES pipeline_runs(id) ON DELETE CASCADE,
                FOREIGN KEY(runner_id) REFERENCES runners(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS job_run_dependencies (
                job_run_id TEXT NOT NULL,
                depends_on_job_run_id TEXT NOT NULL,
                PRIMARY KEY(job_run_id, depends_on_job_run_id),
                FOREIGN KEY(job_run_id) REFERENCES job_runs(id) ON DELETE CASCADE,
                FOREIGN KEY(depends_on_job_run_id) REFERENCES job_runs(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS job_run_artifacts (
                id TEXT PRIMARY KEY,
                job_run_id TEXT NOT NULL,
                artifact_name TEXT NOT NULL,
                artifact_role TEXT NOT NULL,
                runner_artifact_id TEXT NOT NULL,
                sha256 TEXT,
                size_bytes INTEGER,
                FOREIGN KEY(job_run_id) REFERENCES job_runs(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS job_run_logs (
                job_run_id TEXT PRIMARY KEY,
                stdout TEXT NOT NULL,
                stderr TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY(job_run_id) REFERENCES job_runs(id) ON DELETE CASCADE
            );
            "#,
        )?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    pub fn ensure_user(
        &self,
        username: &str,
        password_hash: &str,
        role: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let now = now();
        let conn = self.conn.lock().expect("db mutex poisoned");
        conn.execute(
            "INSERT INTO users (id, username, password_hash, role, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT(username) DO NOTHING",
            params![Uuid::now_v7().to_string(), username, password_hash, role, now],
        )?;
        Ok(())
    }

    pub fn create_user(
        &self,
        username: &str,
        password_hash: &str,
        role: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let conn = self.conn.lock().expect("db mutex poisoned");
        conn.execute(
            "INSERT INTO users (id, username, password_hash, role, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                Uuid::now_v7().to_string(),
                username,
                password_hash,
                role,
                now()
            ],
        )?;
        Ok(())
    }

    pub fn list_users(&self) -> Result<Vec<User>, Box<dyn std::error::Error>> {
        let conn = self.conn.lock().expect("db mutex poisoned");
        let mut stmt = conn.prepare(
            "SELECT id, username, role, created_at FROM users ORDER BY username ASC",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok(User {
                id: row.get(0)?,
                username: row.get(1)?,
                role: row.get(2)?,
                created_at: row.get(3)?,
            })
        })?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    pub fn get_user_credentials(
        &self,
        username: &str,
    ) -> Result<Option<(User, String)>, Box<dyn std::error::Error>> {
        let conn = self.conn.lock().expect("db mutex poisoned");
        Ok(conn
            .query_row(
                "SELECT id, username, role, created_at, password_hash FROM users WHERE username = ?1",
                [username],
                |row| {
                    Ok((
                        User {
                            id: row.get(0)?,
                            username: row.get(1)?,
                            role: row.get(2)?,
                            created_at: row.get(3)?,
                        },
                        row.get(4)?,
                    ))
                },
            )
            .optional()?)
    }

    pub fn create_session(
        &self,
        user_id: &str,
        expires_at: &str,
    ) -> Result<String, Box<dyn std::error::Error>> {
        let session_id = Uuid::new_v4().to_string();
        let conn = self.conn.lock().expect("db mutex poisoned");
        conn.execute(
            "INSERT INTO sessions (id, user_id, expires_at, created_at) VALUES (?1, ?2, ?3, ?4)",
            params![session_id, user_id, expires_at, now()],
        )?;
        Ok(session_id)
    }

    pub fn delete_session(&self, session_id: &str) -> Result<(), Box<dyn std::error::Error>> {
        let conn = self.conn.lock().expect("db mutex poisoned");
        conn.execute("DELETE FROM sessions WHERE id = ?1", [session_id])?;
        Ok(())
    }

    pub fn user_for_session(
        &self,
        session_id: &str,
    ) -> Result<Option<User>, Box<dyn std::error::Error>> {
        let conn = self.conn.lock().expect("db mutex poisoned");
        Ok(conn
            .query_row(
                "SELECT u.id, u.username, u.role, u.created_at
                 FROM sessions s
                 JOIN users u ON u.id = s.user_id
                 WHERE s.id = ?1 AND s.expires_at > ?2",
                params![session_id, now()],
                |row| {
                    Ok(User {
                        id: row.get(0)?,
                        username: row.get(1)?,
                        role: row.get(2)?,
                        created_at: row.get(3)?,
                    })
                },
            )
            .optional()?)
    }

    pub fn create_repo(
        &self,
        owner_id: &str,
        name: &str,
        normalized_name: &str,
        bare_path: &str,
        default_branch: &str,
    ) -> Result<String, Box<dyn std::error::Error>> {
        let id = Uuid::now_v7().to_string();
        let conn = self.conn.lock().expect("db mutex poisoned");
        conn.execute(
            "INSERT INTO repos (id, owner_id, name, normalized_name, bare_path, default_branch, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![id, owner_id, name, normalized_name, bare_path, default_branch, now()],
        )?;
        Ok(id)
    }

    pub fn list_repos(&self) -> Result<Vec<Repo>, Box<dyn std::error::Error>> {
        let conn = self.conn.lock().expect("db mutex poisoned");
        let mut stmt = conn.prepare(
            "SELECT r.id, r.owner_id, u.username, r.name, r.normalized_name, r.bare_path, r.default_branch, r.created_at
             FROM repos r
             JOIN users u ON u.id = r.owner_id
             ORDER BY u.username, r.normalized_name",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok(Repo {
                id: row.get(0)?,
                owner_id: row.get(1)?,
                owner_username: row.get(2)?,
                name: row.get(3)?,
                normalized_name: row.get(4)?,
                bare_path: row.get(5)?,
                default_branch: row.get(6)?,
                created_at: row.get(7)?,
            })
        })?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    pub fn get_repo(&self, repo_id: &str) -> Result<Option<Repo>, Box<dyn std::error::Error>> {
        let conn = self.conn.lock().expect("db mutex poisoned");
        Ok(conn.query_row(
            "SELECT r.id, r.owner_id, u.username, r.name, r.normalized_name, r.bare_path, r.default_branch, r.created_at
             FROM repos r JOIN users u ON u.id = r.owner_id WHERE r.id = ?1",
            [repo_id],
            |row| {
                Ok(Repo {
                    id: row.get(0)?,
                    owner_id: row.get(1)?,
                    owner_username: row.get(2)?,
                    name: row.get(3)?,
                    normalized_name: row.get(4)?,
                    bare_path: row.get(5)?,
                    default_branch: row.get(6)?,
                    created_at: row.get(7)?,
                })
            },
        ).optional()?)
    }

    pub fn create_push_event(
        &self,
        repo_id: &str,
        event_key: &str,
        refs: &[PushEventRef],
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut conn = self.conn.lock().expect("db mutex poisoned");
        let tx = conn.transaction()?;
        let push_event_id = Uuid::now_v7().to_string();
        tx.execute(
            "INSERT OR IGNORE INTO push_events (id, repo_id, received_at, event_key) VALUES (?1, ?2, ?3, ?4)",
            params![push_event_id, repo_id, now(), event_key],
        )?;
        let created = tx.changes() > 0;
        if created {
            for item in refs {
                tx.execute(
                    "INSERT INTO push_event_refs (id, push_event_id, old_rev, new_rev, ref_name) VALUES (?1, ?2, ?3, ?4, ?5)",
                    params![Uuid::now_v7().to_string(), push_event_id, item.old_rev, item.new_rev, item.ref_name],
                )?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    pub fn list_unprocessed_push_events(&self) -> Result<Vec<PushEvent>, Box<dyn std::error::Error>> {
        let conn = self.conn.lock().expect("db mutex poisoned");
        let mut stmt = conn.prepare(
            "SELECT id, repo_id, received_at, event_key, processed_at
             FROM push_events WHERE processed_at IS NULL ORDER BY received_at ASC",
        )?;
        let events = stmt
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, Option<String>>(4)?,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        drop(stmt);

        let mut result = Vec::new();
        for (id, repo_id, received_at, event_key, processed_at) in events {
            let mut refs_stmt = conn.prepare(
                "SELECT old_rev, new_rev, ref_name FROM push_event_refs WHERE push_event_id = ?1 ORDER BY ref_name",
            )?;
            let refs = refs_stmt
                .query_map([id.clone()], |row| {
                    Ok(PushEventRef {
                        old_rev: row.get(0)?,
                        new_rev: row.get(1)?,
                        ref_name: row.get(2)?,
                    })
                })?
                .collect::<Result<Vec<_>, _>>()?;
            result.push(PushEvent {
                id,
                repo_id,
                received_at,
                event_key,
                processed_at,
                refs,
            });
        }
        Ok(result)
    }

    pub fn mark_push_event_processed(&self, push_event_id: &str) -> Result<(), Box<dyn std::error::Error>> {
        let conn = self.conn.lock().expect("db mutex poisoned");
        conn.execute(
            "UPDATE push_events SET processed_at = ?2 WHERE id = ?1",
            params![push_event_id, now()],
        )?;
        Ok(())
    }

    pub fn create_runner(
        &self,
        name: &str,
        base_url: &str,
        token: &str,
    ) -> Result<String, Box<dyn std::error::Error>> {
        let id = Uuid::now_v7().to_string();
        let conn = self.conn.lock().expect("db mutex poisoned");
        conn.execute(
            "INSERT INTO runners (id, name, base_url, token_encrypted_or_local_ref, enabled, last_health_state, created_at)
             VALUES (?1, ?2, ?3, ?4, 1, 'unknown', ?5)",
            params![id, name, base_url, token, now()],
        )?;
        Ok(id)
    }

    pub fn list_runners(&self) -> Result<Vec<Runner>, Box<dyn std::error::Error>> {
        let conn = self.conn.lock().expect("db mutex poisoned");
        let mut stmt = conn.prepare(
            "SELECT id, name, base_url, token_encrypted_or_local_ref, enabled, last_health_state, last_seen_at, created_at FROM runners ORDER BY name ASC",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok(Runner {
                id: row.get(0)?,
                name: row.get(1)?,
                base_url: row.get(2)?,
                token: row.get(3)?,
                enabled: row.get::<_, i64>(4)? != 0,
                last_health_state: row.get(5)?,
                last_seen_at: row.get(6)?,
                created_at: row.get(7)?,
            })
        })?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    pub fn get_runner(&self, runner_id: &str) -> Result<Option<Runner>, Box<dyn std::error::Error>> {
        let conn = self.conn.lock().expect("db mutex poisoned");
        Ok(conn.query_row(
            "SELECT id, name, base_url, token_encrypted_or_local_ref, enabled, last_health_state, last_seen_at, created_at FROM runners WHERE id = ?1",
            [runner_id],
            |row| {
                Ok(Runner {
                    id: row.get(0)?,
                    name: row.get(1)?,
                    base_url: row.get(2)?,
                    token: row.get(3)?,
                    enabled: row.get::<_, i64>(4)? != 0,
                    last_health_state: row.get(5)?,
                    last_seen_at: row.get(6)?,
                    created_at: row.get(7)?,
                })
            },
        ).optional()?)
    }

    pub fn set_runner_enabled(&self, runner_id: &str, enabled: bool) -> Result<(), Box<dyn std::error::Error>> {
        let conn = self.conn.lock().expect("db mutex poisoned");
        conn.execute(
            "UPDATE runners SET enabled = ?2 WHERE id = ?1",
            params![runner_id, enabled as i64],
        )?;
        Ok(())
    }

    pub fn update_runner_health(
        &self,
        runner_id: &str,
        state: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let conn = self.conn.lock().expect("db mutex poisoned");
        conn.execute(
            "UPDATE runners SET last_health_state = ?2, last_seen_at = ?3 WHERE id = ?1",
            params![runner_id, state, now()],
        )?;
        Ok(())
    }

    pub fn replace_runner_jobs(
        &self,
        runner_id: &str,
        jobs: &[(String, String)],
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut conn = self.conn.lock().expect("db mutex poisoned");
        let tx = conn.transaction()?;
        tx.execute("DELETE FROM runner_jobs WHERE runner_id = ?1", [runner_id])?;
        for (name, definition_json) in jobs {
            tx.execute(
                "INSERT INTO runner_jobs (id, runner_id, job_name, definition_json, last_refreshed_at)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                params![Uuid::now_v7().to_string(), runner_id, name, definition_json, now()],
            )?;
        }
        tx.commit()?;
        Ok(())
    }

    pub fn list_runner_jobs(
        &self,
        runner_id: &str,
    ) -> Result<Vec<(String, String)>, Box<dyn std::error::Error>> {
        let conn = self.conn.lock().expect("db mutex poisoned");
        let mut stmt = conn.prepare(
            "SELECT job_name, definition_json FROM runner_jobs WHERE runner_id = ?1 ORDER BY job_name",
        )?;
        let rows = stmt.query_map([runner_id], |row| Ok((row.get(0)?, row.get(1)?)))?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    pub fn create_workflow(
        &self,
        repo_id: &str,
        name: &str,
        enabled: bool,
        trigger_json: &str,
        definition_json: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let workflow_id = Uuid::now_v7().to_string();
        let version_id = Uuid::now_v7().to_string();
        let now = now();
        let mut conn = self.conn.lock().expect("db mutex poisoned");
        let tx = conn.transaction()?;
        tx.execute(
            "INSERT INTO workflows (id, repo_id, name, enabled, created_at) VALUES (?1, ?2, ?3, ?4, ?5)",
            params![workflow_id, repo_id, name, enabled as i64, now,],
        )?;
        tx.execute(
            "INSERT INTO workflow_versions (id, workflow_id, version, trigger_json, definition_json, created_at)
             VALUES (?1, ?2, 1, ?3, ?4, ?5)",
            params![version_id, workflow_id, trigger_json, definition_json, now],
        )?;
        tx.commit()?;
        Ok(())
    }

    pub fn list_workflows(&self) -> Result<Vec<Workflow>, Box<dyn std::error::Error>> {
        let conn = self.conn.lock().expect("db mutex poisoned");
        let mut stmt = conn.prepare(
            "SELECT w.id, w.repo_id, w.name, w.enabled, w.created_at, v.version, v.trigger_json, v.definition_json
             FROM workflows w
             JOIN workflow_versions v ON v.workflow_id = w.id
             WHERE v.version = (SELECT MAX(version) FROM workflow_versions WHERE workflow_id = w.id)
             ORDER BY w.created_at DESC",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok(Workflow {
                id: row.get(0)?,
                repo_id: row.get(1)?,
                name: row.get(2)?,
                enabled: row.get::<_, i64>(3)? != 0,
                created_at: row.get(4)?,
                version: row.get(5)?,
                trigger_json: row.get(6)?,
                definition_json: row.get(7)?,
            })
        })?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    pub fn workflows_for_repo(&self, repo_id: &str) -> Result<Vec<Workflow>, Box<dyn std::error::Error>> {
        let conn = self.conn.lock().expect("db mutex poisoned");
        let mut stmt = conn.prepare(
            "SELECT w.id, w.repo_id, w.name, w.enabled, w.created_at, v.version, v.trigger_json, v.definition_json
             FROM workflows w
             JOIN workflow_versions v ON v.workflow_id = w.id
             WHERE w.repo_id = ?1
               AND v.version = (SELECT MAX(version) FROM workflow_versions WHERE workflow_id = w.id)
             ORDER BY w.name ASC",
        )?;
        let rows = stmt.query_map([repo_id], |row| {
            Ok(Workflow {
                id: row.get(0)?,
                repo_id: row.get(1)?,
                name: row.get(2)?,
                enabled: row.get::<_, i64>(3)? != 0,
                created_at: row.get(4)?,
                version: row.get(5)?,
                trigger_json: row.get(6)?,
                definition_json: row.get(7)?,
            })
        })?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    pub fn create_pipeline_run(
        &self,
        repo_id: &str,
        workflow_id: &str,
        workflow_version_id: &str,
        trigger_type: &str,
        trigger_ref: Option<&str>,
        commit_sha: Option<&str>,
    ) -> Result<String, Box<dyn std::error::Error>> {
        let id = Uuid::now_v7().to_string();
        let conn = self.conn.lock().expect("db mutex poisoned");
        conn.execute(
            "INSERT INTO pipeline_runs (id, repo_id, workflow_id, workflow_version_id, trigger_type, trigger_ref, commit_sha, status, started_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 'running', ?8)",
            params![id, repo_id, workflow_id, workflow_version_id, trigger_type, trigger_ref, commit_sha, now()],
        )?;
        Ok(id)
    }

    pub fn latest_workflow_version_id(
        &self,
        workflow_id: &str,
    ) -> Result<Option<String>, Box<dyn std::error::Error>> {
        let conn = self.conn.lock().expect("db mutex poisoned");
        Ok(conn.query_row(
            "SELECT id FROM workflow_versions WHERE workflow_id = ?1 ORDER BY version DESC LIMIT 1",
            [workflow_id],
            |row| row.get(0),
        ).optional()?)
    }

    pub fn create_job_run(
        &self,
        pipeline_run_id: &str,
        job_id: &str,
        job_name: &str,
        runner_id: &str,
        runner_job_name: &str,
        allow_failure: bool,
    ) -> Result<String, Box<dyn std::error::Error>> {
        let id = Uuid::now_v7().to_string();
        let conn = self.conn.lock().expect("db mutex poisoned");
        conn.execute(
            "INSERT INTO job_runs (id, pipeline_run_id, job_id, job_name, runner_id, runner_job_name, status, allow_failure)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'pending', ?7)",
            params![id, pipeline_run_id, job_id, job_name, runner_id, runner_job_name, allow_failure as i64],
        )?;
        conn.execute(
            "INSERT INTO job_run_logs (job_run_id, stdout, stderr, updated_at) VALUES (?1, '', '', ?2)",
            params![id, now()],
        )?;
        Ok(id)
    }

    pub fn add_job_dependency(
        &self,
        job_run_id: &str,
        depends_on_job_run_id: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let conn = self.conn.lock().expect("db mutex poisoned");
        conn.execute(
            "INSERT INTO job_run_dependencies (job_run_id, depends_on_job_run_id) VALUES (?1, ?2)",
            params![job_run_id, depends_on_job_run_id],
        )?;
        Ok(())
    }

    pub fn list_pipeline_runs(&self) -> Result<Vec<PipelineRun>, Box<dyn std::error::Error>> {
        let conn = self.conn.lock().expect("db mutex poisoned");
        let mut stmt = conn.prepare(
            "SELECT id, repo_id, workflow_id, workflow_version_id, trigger_type, trigger_ref, commit_sha, status, started_at, finished_at
             FROM pipeline_runs ORDER BY started_at DESC",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok(PipelineRun {
                id: row.get(0)?,
                repo_id: row.get(1)?,
                workflow_id: row.get(2)?,
                workflow_version_id: row.get(3)?,
                trigger_type: row.get(4)?,
                trigger_ref: row.get(5)?,
                commit_sha: row.get(6)?,
                status: row.get(7)?,
                started_at: row.get(8)?,
                finished_at: row.get(9)?,
            })
        })?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    pub fn pipeline_snapshot(
        &self,
        pipeline_id: &str,
    ) -> Result<Option<PipelineSnapshot>, Box<dyn std::error::Error>> {
        let conn = self.conn.lock().expect("db mutex poisoned");
        let pipeline = conn
            .query_row(
                "SELECT id, repo_id, workflow_id, workflow_version_id, trigger_type, trigger_ref, commit_sha, status, started_at, finished_at
                 FROM pipeline_runs WHERE id = ?1",
                [pipeline_id],
                |row| {
                    Ok(PipelineRun {
                        id: row.get(0)?,
                        repo_id: row.get(1)?,
                        workflow_id: row.get(2)?,
                        workflow_version_id: row.get(3)?,
                        trigger_type: row.get(4)?,
                        trigger_ref: row.get(5)?,
                        commit_sha: row.get(6)?,
                        status: row.get(7)?,
                        started_at: row.get(8)?,
                        finished_at: row.get(9)?,
                    })
                },
            )
            .optional()?;
        let Some(pipeline) = pipeline else {
            return Ok(None);
        };
        let mut stmt = conn.prepare(
            "SELECT id, pipeline_run_id, job_id, job_name, runner_id, runner_job_name, runner_run_id, status, allow_failure, started_at, finished_at
             FROM job_runs WHERE pipeline_run_id = ?1 ORDER BY job_name",
        )?;
        let job_rows = stmt
            .query_map([pipeline_id], |row| {
                Ok(JobRun {
                    id: row.get(0)?,
                    pipeline_run_id: row.get(1)?,
                    job_id: row.get(2)?,
                    job_name: row.get(3)?,
                    runner_id: row.get(4)?,
                    runner_job_name: row.get(5)?,
                    runner_run_id: row.get(6)?,
                    status: row.get(7)?,
                    allow_failure: row.get::<_, i64>(8)? != 0,
                    started_at: row.get(9)?,
                    finished_at: row.get(10)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;
        drop(stmt);
        let mut jobs = Vec::new();
        for run in job_rows {
            let (stdout, stderr): (String, String) = conn.query_row(
                "SELECT stdout, stderr FROM job_run_logs WHERE job_run_id = ?1",
                [run.id.clone()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )?;
            let mut dep_stmt = conn.prepare(
                "SELECT depends_on_job_run_id FROM job_run_dependencies WHERE job_run_id = ?1",
            )?;
            let dependencies = dep_stmt
                .query_map([run.id.clone()], |row| row.get(0))?
                .collect::<Result<Vec<String>, _>>()?;
            let mut artifact_stmt = conn.prepare(
                "SELECT artifact_name, artifact_role, runner_artifact_id, sha256, size_bytes FROM job_run_artifacts WHERE job_run_id = ?1 ORDER BY artifact_name",
            )?;
            let outputs = artifact_stmt
                .query_map([run.id.clone()], |row| {
                    Ok(JobRunArtifact {
                        artifact_name: row.get(0)?,
                        artifact_role: row.get(1)?,
                        runner_artifact_id: row.get(2)?,
                        sha256: row.get(3)?,
                        size_bytes: row.get(4)?,
                    })
                })?
                .collect::<Result<Vec<_>, _>>()?;
            jobs.push(JobRunDetail {
                run,
                stdout,
                stderr,
                outputs,
                dependencies,
            });
        }
        Ok(Some(PipelineSnapshot { pipeline, jobs }))
    }

    pub fn list_job_runs_by_status(
        &self,
        statuses: &[&str],
    ) -> Result<Vec<JobRun>, Box<dyn std::error::Error>> {
        let conn = self.conn.lock().expect("db mutex poisoned");
        let mut query = String::from(
            "SELECT id, pipeline_run_id, job_id, job_name, runner_id, runner_job_name, runner_run_id, status, allow_failure, started_at, finished_at FROM job_runs WHERE status IN (",
        );
        for idx in 0..statuses.len() {
            if idx > 0 {
                query.push(',');
            }
            query.push('?');
            query.push_str(&(idx + 1).to_string());
        }
        query.push(')');
        let mut stmt = conn.prepare(&query)?;
        let rows = stmt.query_map(rusqlite::params_from_iter(statuses.iter().copied()), |row| {
            Ok(JobRun {
                id: row.get(0)?,
                pipeline_run_id: row.get(1)?,
                job_id: row.get(2)?,
                job_name: row.get(3)?,
                runner_id: row.get(4)?,
                runner_job_name: row.get(5)?,
                runner_run_id: row.get(6)?,
                status: row.get(7)?,
                allow_failure: row.get::<_, i64>(8)? != 0,
                started_at: row.get(9)?,
                finished_at: row.get(10)?,
            })
        })?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    pub fn dependencies_for_job_run(
        &self,
        job_run_id: &str,
    ) -> Result<Vec<JobRun>, Box<dyn std::error::Error>> {
        let conn = self.conn.lock().expect("db mutex poisoned");
        let mut stmt = conn.prepare(
            "SELECT jr.id, jr.pipeline_run_id, jr.job_id, jr.job_name, jr.runner_id, jr.runner_job_name, jr.runner_run_id, jr.status, jr.allow_failure, jr.started_at, jr.finished_at
             FROM job_run_dependencies d
             JOIN job_runs jr ON jr.id = d.depends_on_job_run_id
             WHERE d.job_run_id = ?1",
        )?;
        let rows = stmt.query_map([job_run_id], |row| {
            Ok(JobRun {
                id: row.get(0)?,
                pipeline_run_id: row.get(1)?,
                job_id: row.get(2)?,
                job_name: row.get(3)?,
                runner_id: row.get(4)?,
                runner_job_name: row.get(5)?,
                runner_run_id: row.get(6)?,
                status: row.get(7)?,
                allow_failure: row.get::<_, i64>(8)? != 0,
                started_at: row.get(9)?,
                finished_at: row.get(10)?,
            })
        })?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    pub fn set_job_run_started(
        &self,
        job_run_id: &str,
        runner_run_id: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let conn = self.conn.lock().expect("db mutex poisoned");
        conn.execute(
            "UPDATE job_runs SET status = 'running', runner_run_id = ?2, started_at = ?3 WHERE id = ?1",
            params![job_run_id, runner_run_id, now()],
        )?;
        Ok(())
    }

    pub fn finish_job_run(
        &self,
        job_run_id: &str,
        status: &str,
        stdout: &str,
        stderr: &str,
        outputs: &[(String, String, String, u64)],
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut conn = self.conn.lock().expect("db mutex poisoned");
        let tx = conn.transaction()?;
        tx.execute(
            "UPDATE job_runs SET status = ?2, finished_at = ?3 WHERE id = ?1",
            params![job_run_id, status, now()],
        )?;
        tx.execute(
            "UPDATE job_run_logs SET stdout = ?2, stderr = ?3, updated_at = ?4 WHERE job_run_id = ?1",
            params![job_run_id, stdout, stderr, now()],
        )?;
        tx.execute("DELETE FROM job_run_artifacts WHERE job_run_id = ?1", [job_run_id])?;
        for (name, artifact_id, sha256, size) in outputs {
            tx.execute(
                "INSERT INTO job_run_artifacts (id, job_run_id, artifact_name, artifact_role, runner_artifact_id, sha256, size_bytes)
                 VALUES (?1, ?2, ?3, 'output', ?4, ?5, ?6)",
                params![Uuid::now_v7().to_string(), job_run_id, name, artifact_id, sha256, *size as i64],
            )?;
        }
        tx.commit()?;
        Ok(())
    }

    pub fn set_job_run_status(&self, job_run_id: &str, status: &str) -> Result<(), Box<dyn std::error::Error>> {
        let conn = self.conn.lock().expect("db mutex poisoned");
        conn.execute(
            "UPDATE job_runs SET status = ?2, finished_at = CASE WHEN ?2 IN ('success', 'failed', 'canceled', 'blocked', 'skipped') THEN ?3 ELSE finished_at END WHERE id = ?1",
            params![job_run_id, status, now()],
        )?;
        Ok(())
    }

    pub fn job_outputs(
        &self,
        job_run_id: &str,
    ) -> Result<Vec<JobRunArtifact>, Box<dyn std::error::Error>> {
        let conn = self.conn.lock().expect("db mutex poisoned");
        let mut stmt = conn.prepare(
            "SELECT artifact_name, artifact_role, runner_artifact_id, sha256, size_bytes FROM job_run_artifacts WHERE job_run_id = ?1 ORDER BY artifact_name",
        )?;
        let rows = stmt.query_map([job_run_id], |row| {
            Ok(JobRunArtifact {
                artifact_name: row.get(0)?,
                artifact_role: row.get(1)?,
                runner_artifact_id: row.get(2)?,
                sha256: row.get(3)?,
                size_bytes: row.get(4)?,
            })
        })?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    pub fn pipeline_for_job_run(
        &self,
        job_run_id: &str,
    ) -> Result<Option<PipelineRun>, Box<dyn std::error::Error>> {
        let conn = self.conn.lock().expect("db mutex poisoned");
        Ok(conn.query_row(
            "SELECT p.id, p.repo_id, p.workflow_id, p.workflow_version_id, p.trigger_type, p.trigger_ref, p.commit_sha, p.status, p.started_at, p.finished_at
             FROM job_runs j JOIN pipeline_runs p ON p.id = j.pipeline_run_id WHERE j.id = ?1",
            [job_run_id],
            |row| {
                Ok(PipelineRun {
                    id: row.get(0)?,
                    repo_id: row.get(1)?,
                    workflow_id: row.get(2)?,
                    workflow_version_id: row.get(3)?,
                    trigger_type: row.get(4)?,
                    trigger_ref: row.get(5)?,
                    commit_sha: row.get(6)?,
                    status: row.get(7)?,
                    started_at: row.get(8)?,
                    finished_at: row.get(9)?,
                })
            },
        ).optional()?)
    }

    pub fn workflow_definition_json(
        &self,
        workflow_version_id: &str,
    ) -> Result<String, Box<dyn std::error::Error>> {
        let conn = self.conn.lock().expect("db mutex poisoned");
        Ok(conn.query_row(
            "SELECT definition_json FROM workflow_versions WHERE id = ?1",
            [workflow_version_id],
            |row| row.get(0),
        )?)
    }

    pub fn finalize_pipeline_status(
        &self,
        pipeline_id: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let conn = self.conn.lock().expect("db mutex poisoned");
        let mut stmt = conn.prepare("SELECT status, allow_failure FROM job_runs WHERE pipeline_run_id = ?1")?;
        let rows = stmt
            .query_map([pipeline_id], |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)? != 0)))?
            .collect::<Result<Vec<_>, _>>()?;
        let mut pipeline_status = "running";
        if !rows.is_empty() && rows.iter().all(|(status, _)| matches!(status.as_str(), "success" | "skipped")) {
            pipeline_status = "success";
        } else if rows.iter().any(|(status, allow_failure)| status == "failed" && !allow_failure) {
            pipeline_status = "failed";
        } else if rows.iter().any(|(status, _)| status == "blocked") {
            pipeline_status = "blocked";
        }

        if pipeline_status != "running" {
            conn.execute(
                "UPDATE pipeline_runs SET status = ?2, finished_at = ?3 WHERE id = ?1",
                params![pipeline_id, pipeline_status, now()],
            )?;
        }
        Ok(())
    }
}

fn now() -> String {
    Utc::now().to_rfc3339()
}
