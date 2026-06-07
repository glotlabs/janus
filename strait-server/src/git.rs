use std::{
    fs,
    io::{self, ErrorKind, Read},
    path::{Path, PathBuf},
    process::Command,
};

use sha2::{Digest, Sha256};

use crate::models::PushEventRef;

pub fn validate_repo_name(name: &str) -> Result<String, String> {
    let trimmed = name.trim();
    if trimmed.is_empty() || trimmed == "." || trimmed == ".." {
        return Err("repo name cannot be empty, . or ..".to_string());
    }
    if !trimmed
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '.' | '_' | '-'))
    {
        return Err("repo name must match ^[A-Za-z0-9._-]+$".to_string());
    }
    Ok(trimmed.to_lowercase())
}

pub fn init_bare_repo(path: &Path) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|error| error.to_string())?;
    }
    let status = Command::new("git")
        .args(["init", "--bare", "--shared=group", "--initial-branch=main"])
        .arg(path)
        .status()
        .map_err(|error| git_spawn_error(error))?;
    if !status.success() {
        return Err(format!("git init --bare failed for {}", path.display()));
    }
    Ok(())
}

fn git_spawn_error(error: io::Error) -> String {
    if error.kind() == ErrorKind::NotFound {
        return "failed to execute git: not found in PATH".to_string();
    }
    format!("failed to execute git: {error}")
}

pub fn install_post_receive_hook(
    bare_path: &Path,
    server_bin: &Path,
    socket_path: &Path,
    repo_id: &str,
) -> Result<(), String> {
    let hooks_dir = bare_path.join("hooks");
    fs::create_dir_all(&hooks_dir).map_err(|error| error.to_string())?;
    let hook_path = hooks_dir.join("post-receive");
    let script = format!(
        "#!/bin/sh\nset -eu\nexec \"{}\" git post-receive --socket-path \"{}\" --repo-id \"{}\"\n",
        server_bin.display(),
        socket_path.display(),
        repo_id
    );
    fs::write(&hook_path, script).map_err(|error| error.to_string())?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut permissions = fs::metadata(&hook_path)
            .map_err(|error| error.to_string())?
            .permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(&hook_path, permissions).map_err(|error| error.to_string())?;
    }
    Ok(())
}

pub fn read_push_refs(stdin: &mut dyn Read) -> Result<Vec<PushEventRef>, io::Error> {
    let mut raw = String::new();
    stdin.read_to_string(&mut raw)?;
    let refs = raw
        .lines()
        .filter(|line| !line.trim().is_empty())
        .filter_map(|line| {
            let mut parts = line.split_whitespace();
            Some(PushEventRef {
                old_rev: parts.next()?.to_string(),
                new_rev: parts.next()?.to_string(),
                ref_name: parts.next()?.to_string(),
            })
        })
        .collect();
    Ok(refs)
}

pub fn event_key(repo_id: &str, refs: &[PushEventRef]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(repo_id.as_bytes());
    for item in refs {
        hasher.update(item.old_rev.as_bytes());
        hasher.update(item.new_rev.as_bytes());
        hasher.update(item.ref_name.as_bytes());
    }
    format!("{:x}", hasher.finalize())
}

pub fn create_source_archive(
    bare_path: &Path,
    commit_sha: &str,
    target_path: &Path,
) -> Result<PathBuf, String> {
    if let Some(parent) = target_path.parent() {
        fs::create_dir_all(parent).map_err(|error| error.to_string())?;
    }
    let status = Command::new("git")
        .arg(format!("--git-dir={}", bare_path.display()))
        .args(["archive", "--format=tar.gz", "-o"])
        .arg(target_path)
        .arg(commit_sha)
        .status()
        .map_err(|error| git_spawn_error(error))?;
    if !status.success() {
        return Err(format!(
            "git archive failed for commit {commit_sha} in {}",
            bare_path.display()
        ));
    }
    Ok(target_path.to_path_buf())
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        time::{SystemTime, UNIX_EPOCH},
    };

    use super::{init_bare_repo, validate_repo_name};

    #[test]
    fn repo_name_is_normalized() {
        assert_eq!(validate_repo_name("My.Repo").expect("valid"), "my.repo");
        assert!(validate_repo_name("../bad").is_err());
    }

    #[test]
    fn bare_repo_uses_main_as_initial_branch() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let path = std::env::temp_dir().join(format!("strait-server-git-test-{unique}.git"));

        init_bare_repo(&path).expect("init bare repo");

        let head = fs::read_to_string(path.join("HEAD")).expect("read HEAD");
        assert_eq!(head, "ref: refs/heads/main\n");

        fs::remove_dir_all(path).expect("remove temp repo");
    }
}
