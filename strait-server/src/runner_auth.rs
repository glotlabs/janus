use std::{
    fs,
    path::{Path, PathBuf},
};

use base64::{Engine as _, engine::general_purpose::STANDARD};
use ed25519_dalek::{Signer, SigningKey, VerifyingKey};
use rand::{RngCore, rngs::OsRng};
use strait_lib::{
    HEADER_SIGNATURE, HEADER_SIGNATURE_CONTENT_SHA256, HEADER_SIGNATURE_KEY_ID,
    HEADER_SIGNATURE_NONCE, HEADER_SIGNATURE_TIMESTAMP, SIGNATURE_ALGORITHM_ED25519,
    canonical_signed_request, sha256_hex,
};
use uuid::Uuid;

use crate::config::{Config, RunnerAuthConfig};

#[derive(Clone, Debug)]
pub(crate) struct RunnerSigner {
    key_id: String,
    signing_key: SigningKey,
}

#[derive(Clone, Copy)]
pub(crate) enum RunnerKeyShowFormat {
    Text,
    Toml,
}

pub(crate) fn show_runner_key(
    config_path: &Path,
    format: RunnerKeyShowFormat,
) -> Result<(), Box<dyn std::error::Error>> {
    let config = Config::load_from_path(config_path)?;
    let signer = RunnerSigner::load_or_generate(&config.runner_auth)?;

    if matches!(format, RunnerKeyShowFormat::Toml) {
        print_runner_trust_toml(&signer);
        return Ok(());
    }

    println!("active runner signing key");
    println!("key_id = {}", signer.key_id());
    println!("public_key = {}", signer.public_key_base64());
    println!();
    println!("runner config snippet");
    print_runner_trust_toml(&signer);

    if let Some(public_key_path) = &config.runner_auth.public_key_path {
        print_public_key_files(Path::new(public_key_path))?;
    }

    Ok(())
}

fn print_runner_trust_toml(signer: &RunnerSigner) {
    println!("[[auth.servers]]");
    println!("name = \"{}\"", signer.key_id());
    println!("key_id = \"{}\"", signer.key_id());
    println!("public_key = \"{}\"", signer.public_key_base64());
    println!(
        "permissions = [\"artifacts:write\", \"artifacts:read\", \"jobs:run\", \"jobs:read\", \"logs:read\"]"
    );
}

pub(crate) fn init_runner_key(config_path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let raw = fs::read_to_string(config_path)?;
    let mut document = raw.parse::<toml_edit::DocumentMut>()?;
    if document.get("runner_auth").is_some() {
        return Err("[runner_auth] already exists".into());
    }

    let data_dir = document["data_dir"]
        .as_str()
        .ok_or("missing data_dir")?
        .to_string();
    let key_id = generate_key_id();
    let keys_dir = Path::new(&data_dir).join("keys");
    let private_key_path = keys_dir.join(format!("{key_id}.key"));
    let public_key_path = keys_dir.join(format!("{key_id}.pub"));
    let signing_key = generate_signing_key();

    write_key_file(&private_key_path, &STANDARD.encode(signing_key.to_bytes()))?;
    write_public_key(
        public_key_path
            .to_str()
            .ok_or("public key path is not valid UTF-8")?,
        &signing_key.verifying_key(),
    )?;

    let mut runner_auth = toml_edit::Table::new();
    runner_auth["key_id"] = toml_edit::value(key_id.as_str());
    runner_auth["private_key_path"] = toml_edit::value(private_key_path.display().to_string());
    runner_auth["public_key_path"] = toml_edit::value(public_key_path.display().to_string());
    document["runner_auth"] = toml_edit::Item::Table(runner_auth);
    fs::write(config_path, document.to_string())?;

    println!("initialized runner signing key");
    println!("key_id = {key_id}");
    println!("private_key_path = {}", private_key_path.display());
    println!("public_key_path = {}", public_key_path.display());
    println!(
        "public_key = {}",
        STANDARD.encode(signing_key.verifying_key().to_bytes())
    );

    Ok(())
}

pub(crate) fn rotate_runner_key(config_path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let new_key_id = generate_key_id();
    validate_key_id(&new_key_id)?;
    let raw = fs::read_to_string(config_path)?;
    let mut document = raw.parse::<toml_edit::DocumentMut>()?;
    let config = Config::load_from_path(config_path)?;
    let _old_signer = RunnerSigner::load_or_generate(&config.runner_auth)?;

    let old_private_path = PathBuf::from(&config.runner_auth.private_key_path);
    let private_dir = old_private_path
        .parent()
        .ok_or("runner_auth.private_key_path must have a parent directory")?;
    let public_dir = config
        .runner_auth
        .public_key_path
        .as_deref()
        .and_then(|path| Path::new(path).parent())
        .unwrap_or(private_dir);
    let new_private_path = private_dir.join(format!("{new_key_id}.key"));
    let new_public_path = public_dir.join(format!("{new_key_id}.pub"));

    if new_private_path.exists() {
        return Err(format!("private key already exists: {}", new_private_path.display()).into());
    }
    if new_public_path.exists() {
        return Err(format!("public key already exists: {}", new_public_path.display()).into());
    }

    let signing_key = generate_signing_key();
    write_key_file(&new_private_path, &STANDARD.encode(signing_key.to_bytes()))?;
    write_public_key(
        new_public_path
            .to_str()
            .ok_or("new public key path is not valid UTF-8")?,
        &signing_key.verifying_key(),
    )?;

    let runner_auth = document["runner_auth"]
        .as_table_mut()
        .ok_or("missing [runner_auth] table")?;
    runner_auth["key_id"] = toml_edit::value(new_key_id.as_str());
    runner_auth["private_key_path"] = toml_edit::value(new_private_path.display().to_string());
    runner_auth["public_key_path"] = toml_edit::value(new_public_path.display().to_string());
    fs::write(config_path, document.to_string())?;

    println!("rotated runner signing key");
    println!("old_key_id = {}", config.runner_auth.key_id);
    println!("new_key_id = {new_key_id}");
    println!("private_key_path = {}", new_private_path.display());
    println!("public_key_path = {}", new_public_path.display());
    println!(
        "public_key = {}",
        STANDARD.encode(signing_key.verifying_key().to_bytes())
    );
    println!();
    println!(
        "Keep the old public key in runner configs until all in-flight requests signed with the old key have drained."
    );

    Ok(())
}

pub(crate) struct SignedRequestHeaders {
    pub(crate) key_id: String,
    pub(crate) timestamp: String,
    pub(crate) nonce: String,
    pub(crate) content_sha256: String,
    pub(crate) signature: String,
}

impl RunnerSigner {
    pub(crate) fn load_or_generate(
        config: &RunnerAuthConfig,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let private_key_path = PathBuf::from(&config.private_key_path);
        validate_key_id(&config.key_id)?;
        let signing_key = if private_key_path.exists() {
            read_signing_key(&private_key_path)?
        } else {
            let signing_key = generate_signing_key();
            write_key_file(&private_key_path, &STANDARD.encode(signing_key.to_bytes()))?;
            signing_key
        };

        if let Some(public_key_path) = &config.public_key_path {
            write_public_key(public_key_path, &signing_key.verifying_key())?;
        }

        Ok(Self {
            key_id: config.key_id.clone(),
            signing_key,
        })
    }

    pub(crate) fn public_key_base64(&self) -> String {
        STANDARD.encode(self.signing_key.verifying_key().to_bytes())
    }

    pub(crate) fn key_id(&self) -> &str {
        &self.key_id
    }

    pub(crate) fn sign(
        &self,
        method: &str,
        path_and_query: &str,
        body: &[u8],
    ) -> SignedRequestHeaders {
        let timestamp = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
        let nonce = Uuid::now_v7().to_string();
        let content_sha256 = sha256_hex(body);
        let canonical =
            canonical_signed_request(method, path_and_query, &content_sha256, &timestamp, &nonce);
        let signature = self.signing_key.sign(canonical.as_bytes());

        SignedRequestHeaders {
            key_id: self.key_id.clone(),
            timestamp,
            nonce,
            content_sha256,
            signature: format!(
                "{SIGNATURE_ALGORITHM_ED25519}:{}",
                STANDARD.encode(signature.to_bytes())
            ),
        }
    }
}

impl SignedRequestHeaders {
    pub(crate) fn apply(self, request: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        request
            .header(HEADER_SIGNATURE_KEY_ID, self.key_id)
            .header(HEADER_SIGNATURE_TIMESTAMP, self.timestamp)
            .header(HEADER_SIGNATURE_NONCE, self.nonce)
            .header(HEADER_SIGNATURE_CONTENT_SHA256, self.content_sha256)
            .header(HEADER_SIGNATURE, self.signature)
    }
}

fn generate_signing_key() -> SigningKey {
    let mut secret = [0_u8; 32];
    OsRng.fill_bytes(&mut secret);
    SigningKey::from_bytes(&secret)
}

fn generate_key_id() -> String {
    let now = chrono::Utc::now();
    let mut random = [0_u8; 4];
    OsRng.fill_bytes(&mut random);
    format!("strait-{}-{}", now.format("%Y%m"), hex::encode(random))
}

fn validate_key_id(key_id: &str) -> Result<(), Box<dyn std::error::Error>> {
    if key_id.is_empty() {
        return Err("key id cannot be empty".into());
    }
    if key_id.len() > 120 {
        return Err("key id must be 120 characters or fewer".into());
    }
    if !key_id
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.'))
    {
        return Err("key id may only contain ASCII letters, numbers, '-', '_', and '.'".into());
    }
    Ok(())
}

fn print_public_key_files(active_public_key_path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let Some(public_dir) = active_public_key_path.parent() else {
        return Ok(());
    };
    if !public_dir.exists() {
        return Ok(());
    }

    let mut public_keys = fs::read_dir(public_dir)?
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| path.extension().and_then(|value| value.to_str()) == Some("pub"))
        .collect::<Vec<_>>();
    public_keys.sort();
    if public_keys.is_empty() {
        return Ok(());
    }

    println!();
    println!("public key files");
    for path in public_keys {
        let key = fs::read_to_string(&path)?;
        println!("{} = {}", path.display(), key.trim());
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rotation_updates_config_and_keeps_existing_public_key_file() {
        let temp = temp_dir("runner-key-rotate");
        let config_path = temp.join("server.toml");
        let keys_dir = temp.join("keys");
        fs::create_dir_all(&keys_dir).expect("keys dir");
        write_config(&config_path, &temp, &keys_dir, "old-key");

        let config = Config::load_from_path(&config_path).expect("config should load");
        let old_signer =
            RunnerSigner::load_or_generate(&config.runner_auth).expect("old key should generate");
        let old_public_key_path = keys_dir.join("old-key.pub");
        fs::write(&old_public_key_path, old_signer.public_key_base64()).expect("old public key");

        rotate_runner_key(&config_path).expect("rotation should succeed");

        let rotated = Config::load_from_path(&config_path).expect("rotated config should load");
        assert!(rotated.runner_auth.key_id.starts_with("strait-"));
        assert!(
            rotated
                .runner_auth
                .private_key_path
                .ends_with(&format!("{}.key", rotated.runner_auth.key_id))
        );
        assert_eq!(
            rotated.runner_auth.public_key_path.as_deref(),
            Some(
                keys_dir
                    .join(format!("{}.pub", rotated.runner_auth.key_id))
                    .to_str()
                    .expect("path utf8")
            )
        );
        assert!(Path::new(&rotated.runner_auth.private_key_path).exists());
        assert!(Path::new(rotated.runner_auth.public_key_path.as_ref().unwrap()).exists());
        assert!(old_public_key_path.exists());
    }

    #[test]
    fn init_adds_generated_runner_auth_to_minimal_config() {
        let temp = temp_dir("runner-key-init");
        let config_path = temp.join("server.toml");
        write_minimal_config(&config_path, &temp);

        init_runner_key(&config_path).expect("init should succeed");

        let config = Config::load_from_path(&config_path).expect("config should load");
        assert!(config.runner_auth.key_id.starts_with("strait-"));
        assert!(Path::new(&config.runner_auth.private_key_path).exists());
        assert!(Path::new(config.runner_auth.public_key_path.as_ref().unwrap()).exists());
    }

    #[test]
    fn init_rejects_existing_runner_auth() {
        let temp = temp_dir("runner-key-init-existing");
        let config_path = temp.join("server.toml");
        write_config(&config_path, &temp, &temp.join("keys"), "old-key");

        let error = init_runner_key(&config_path).expect_err("existing runner_auth should fail");

        assert!(error.to_string().contains("already exists"));
    }

    #[test]
    fn missing_runner_auth_reports_init_command() {
        let temp = temp_dir("runner-key-missing");
        let config_path = temp.join("server.toml");
        write_minimal_config(&config_path, &temp);

        let error = Config::load_from_path(&config_path).expect_err("config should fail");

        assert!(error.to_string().contains("admin runner-key init"));
    }

    fn write_config(config_path: &Path, temp: &Path, keys_dir: &Path, key_id: &str) {
        fs::write(
            config_path,
            format!(
                r#"data_dir = "{}"
repos_dir = "{}"

[database]
path = "{}"

[server]
listen = "127.0.0.1:0"
public_base_url = "ci.test"

[auth]
session_secret = "test-secret"
session_ttl_days = 1
session_cookie_secure = false
login_rate_limit_per_minute = 100

[runner_auth]
key_id = "{key_id}"
private_key_path = "{}"
public_key_path = "{}"

[scheduler]
poll_interval_ms = 50
cancel_stuck_timeout_seconds = 1
max_cancel_retries = 2
max_infra_retries = 2

[runners]
healthcheck_interval_seconds = 60
connect_timeout_seconds = 5
request_timeout_seconds = 120

[runner_url_policy]
require_https = true
allow_credentials = false
allow_query = false
allow_fragment = false
allow_path = false
allow_localhost = false
allow_private_ips = false
allow_link_local_ips = false
allow_documentation_ips = false
allow_multicast_ips = false

[limits]
request_body_bytes = 1048576
runner_json_bytes = 4194304
runner_logs_bytes = 8388608
runner_artifact_bytes = 268435456
runner_error_bytes = 16384
server_artifact_bytes = 268435456
"#,
                temp.join("data").display(),
                temp.join("repos").display(),
                temp.join("server.sqlite3").display(),
                keys_dir.join(format!("{key_id}.key")).display(),
                keys_dir.join(format!("{key_id}.pub")).display(),
            ),
        )
        .expect("config should write");
    }

    fn write_minimal_config(config_path: &Path, temp: &Path) {
        fs::write(
            config_path,
            format!(
                r#"data_dir = "{}"
repos_dir = "{}"

[database]
path = "{}"

[server]
listen = "127.0.0.1:0"
public_base_url = "ci.test"

[auth]
session_secret = "test-secret"
session_ttl_days = 1
session_cookie_secure = false
login_rate_limit_per_minute = 100

[scheduler]
poll_interval_ms = 50
cancel_stuck_timeout_seconds = 1
max_cancel_retries = 2
max_infra_retries = 2

[runners]
healthcheck_interval_seconds = 60
connect_timeout_seconds = 5
request_timeout_seconds = 120

[runner_url_policy]
require_https = true
allow_credentials = false
allow_query = false
allow_fragment = false
allow_path = false
allow_localhost = false
allow_private_ips = false
allow_link_local_ips = false
allow_documentation_ips = false
allow_multicast_ips = false

[limits]
request_body_bytes = 1048576
runner_json_bytes = 4194304
runner_logs_bytes = 8388608
runner_artifact_bytes = 268435456
runner_error_bytes = 16384
server_artifact_bytes = 268435456
"#,
                temp.join("data").display(),
                temp.join("repos").display(),
                temp.join("server.sqlite3").display(),
            ),
        )
        .expect("config should write");
    }

    fn temp_dir(label: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("strait-server-{label}-{}", Uuid::now_v7()));
        fs::create_dir_all(&dir).expect("temp dir");
        dir
    }
}

fn read_signing_key(path: &Path) -> Result<SigningKey, Box<dyn std::error::Error>> {
    let raw = fs::read_to_string(path)?;
    let decoded = STANDARD.decode(raw.trim())?;
    let bytes: [u8; 32] = decoded
        .try_into()
        .map_err(|_| format!("private key at {} must decode to 32 bytes", path.display()))?;
    Ok(SigningKey::from_bytes(&bytes))
}

fn write_public_key(
    path: &str,
    verifying_key: &VerifyingKey,
) -> Result<(), Box<dyn std::error::Error>> {
    write_key_file(Path::new(path), &STANDARD.encode(verifying_key.to_bytes()))
}

fn write_key_file(path: &Path, value: &str) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(path, format!("{value}\n"))?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
    }

    Ok(())
}
