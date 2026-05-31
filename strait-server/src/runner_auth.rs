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

use crate::config::RunnerAuthConfig;

#[derive(Clone, Debug)]
pub(crate) struct RunnerSigner {
    key_id: String,
    signing_key: SigningKey,
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
