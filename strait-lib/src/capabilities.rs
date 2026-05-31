use serde::{Deserialize, Serialize};

pub const RUNNER_PROTOCOL_VERSION: u32 = 1;
pub const SUPPORTED_RUNNER_PROTOCOL_VERSIONS: &[u32] = &[RUNNER_PROTOCOL_VERSION];

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RunnerCapabilitiesResponse {
    pub protocol_version: u32,
    pub supported_protocol_versions: Vec<u32>,
}

impl RunnerCapabilitiesResponse {
    pub fn current() -> Self {
        Self {
            protocol_version: RUNNER_PROTOCOL_VERSION,
            supported_protocol_versions: SUPPORTED_RUNNER_PROTOCOL_VERSIONS.to_vec(),
        }
    }

    pub fn is_compatible_with_supported_versions(&self, supported_versions: &[u32]) -> bool {
        self.supported_protocol_versions
            .iter()
            .any(|version| supported_versions.contains(version))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn current_capabilities_advertise_supported_protocol_version() {
        let capabilities = RunnerCapabilitiesResponse::current();

        assert_eq!(capabilities.protocol_version, RUNNER_PROTOCOL_VERSION);
        assert!(
            capabilities.is_compatible_with_supported_versions(SUPPORTED_RUNNER_PROTOCOL_VERSIONS)
        );
        assert!(!capabilities.is_compatible_with_supported_versions(&[999]));
    }
}
