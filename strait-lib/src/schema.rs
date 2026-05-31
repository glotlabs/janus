use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct JobDefinitionResponse {
    pub name: String,
    #[serde(default)]
    pub concurrency: Concurrency,
    #[serde(default)]
    pub timeout_seconds: u64,
    #[serde(default)]
    pub inputs: BTreeMap<String, JobInputDefinitionResponse>,
    #[serde(default)]
    pub outputs: BTreeMap<String, JobOutputDefinitionResponse>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct JobInputDefinitionResponse {
    #[serde(rename = "type")]
    pub kind: InputType,
    pub required: bool,
    #[serde(default, skip_serializing_if = "is_false")]
    pub sensitive: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_length: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pattern: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_json_bytes: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct JobOutputDefinitionResponse {
    #[serde(rename = "type")]
    pub kind: OutputType,
    #[serde(default)]
    pub path: String,
    pub required: bool,
}

impl Default for JobDefinitionResponse {
    fn default() -> Self {
        Self {
            name: String::new(),
            concurrency: Concurrency::Parallel,
            timeout_seconds: 0,
            inputs: BTreeMap::new(),
            outputs: BTreeMap::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum InputType {
    String,
    Integer,
    Boolean,
    Artifact,
    Json,
}

impl InputType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::String => "string",
            Self::Integer => "integer",
            Self::Boolean => "boolean",
            Self::Artifact => "artifact",
            Self::Json => "json",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "string" => Some(Self::String),
            "integer" => Some(Self::Integer),
            "boolean" => Some(Self::Boolean),
            "artifact" => Some(Self::Artifact),
            "json" => Some(Self::Json),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum OutputType {
    Artifact,
    String,
    Integer,
    Boolean,
    Json,
}

impl OutputType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Artifact => "artifact",
            Self::String => "string",
            Self::Integer => "integer",
            Self::Boolean => "boolean",
            Self::Json => "json",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "artifact" => Some(Self::Artifact),
            "string" => Some(Self::String),
            "integer" => Some(Self::Integer),
            "boolean" => Some(Self::Boolean),
            "json" => Some(Self::Json),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Concurrency {
    Parallel,
    JobExclusive,
    GlobalExclusive,
}

impl Default for Concurrency {
    fn default() -> Self {
        Self::Parallel
    }
}

impl Concurrency {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Parallel => "parallel",
            Self::JobExclusive => "job_exclusive",
            Self::GlobalExclusive => "global_exclusive",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "parallel" => Some(Self::Parallel),
            "job_exclusive" => Some(Self::JobExclusive),
            "global_exclusive" => Some(Self::GlobalExclusive),
            _ => None,
        }
    }
}

fn is_false(value: &bool) -> bool {
    !*value
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serializes_schema_protocol_enum_values_as_snake_case() {
        assert_eq!(
            serde_json::to_value(InputType::Artifact).expect("input type"),
            "artifact"
        );
        assert_eq!(
            serde_json::to_value(OutputType::Json).expect("output type"),
            "json"
        );
    }

    #[test]
    fn job_definition_defaults_collections() {
        let value: JobDefinitionResponse = serde_json::from_str(
            r#"{"name":"build","concurrency":"parallel","timeout_seconds":60}"#,
        )
        .expect("definition");

        assert!(value.inputs.is_empty());
        assert!(value.outputs.is_empty());
    }
}
