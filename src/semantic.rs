use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub const DEFAULT_MODEL_ID: &str = "miniLM-L6-v2-local-v1";
pub const DEFAULT_MODEL_NAME: &str = "sentence-transformers/all-MiniLM-L6-v2";
pub const DEFAULT_EMBEDDING_DIM: usize = 384;
pub const DEFAULT_NORMALIZATION: &str = "l2";
pub const DEFAULT_BACKEND: &str = "local-hash-v1";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SemanticModelConfig {
    pub model_id: String,
    pub model_name: String,
    pub dimensions: usize,
    pub normalization: String,
    pub backend: String,
}

pub fn resolve_model_config(model_id: &str) -> SemanticModelConfig {
    let trimmed = model_id.trim();
    let resolved_id = if trimmed.is_empty() {
        DEFAULT_MODEL_ID
    } else {
        trimmed
    };

    if resolved_id == DEFAULT_MODEL_ID {
        return SemanticModelConfig {
            model_id: DEFAULT_MODEL_ID.to_string(),
            model_name: DEFAULT_MODEL_NAME.to_string(),
            dimensions: DEFAULT_EMBEDDING_DIM,
            normalization: DEFAULT_NORMALIZATION.to_string(),
            backend: DEFAULT_BACKEND.to_string(),
        };
    }

    SemanticModelConfig {
        model_id: resolved_id.to_string(),
        model_name: resolved_id.to_string(),
        dimensions: DEFAULT_EMBEDDING_DIM,
        normalization: DEFAULT_NORMALIZATION.to_string(),
        backend: DEFAULT_BACKEND.to_string(),
    }
}

pub fn normalize_whitespace(input: &str) -> String {
    input.split_whitespace().collect::<Vec<&str>>().join(" ")
}

pub fn chunk_payload_for_embedding(
    chunk_type: &str,
    reference: &str,
    heading: &str,
    text: Option<&str>,
    table_md: Option<&str>,
) -> Option<String> {
    let chunk_type_norm = chunk_type.trim().to_ascii_lowercase();
    if !matches!(chunk_type_norm.as_str(), "clause" | "annex" | "table") {
        return None;
    }

    let mut parts = Vec::<String>::new();

    let reference_norm = normalize_whitespace(reference);
    if !reference_norm.is_empty() {
        parts.push(reference_norm);
    }

    let heading_norm = normalize_whitespace(heading);
    if !heading_norm.is_empty() {
        parts.push(heading_norm);
    }

    let body_source = if chunk_type_norm == "table" {
        table_md.or(text)
    } else {
        text
    };

    let body_norm = body_source.map(normalize_whitespace).unwrap_or_default();
    if body_norm.is_empty() {
        return None;
    }

    if parts.is_empty() {
        Some(body_norm)
    } else {
        Some(format!("{}\n\n{}", parts.join("\n"), body_norm))
    }
}

pub fn embedding_text_hash(payload: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(payload.as_bytes());
    format!("{:x}", hasher.finalize())
}

pub fn embed_text_local(payload: &str, dimensions: usize) -> Vec<f32> {
    let dims = dimensions.max(8);
    let mut vector = vec![0_f32; dims];
    let mut tokens = tokenize_payload(payload);

    if tokens.is_empty() {
        return vector;
    }

    for token in tokens.drain(..) {
        let hash = stable_hash(&token);
        let index = (hash as usize) % dims;
        let sign = if (hash >> 63) & 1 == 0 { 1.0 } else { -1.0 };
        let weight = 1.0 + (((hash >> 48) & 0xFF) as f32 / 255.0);
        vector[index] += sign * weight;
    }

    normalize_vector(&mut vector);
    vector
}

pub fn cosine_similarity(left: &[f32], right: &[f32]) -> f64 {
    if left.len() != right.len() || left.is_empty() {
        return 0.0;
    }

    left.iter()
        .zip(right.iter())
        .map(|(left_value, right_value)| f64::from(*left_value) * f64::from(*right_value))
        .sum::<f64>()
}

pub fn encode_embedding_blob(values: &[f32]) -> Vec<u8> {
    let mut out = Vec::<u8>::with_capacity(values.len() * 4);
    for value in values {
        out.extend_from_slice(&value.to_le_bytes());
    }
    out
}

pub fn decode_embedding_blob(blob: &[u8], expected_dim: usize) -> Option<Vec<f32>> {
    if expected_dim == 0 || blob.len() != expected_dim.saturating_mul(4) {
        return None;
    }

    let mut out = Vec::<f32>::with_capacity(expected_dim);
    for chunk in blob.chunks_exact(4) {
        out.push(f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]));
    }

    if out.len() == expected_dim {
        Some(out)
    } else {
        None
    }
}

fn stable_hash(value: &str) -> u64 {
    let mut hasher = DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}

fn tokenize_payload(payload: &str) -> Vec<String> {
    let normalized = normalize_whitespace(payload);
    if normalized.is_empty() {
        return Vec::new();
    }

    let words = normalized
        .split(' ')
        .map(|value| {
            value
                .chars()
                .filter(|character| character.is_ascii_alphanumeric())
                .collect::<String>()
                .to_ascii_lowercase()
        })
        .filter(|value| !value.is_empty())
        .collect::<Vec<String>>();

    if words.is_empty() {
        return Vec::new();
    }

    let mut features = Vec::<String>::with_capacity(words.len() * 2);
    for (index, word) in words.iter().enumerate() {
        features.push(format!("w:{word}"));
        if let Some(next) = words.get(index + 1) {
            features.push(format!("b:{word}_{next}"));
        }
    }
    features
}

fn normalize_vector(values: &mut [f32]) {
    let squared_norm = values
        .iter()
        .map(|value| f64::from(*value) * f64::from(*value))
        .sum::<f64>();

    if squared_norm <= 0.0 {
        return;
    }

    let norm = squared_norm.sqrt() as f32;
    if norm == 0.0 {
        return;
    }

    for value in values {
        *value /= norm;
    }
}
