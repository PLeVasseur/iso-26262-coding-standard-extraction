use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::Path;

use anyhow::{Context, Result};
use chrono::{DateTime, SecondsFormat, Utc};
use serde::Serialize;
use sha2::{Digest, Sha256};

pub fn now_utc_string() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true)
}

pub fn utc_compact_string(ts: DateTime<Utc>) -> String {
    ts.format("%Y%m%dT%H%M%SZ").to_string()
}

pub fn ensure_directory(path: &Path) -> Result<()> {
    fs::create_dir_all(path)
        .with_context(|| format!("failed to create directory: {}", path.display()))
}

pub fn sha256_file(path: &Path) -> Result<String> {
    let mut file = File::open(path)
        .with_context(|| format!("failed to open file for hashing: {}", path.display()))?;

    let mut hasher = Sha256::new();
    let mut buf = [0_u8; 8192];

    loop {
        let count = file
            .read(&mut buf)
            .with_context(|| format!("failed to read file for hashing: {}", path.display()))?;
        if count == 0 {
            break;
        }
        hasher.update(&buf[..count]);
    }

    Ok(format!("{:x}", hasher.finalize()))
}

pub fn write_json_pretty<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    if let Some(parent) = path.parent() {
        ensure_directory(parent)?;
    }

    let data = serde_json::to_vec_pretty(value)
        .with_context(|| format!("failed to serialize json: {}", path.display()))?;

    let mut file = File::create(path)
        .with_context(|| format!("failed to create json file: {}", path.display()))?;
    file.write_all(&data)
        .with_context(|| format!("failed to write json file: {}", path.display()))?;
    file.write_all(b"\n")
        .with_context(|| format!("failed to finalize json file: {}", path.display()))?;

    Ok(())
}
