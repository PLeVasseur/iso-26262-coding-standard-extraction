use std::fs;
use std::path::Path;

use anyhow::{Context, Result, bail};
use regex::Regex;
use tracing::info;

use crate::cli::InventoryArgs;
use crate::model::{PdfEntry, PdfInventoryManifest};
use crate::util::{now_utc_string, sha256_file, write_json_pretty};

pub fn run(args: InventoryArgs) -> Result<()> {
    let manifest = build_manifest(&args.cache_root)?;

    if args.dry_run {
        info!(
            pdf_count = manifest.pdf_count,
            source = %manifest.source_directory,
            "inventory dry-run complete"
        );
        return Ok(());
    }

    let manifest_path = args
        .manifest_path
        .unwrap_or_else(|| args.cache_root.join("manifests").join("pdf_inventory.json"));

    write_json_pretty(&manifest_path, &manifest)?;
    info!(path = %manifest_path.display(), "wrote inventory manifest");
    info!(pdf_count = manifest.pdf_count, "inventory completed");

    Ok(())
}

pub fn build_manifest(cache_root: &Path) -> Result<PdfInventoryManifest> {
    let pattern =
        Regex::new(r"ISO 26262-(\d+);(\d{4})").context("failed to compile PDF filename regex")?;

    let mut pdf_paths = discover_pdfs(cache_root)?;
    pdf_paths.sort();

    if pdf_paths.is_empty() {
        bail!("no PDFs found in {}", cache_root.display());
    }

    let mut pdfs = Vec::with_capacity(pdf_paths.len());
    for path in pdf_paths {
        let filename = path
            .file_name()
            .and_then(|name| name.to_str())
            .map(ToOwned::to_owned)
            .with_context(|| format!("invalid UTF-8 filename: {}", path.display()))?;

        let (part, year) = parse_part_year(&filename, &pattern)?;
        let sha256 = sha256_file(&path)?;

        pdfs.push(PdfEntry {
            filename,
            part,
            year,
            sha256,
        });
    }

    pdfs.sort_by(|a, b| a.part.cmp(&b.part).then(a.filename.cmp(&b.filename)));

    Ok(PdfInventoryManifest {
        manifest_version: 1,
        generated_at: now_utc_string(),
        source_directory: cache_root.display().to_string(),
        pdf_count: pdfs.len(),
        pdfs,
    })
}

fn discover_pdfs(cache_root: &Path) -> Result<Vec<std::path::PathBuf>> {
    let mut pdfs = Vec::new();

    let entries = fs::read_dir(cache_root)
        .with_context(|| format!("failed to read {}", cache_root.display()))?;

    for entry in entries {
        let entry =
            entry.with_context(|| format!("failed to read entry in {}", cache_root.display()))?;
        let path = entry.path();

        if !entry
            .file_type()
            .with_context(|| format!("failed to inspect file type: {}", path.display()))?
            .is_file()
        {
            continue;
        }

        let is_pdf = path
            .extension()
            .and_then(|ext| ext.to_str())
            .map(|ext| ext.eq_ignore_ascii_case("pdf"))
            .unwrap_or(false);

        if is_pdf {
            pdfs.push(path);
        }
    }

    Ok(pdfs)
}

fn parse_part_year(filename: &str, pattern: &Regex) -> Result<(u32, u32)> {
    let captures = pattern
        .captures(filename)
        .with_context(|| format!("filename does not match expected ISO pattern: {filename}"))?;

    let part = captures
        .get(1)
        .map(|m| m.as_str())
        .context("missing part capture")?
        .parse::<u32>()
        .with_context(|| format!("invalid part number in filename: {filename}"))?;

    let year = captures
        .get(2)
        .map(|m| m.as_str())
        .context("missing year capture")?
        .parse::<u32>()
        .with_context(|| format!("invalid year in filename: {filename}"))?;

    Ok((part, year))
}
