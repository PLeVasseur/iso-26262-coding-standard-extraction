fn non_whitespace_char_count(text: &str) -> usize {
    text.chars()
        .filter(|character| !character.is_whitespace())
        .count()
}

fn extract_page_with_ocr(pdf_path: &Path, page_number: usize, ocr_lang: &str) -> Result<String> {
    let pdf_stem = pdf_path
        .file_stem()
        .and_then(|value| value.to_str())
        .unwrap_or("pdf");
    let safe_stem = pdf_stem
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() {
                character
            } else {
                '_'
            }
        })
        .collect::<String>();

    let stamp = Utc::now().timestamp_nanos_opt().unwrap_or_default();
    let output_root = std::env::temp_dir().join(format!(
        "iso26262_ocr_{}_{}_{}_{}",
        safe_stem,
        std::process::id(),
        page_number,
        stamp
    ));
    let png_path = PathBuf::from(format!("{}.png", output_root.display()));

    let pdftoppm_output = Command::new("pdftoppm")
        .arg("-f")
        .arg(page_number.to_string())
        .arg("-l")
        .arg(page_number.to_string())
        .arg("-singlefile")
        .arg("-png")
        .arg(pdf_path)
        .arg(&output_root)
        .output()
        .with_context(|| format!("failed to execute pdftoppm for {}", pdf_path.display()))?;

    if !pdftoppm_output.status.success() {
        let stderr = String::from_utf8_lossy(&pdftoppm_output.stderr);
        bail!(
            "pdftoppm returned non-zero exit status for {} page {}: {}",
            pdf_path.display(),
            page_number,
            stderr.trim()
        );
    }

    if !png_path.exists() {
        bail!(
            "pdftoppm did not produce expected image for {} page {}",
            pdf_path.display(),
            page_number
        );
    }

    let tesseract_output = Command::new("tesseract")
        .arg(&png_path)
        .arg("stdout")
        .arg("-l")
        .arg(ocr_lang)
        .output()
        .with_context(|| format!("failed to execute tesseract for {}", png_path.display()))?;

    let _ = fs::remove_file(&png_path);

    if !tesseract_output.status.success() {
        let stderr = String::from_utf8_lossy(&tesseract_output.stderr);
        bail!(
            "tesseract returned non-zero exit status for {} page {}: {}",
            pdf_path.display(),
            page_number,
            stderr.trim()
        );
    }

    Ok(String::from_utf8_lossy(&tesseract_output.stdout)
        .replace('\u{0000}', "")
        .trim()
        .to_string())
}

fn command_available(program: &str) -> bool {
    Command::new(program).arg("--version").output().is_ok()
}

fn extract_pages_with_pdftotext(
    pdf_path: &Path,
    max_pages_per_doc: Option<usize>,
) -> Result<Vec<String>> {
    let mut command = Command::new("pdftotext");
    command.arg("-enc").arg("UTF-8").arg("-f").arg("1");
    if let Some(max_pages) = max_pages_per_doc {
        command.arg("-l").arg(max_pages.to_string());
    }
    command.arg(pdf_path).arg("-");

    let output = command
        .output()
        .with_context(|| format!("failed to execute pdftotext for {}", pdf_path.display()))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!(
            "pdftotext returned non-zero exit status for {}: {}",
            pdf_path.display(),
            stderr.trim()
        );
    }

    let raw = String::from_utf8_lossy(&output.stdout);
    let mut pages: Vec<String> = raw
        .split('\u{000C}')
        .map(|chunk| chunk.replace('\u{0000}', ""))
        .collect();

    while let Some(last_page) = pages.last() {
        if last_page.trim().is_empty() {
            pages.pop();
            continue;
        }
        break;
    }

    Ok(pages)
}

fn sync_fts_index(connection: &Connection) -> Result<()> {
    connection
        .execute("INSERT INTO chunks_fts(chunks_fts) VALUES('rebuild')", [])
        .context("failed to rebuild FTS index")?;
    Ok(())
}

fn count_rows(connection: &Connection, sql: &str) -> Result<i64> {
    let count = connection.query_row(sql, [], |row| row.get(0))?;
    Ok(count)
}

fn collect_tool_versions() -> Result<ToolVersions> {
    Ok(ToolVersions {
        rustc: command_version("rustc", &["--version"])?,
        cargo: command_version("cargo", &["--version"])?,
        pdftotext: command_version("pdftotext", &["-v"])?,
        pdftohtml: command_version("pdftohtml", &["-v"])?,
        pdftoppm: command_version_optional("pdftoppm", &["-v"]),
        tesseract: command_version_optional("tesseract", &["--version"]),
    })
}

fn command_version_optional(program: &str, args: &[&str]) -> Option<String> {
    let output = Command::new(program).args(args).output().ok()?;

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let source = if stdout.trim().is_empty() {
        stderr.trim()
    } else {
        stdout.trim()
    };

    source
        .lines()
        .next()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(|line| line.to_string())
}

fn command_version(program: &str, args: &[&str]) -> Result<String> {
    let output = Command::new(program)
        .args(args)
        .output()
        .with_context(|| format!("failed to run {} {}", program, args.join(" ")))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!("{} {} failed: {}", program, args.join(" "), stderr.trim());
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let source = if stdout.trim().is_empty() {
        stderr.trim()
    } else {
        stdout.trim()
    };

    let version_line = source
        .lines()
        .next()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .unwrap_or("unknown");

    Ok(version_line.to_string())
}

fn doc_id_for(pdf: &PdfEntry) -> String {
    format!("ISO26262-{}-{}", pdf.part, pdf.year)
}

fn render_ingest_command(args: &IngestArgs) -> String {
    let mut command = vec![
        "iso26262".to_string(),
        "ingest".to_string(),
        "--cache-root".to_string(),
        args.cache_root.display().to_string(),
    ];

    if let Some(path) = &args.inventory_manifest_path {
        command.push("--inventory-manifest-path".to_string());
        command.push(path.display().to_string());
    }
    if let Some(path) = &args.ingest_manifest_path {
        command.push("--ingest-manifest-path".to_string());
        command.push(path.display().to_string());
    }
    if let Some(path) = &args.db_path {
        command.push("--db-path".to_string());
        command.push(path.display().to_string());
    }
    if args.refresh_inventory {
        command.push("--refresh-inventory".to_string());
    }
    if args.seed_page_chunks {
        command.push("--seed-page-chunks".to_string());
    }
    for part in &args.target_parts {
        command.push("--target-part".to_string());
        command.push(part.to_string());
    }
    if let Some(max_pages) = args.max_pages_per_doc {
        command.push("--max-pages-per-doc".to_string());
        command.push(max_pages.to_string());
    }
    if args.ocr_mode != OcrMode::Off {
        command.push("--ocr-mode".to_string());
        command.push(args.ocr_mode.as_str().to_string());
        command.push("--ocr-lang".to_string());
        command.push(args.ocr_lang.clone());
        command.push("--ocr-min-text-chars".to_string());
        command.push(args.ocr_min_text_chars.to_string());
    }

    command.join(" ")
}
