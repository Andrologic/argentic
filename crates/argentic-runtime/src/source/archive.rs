//! AgSDL archive format 1. Read or write one bounded ZIP32 member at a time.
use super::{invalid, parse_json, sha256, Files, ProjectFile, MANIFEST, MAX_FILES, MAX_TOTAL};
use crate::persistence::Result;
use serde::Deserialize;
use std::{
    collections::BTreeMap,
    io::{BufReader, Read, Seek, SeekFrom, Write},
};
use unicode_general_category::{get_general_category, GeneralCategory};
use unicode_normalization::UnicodeNormalization;

pub(super) const INDEX_LIMIT: u64 = 2 * 1024 * 1024;
const DIRECTORY_LIMIT: u64 = 8 * 1024 * 1024;
const FORMAT: &str = "agsdl-archive-1";
const INDEX_NAME: &str = "archive.json";
const PROJECT_PREFIX: &str = "project/";

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct Inventory {
    archive_format: String,
    files: BTreeMap<String, InventoryFile>,
    history: Option<History>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct InventoryFile {
    size: serde_json::Number,
    sha256: String,
    executable: bool,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct History {
    size: serde_json::Number,
    sha256: String,
    commit: String,
}

pub(super) fn hex(value: &str, len: usize) -> bool {
    value.len() == len
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

pub(super) fn inventory(files: &Files) -> Result<Vec<u8>> {
    let rows = files
        .iter()
        .map(|(name, file)| {
            (
                name.clone(),
                serde_json::json!({"executable":file.executable,
                    "sha256":sha256(&file.bytes),"size":file.bytes.len()}),
            )
        })
        .collect::<serde_json::Map<_, _>>();
    let index = serde_json::json!({"archiveFormat":FORMAT,"files":rows});
    let mut bytes = serde_json::to_vec(&index)?;
    bytes.push(b'\n');
    Ok(bytes)
}

fn check_export_limits(files: usize, total: u64, index: u64, directory: u64) -> Result<()> {
    let message = if files + 1 > MAX_FILES {
        "Archive including its inventory exceeds the 8192 entry limit"
    } else if index > INDEX_LIMIT {
        "Archive inventory exceeds the 2 MiB limit"
    } else if total + index > MAX_TOTAL {
        "Archive including its inventory exceeds the 256 MiB total"
    } else if directory > DIRECTORY_LIMIT {
        "Archive central directory exceeds the 8 MiB limit"
    } else {
        return Ok(());
    };
    Err(invalid(INDEX_NAME, "", message))
}

/// Package the already validated selection from an opened project.
pub(super) fn write(files: &Files, output: impl Write) -> Result<()> {
    if !files.contains_key(MANIFEST) {
        return Err(invalid(
            MANIFEST,
            "",
            "No selected project files to archive",
        ));
    }
    let index = inventory(files)?;
    let total = files.values().map(|file| file.bytes.len() as u64).sum();
    let directory = 46
        + INDEX_NAME.len() as u64
        + files
            .keys()
            .map(|name| (46 + PROJECT_PREFIX.len() + name.len()) as u64)
            .sum::<u64>();
    check_export_limits(files.len(), total, index.len() as u64, directory)?;

    let members = files.iter().map(|(name, file)| {
        (
            format!("{PROJECT_PREFIX}{name}"),
            file.bytes.as_slice(),
            file.executable,
        )
    });
    // Put the inventory last. Its fixed name and attributes prevent a local
    // header offset in the final directory record from mimicking a ZIP64 locator.
    let members = members.chain(std::iter::once((
        INDEX_NAME.into(),
        index.as_slice(),
        false,
    )));
    write_stored_zip(output, files.len() + 1, members)
        .map_err(|error| crate::persistence::failure("STORAGE", "archive", &error.to_string()))
}

/// The bounded sizes checked above fit ZIP32 with room for both sets of
/// headers. Stored entries let us write the ZIP to any sequential sink.
fn write_stored_zip<'a>(
    mut output: impl Write,
    count: usize,
    entries: impl Iterator<Item = (String, &'a [u8], bool)>,
) -> std::io::Result<()> {
    fn set16(bytes: &mut [u8], at: usize, value: u16) {
        bytes[at..at + 2].copy_from_slice(&value.to_le_bytes());
    }
    fn set32(bytes: &mut [u8], at: usize, value: u32) {
        bytes[at..at + 4].copy_from_slice(&value.to_le_bytes());
    }

    let mut offset = 0;
    let mut directory = Vec::new();
    for (name, bytes, executable) in entries {
        let mut local = [0; 30];
        local[..4].copy_from_slice(b"PK\x03\x04");
        set16(&mut local, 4, 20);
        set16(&mut local, 6, 0x800); // UTF-8 names.
        set16(&mut local, 12, 0x21); // 1980-01-01 at midnight.
        set32(&mut local, 14, crc32fast::hash(bytes));
        set32(&mut local, 18, bytes.len() as u32);
        set32(&mut local, 22, bytes.len() as u32);
        set16(&mut local, 26, name.len() as u16);

        let mut central = [0; 46];
        central[..4].copy_from_slice(b"PK\x01\x02");
        set16(&mut central, 4, 0x0314); // Unix external attributes.
        central[6..30].copy_from_slice(&local[4..28]);
        let mode = if executable { 0o100755 } else { 0o100644 };
        set32(&mut central, 38, mode << 16);
        set32(&mut central, 42, offset);
        directory.extend(central);
        directory.extend(name.as_bytes());

        output.write_all(&local)?;
        output.write_all(name.as_bytes())?;
        output.write_all(bytes)?;
        offset += (local.len() + name.len() + bytes.len()) as u32;
    }
    let mut footer = [0; 22];
    footer[..4].copy_from_slice(b"PK\x05\x06");
    set16(&mut footer, 8, count as u16);
    set16(&mut footer, 10, count as u16);
    set32(&mut footer, 12, directory.len() as u32);
    set32(&mut footer, 16, offset);
    output.write_all(&directory)?;
    output.write_all(&footer)
}

/// Collision key: case folding then NFC, as the archive format requires.
pub(super) fn path_key(path: &str) -> String {
    unicase::UniCase::new(path).to_folded_case().nfc().collect()
}

pub(super) fn portable(name: &str) -> Result<()> {
    if name.len() > 4096 || name.split('/').count() > 64 {
        return Err(invalid(
            name,
            "",
            "Portable path exceeds depth or length limit",
        ));
    }
    for part in name.split('/') {
        let folded = unicase::UniCase::new(part).to_folded_case();
        // Windows ignores spaces before the extension: `nul .md` is NUL.
        // Compare as the official tool's case-insensitive pattern does,
        // where dotless and dotted capital I also match `i`.
        let stem: String = part
            .split('.')
            .next()
            .unwrap_or("")
            .trim_end_matches(' ')
            .chars()
            .map(|c| match c {
                '\u{131}' | '\u{130}' => 'i',
                c => c.to_ascii_lowercase(),
            })
            .collect();
        let stem = stem.as_str();
        let device = matches!(stem, "con" | "prn" | "aux" | "nul" | "conin$" | "conout$")
            || stem
                .strip_prefix("com")
                .or_else(|| stem.strip_prefix("lpt"))
                .is_some_and(|n| {
                    matches!(
                        n,
                        "1" | "2" | "3" | "4" | "5" | "6" | "7" | "8" | "9" | "¹" | "²" | "³"
                    )
                });
        if part.is_empty()
            || part.len() > 255
            || part.ends_with([' ', '.'])
            || device
            || matches!(folded.as_str(), ".git" | ".env" | ".agsdl-history.bundle")
            || folded.starts_with(".env.")
            || part.chars().any(|c| {
                "<>:\"\\|?*".contains(c)
                    || matches!(
                        get_general_category(c),
                        GeneralCategory::Control
                            | GeneralCategory::Format
                            | GeneralCategory::PrivateUse
                            | GeneralCategory::Surrogate
                            | GeneralCategory::Unassigned
                    )
            })
        {
            return Err(invalid(name, "", "Invalid portable path"));
        }
    }
    Ok(())
}

fn u16_at(bytes: &[u8], at: usize) -> u16 {
    u16::from_le_bytes(bytes[at..at + 2].try_into().unwrap())
}

fn u32_at(bytes: &[u8], at: usize) -> u32 {
    u32::from_le_bytes(bytes[at..at + 4].try_into().unwrap())
}

fn read_at<const N: usize>(
    input: &mut (impl Read + Seek),
    offset: u64,
) -> std::io::Result<[u8; N]> {
    let mut bytes = [0; N];
    input.seek(SeekFrom::Start(offset))?;
    input.read_exact(&mut bytes)?;
    Ok(bytes)
}

/// Accept extra fields that neither need ZIP64 nor rename the entry. An
/// Info-ZIP Unicode Path field (0x7075) must repeat the entry's own name,
/// since readers that honour it would otherwise see another file.
fn plain_extras(mut extra: &[u8], name: &str) -> bool {
    while !extra.is_empty() {
        if extra.len() < 4 || u16_at(extra, 0) == 1 {
            return false;
        }
        let len = 4 + usize::from(u16_at(extra, 2));
        let Some(rest) = extra.get(len..) else {
            return false;
        };
        if u16_at(extra, 0) == 0x7075 && extra.get(9..len) != Some(name.as_bytes()) {
            return false;
        }
        extra = rest;
    }
    true
}

struct Member {
    offset: u64,
    end: u64,
    header: [u8; 46],
}

impl Member {
    fn size(&self) -> u64 {
        u64::from(u32_at(&self.header, 24))
    }

    fn read(&self, input: &mut (impl Read + Seek), name: &str) -> Result<Vec<u8>> {
        let malformed = |detail| invalid(name, "", detail);
        let io = |e: std::io::Error| invalid(name, "", &e.to_string());
        let header = read_at::<30>(input, self.offset).map_err(io)?;
        let flags = u16_at(&header, 6);
        let method = u16_at(&header, 8);
        let descriptor = flags & 8 != 0;
        let crc = u32_at(&self.header, 16);
        let compressed = u64::from(u32_at(&self.header, 20));
        let sizes_match = (14..26).step_by(4).all(|at| {
            u32_at(&header, at) == u32_at(&self.header, at + 2)
                || (descriptor && u32_at(&header, at) == 0)
        });
        if &header[..4] != b"PK\x03\x04"
            || header[4..14] != self.header[6..16]
            || !sizes_match
            || usize::from(u16_at(&header, 26)) != name.len()
        {
            return Err(malformed("Local header differs from ZIP directory"));
        }
        let extra_len = usize::from(u16_at(&header, 28));
        let start = self.offset + 30 + name.len() as u64 + extra_len as u64;
        let data_end = start + compressed;
        if data_end > self.end || (method == 0 && compressed != self.size()) {
            return Err(malformed(
                "ZIP member overlaps another structure or has inconsistent sizes",
            ));
        }
        let mut fields = vec![0; name.len() + extra_len];
        input.read_exact(&mut fields).map_err(io)?;
        if &fields[..name.len()] != name.as_bytes() || !plain_extras(&fields[name.len()..], name) {
            return Err(malformed("Invalid ZIP local name or extra fields"));
        }
        let bytes = if method == 0 {
            let mut bytes = Vec::new();
            input.take(compressed).read_to_end(&mut bytes).map_err(io)?;
            bytes
        } else {
            inflate(input.take(compressed), compressed, self.size()).map_err(io)?
        };
        if bytes.len() as u64 != self.size() || crc32fast::hash(&bytes) != crc {
            return Err(malformed("Incorrect ZIP member size or CRC"));
        }
        if descriptor {
            let len = (self.end - data_end).min(16) as usize;
            let mut tail = [0; 16];
            input.seek(SeekFrom::Start(data_end)).map_err(io)?;
            input.read_exact(&mut tail[..len]).map_err(io)?;
            let matches = |at| tail[at..at + 12] == self.header[16..28];
            if !(len >= 12 && matches(0) || len >= 16 && &tail[..4] == b"PK\x07\x08" && matches(4))
            {
                return Err(malformed("Invalid ZIP data descriptor"));
            }
        }
        Ok(bytes)
    }
}

fn inflate(input: impl Read, compressed: u64, size: u64) -> std::io::Result<Vec<u8>> {
    use flate2::{Decompress, FlushDecompress, Status};
    use std::io::BufRead;

    let mut stream = BufReader::new(input);
    let mut decoder = Decompress::new(false);
    let mut output = [0; 8192];
    let mut bytes = Vec::new();
    loop {
        let data = stream.fill_buf()?;
        let before = (decoder.total_in(), decoder.total_out());
        let status = decoder.decompress(data, &mut output, FlushDecompress::None)?;
        stream.consume((decoder.total_in() - before.0) as usize);
        if decoder.total_out() > size {
            return Err(std::io::Error::other(
                "Deflate stream exceeds its declared size",
            ));
        }
        bytes.extend_from_slice(&output[..(decoder.total_out() - before.1) as usize]);
        if status == Status::StreamEnd
            && decoder.total_in() == compressed
            && decoder.total_out() == size
        {
            return Ok(bytes);
        }
        if status == Status::StreamEnd || before == (decoder.total_in(), decoder.total_out()) {
            return Err(std::io::Error::other("Invalid complete deflate stream"));
        }
    }
}

fn members(input: &mut (impl Read + Seek)) -> Result<BTreeMap<String, Member>> {
    let zip_error = |detail| invalid("ZIP", "", detail);
    let io = |e: std::io::Error| invalid("ZIP", "", &e.to_string());
    let size = input.seek(SeekFrom::End(0)).map_err(io)?;
    if !(22..=2 * MAX_TOTAL).contains(&size) {
        return Err(zip_error("Invalid ZIP container size"));
    }
    let footer = read_at::<22>(input, size - 22).map_err(io)?;
    let count = usize::from(u16_at(&footer, 10));
    let length = u64::from(u32_at(&footer, 12));
    let directory = u64::from(u32_at(&footer, 16));
    if &footer[..4] != b"PK\x05\x06"
        || footer[4..8] != [0; 4]
        || footer[8..10] != footer[10..12]
        || footer[20..22] != [0; 2]
        || !(1..=MAX_FILES).contains(&count)
        || length > DIRECTORY_LIMIT
        || directory + length != size - 22
        || (size >= 42 && read_at::<4>(input, size - 42).map_err(io)? == *b"PK\x06\x07")
    {
        return Err(zip_error(
            "Expected single-volume ZIP32 with a bounded central directory",
        ));
    }
    let mut cursor = directory;
    let mut total = 0;
    let mut entries = BTreeMap::new();
    for _ in 0..count {
        if cursor + 46 > size - 22 {
            return Err(zip_error("Truncated ZIP directory"));
        }
        let header = read_at::<46>(input, cursor).map_err(io)?;
        let name_len = usize::from(u16_at(&header, 28));
        let extra_len = usize::from(u16_at(&header, 30));
        cursor += 46 + (name_len + extra_len + usize::from(u16_at(&header, 32))) as u64;
        let flags = u16_at(&header, 8);
        let method = u16_at(&header, 10);
        let attrs = u32_at(&header, 38);
        let offset = u64::from(u32_at(&header, 42));
        total += u64::from(u32_at(&header, 24));
        if &header[..4] != b"PK\x01\x02"
            || flags & !(0x800 | 8 | if method == 8 { 6 } else { 0 }) != 0
            || !matches!(method, 0 | 8)
            || !matches!((attrs >> 16) & 0o170000, 0 | 0o100000)
            || attrs & 0x10 != 0
            || u16_at(&header, 34) != 0
            || cursor > size - 22
            || total > MAX_TOTAL
            || offset + 30 > directory
        {
            return Err(zip_error(
                "Invalid ZIP directory entry or exceeded archive limits",
            ));
        }
        let mut fields = vec![0; name_len + extra_len];
        input.read_exact(&mut fields).map_err(io)?;
        let name = std::str::from_utf8(&fields[..name_len])
            .map_err(|_| zip_error("Non-UTF-8 ZIP name"))?;
        if (!name.is_ascii() && flags & 0x800 == 0) || !plain_extras(&fields[name_len..], name) {
            return Err(invalid(name, "", "Invalid UTF-8 flag or ZIP extra fields"));
        }
        if let Some(path) = name.strip_prefix(PROJECT_PREFIX) {
            portable(path)?;
        } else if !matches!(name, "archive.json" | "history.bundle") {
            return Err(invalid(name, "", "Unexpected ZIP entry"));
        }
        if entries
            .insert(
                name.to_owned(),
                Member {
                    offset,
                    end: directory,
                    header,
                },
            )
            .is_some()
        {
            return Err(invalid(name, "", "Duplicate ZIP entry"));
        }
    }
    if cursor != size - 22 {
        return Err(zip_error("ZIP directory size or entry count differs"));
    }
    let mut by_offset: Vec<_> = entries.values_mut().collect();
    by_offset.sort_unstable_by_key(|member| member.offset);
    let mut end = directory;
    for member in by_offset.into_iter().rev() {
        if member.offset + 30 > end {
            return Err(zip_error("Overlapping ZIP local headers"));
        }
        member.end = end;
        end = member.offset;
    }
    Ok(entries)
}

pub(super) fn read(mut input: impl Read + Seek) -> Result<Files> {
    let mut entries = members(&mut input)?;
    let manifest = entries
        .remove(INDEX_NAME)
        .filter(|entry| entry.size() <= INDEX_LIMIT)
        .ok_or_else(|| invalid("archive.json", "", "Missing or oversized inventory"))?;
    let raw = manifest.read(&mut input, "archive.json")?;
    let value = parse_json(&raw, "archive.json", false)?;
    if value.get("history").is_some_and(serde_json::Value::is_null) {
        return Err(invalid(
            "archive.json",
            "/history",
            "Expected a history object",
        ));
    }
    // Serde's arbitrary-precision Number uses a private object encoding.
    // Inventory sizes must be authored as JSON numbers.
    for (name, file) in value["files"].as_object().into_iter().flatten() {
        if !file["size"].is_number() {
            return Err(invalid(
                name,
                "/size",
                "Expected an inventory size as a number",
            ));
        }
    }
    if value
        .get("history")
        .is_some_and(|history| !history["size"].is_number())
    {
        return Err(invalid(
            "archive.json",
            "/history/size",
            "Expected a bundle size as a number",
        ));
    }
    let index: Inventory =
        serde_json::from_value(value).map_err(|e| invalid("archive.json", "", &e.to_string()))?;
    if index.archive_format != FORMAT || !index.files.contains_key(MANIFEST) {
        return Err(invalid(
            "archive.json",
            "",
            "Expected archive format 1 and project/agsdl.json",
        ));
    }
    let mut spellings = BTreeMap::new();
    for name in index.files.keys() {
        portable(name)?;
        for (end, _) in name
            .match_indices('/')
            .chain(std::iter::once((name.len(), "")))
        {
            let path = &name[..end];
            let key = path_key(path);
            if spellings
                .insert(key, path)
                .is_some_and(|previous| previous != path)
                || (path != name && index.files.contains_key(path))
                || spellings.len() > 16_384
            {
                return Err(invalid(
                    name,
                    "",
                    "Conflicting project paths or too many paths",
                ));
            }
        }
    }
    let mut read = |name: &str, size: serde_json::Number, digest: &str| {
        let member = entries
            .remove(name)
            .ok_or_else(|| invalid(name, "", "Missing ZIP member"))?;
        if !argentic_core::durable_json::equal(&member.size().to_string(), &size.to_string())
            .unwrap_or(false)
            || !hex(digest, 64)
        {
            return Err(invalid(name, "", "Invalid inventory size or SHA-256"));
        }
        let bytes = member.read(&mut input, name)?;
        if sha256(&bytes) != digest {
            return Err(invalid(name, "", "SHA-256 does not match the inventory"));
        }
        Ok(bytes)
    };
    if let Some(history) = index.history {
        if !hex(&history.commit, 40) {
            return Err(invalid(
                "archive.json",
                "/history/commit",
                "Invalid history commit",
            ));
        }
        read("history.bundle", history.size, &history.sha256)?;
    }
    let mut files = Files::new();
    for (name, file) in index.files {
        let bytes = read(&format!("{PROJECT_PREFIX}{name}"), file.size, &file.sha256)?;
        files.insert(
            name,
            ProjectFile {
                bytes,
                executable: file.executable,
            },
        );
    }
    if let Some(name) = entries.keys().next() {
        return Err(invalid(name, "", "ZIP member absent from inventory"));
    }
    Ok(files)
}

#[cfg(test)]
pub(super) mod tests {
    use super::*;
    use crate::source::OpenedSource;
    use serde_json::{json, Value};

    // Stored ZIP32 entries make the bytes under each integrity check explicit.
    // The successful interoperability cases use the official packer fixtures.
    pub(in crate::source) fn stored_zip(entries: &BTreeMap<String, Vec<u8>>) -> Vec<u8> {
        let mut archive = Vec::new();
        let members = entries
            .iter()
            .map(|(name, bytes)| (name.clone(), bytes.as_slice(), false));
        write_stored_zip(&mut archive, entries.len(), members).unwrap();
        archive
    }

    const FIXTURE: &[u8] = include_bytes!("../../tests/fixtures/agsdl-project/review.agsdl");

    fn baseline() -> (BTreeMap<String, Vec<u8>>, Value) {
        let files = read(std::io::Cursor::new(FIXTURE)).unwrap();
        let inventory = files
            .iter()
            .map(|(name, file)| (name.clone(), row(&file.bytes)))
            .collect::<serde_json::Map<String, Value>>();
        let entries = files
            .into_iter()
            .map(|(name, file)| (format!("project/{name}"), file.bytes))
            .collect();
        (
            entries,
            json!({"archiveFormat":"agsdl-archive-1","files":inventory}),
        )
    }

    pub(in crate::source) fn row(bytes: &[u8]) -> Value {
        json!({"size":bytes.len(),"sha256":sha256(bytes),"executable":false})
    }

    #[test]
    fn export_limits_count_the_inventory() {
        let files = MAX_FILES - 1;
        let total = MAX_TOTAL - INDEX_LIMIT;
        check_export_limits(files, total, INDEX_LIMIT, DIRECTORY_LIMIT).unwrap();
        for (files, total, index, directory, expected) in [
            (files + 1, total, INDEX_LIMIT, DIRECTORY_LIMIT, "8192 entry"),
            (files, total + 1, INDEX_LIMIT, DIRECTORY_LIMIT, "256 MiB"),
            (files, 0, INDEX_LIMIT + 1, DIRECTORY_LIMIT, "2 MiB"),
            (files, total, INDEX_LIMIT, DIRECTORY_LIMIT + 1, "8 MiB"),
        ] {
            let error = check_export_limits(files, total, index, directory).unwrap_err();
            assert_eq!(error.code, "PROJECT");
            assert!(error.message.contains(expected), "{error:?}");
        }
    }

    #[tokio::test]
    async fn invalid_archives_are_refused_before_the_standalone_database_is_opened() {
        let temp = tempfile::tempdir().unwrap();
        let workspace = temp.path().join("workspace");
        let data = temp.path().join("data");
        let source = temp.path().join("review.agsdl");
        let policy = temp.path().join("policy.json");
        std::fs::create_dir(&workspace).unwrap();
        let approval = include_bytes!("../../examples/fixtures/policy.json");
        std::fs::write(&policy, approval).unwrap();
        let digest = sha256(approval);
        let asset = "mcp/asset.txt";
        for (variant, needle) in [
            ("hash", "SHA-256"),
            ("outside", "agents/reviewer/agent.json#/instructions/0/uri"),
            ("unlisted", "absent from inventory"),
            ("unused", "not selected by the project"),
            ("missing", "/mcpFiles/local/0"),
            ("history", "history.bundle"),
        ] {
            let (mut entries, mut index) = baseline();
            match variant {
                "hash" => index["files"][asset]["sha256"] = json!("0".repeat(64)),
                "outside" => {
                    let name = "agents/reviewer/agent.json";
                    let entry = entries.get_mut(&format!("project/{name}")).unwrap();
                    let mut agent: Value = serde_json::from_slice(entry).unwrap();
                    agent["instructions"][0]["uri"] = json!("../../../outside.txt");
                    *entry = serde_json::to_vec(&agent).unwrap();
                    index["files"][name] = row(entry);
                }
                "unlisted" => {
                    entries.insert("project/extra.txt".into(), b"extra".to_vec());
                }
                "unused" => {
                    entries.insert("project/extra.txt".into(), b"extra".to_vec());
                    index["files"]["extra.txt"] = row(b"extra");
                }
                "missing" => {
                    entries.remove(&format!("project/{asset}"));
                    index["files"].as_object_mut().unwrap().remove(asset);
                }
                "history" => {
                    entries.insert("history.bundle".into(), b"bundle".to_vec());
                    index["history"] =
                        json!({"size":6,"sha256":"0".repeat(64),"commit":"0".repeat(40)});
                }
                _ => unreachable!(),
            }
            entries.insert("archive.json".into(), serde_json::to_vec(&index).unwrap());
            std::fs::write(&source, stored_zip(&entries)).unwrap();
            let Err(error) =
                crate::standalone::Host::open(&data, &workspace, &source, &policy, &digest).await
            else {
                panic!("accepted {variant} archive")
            };
            assert_eq!(error.code, "PROJECT");
            assert!(error.message.contains(needle), "{variant}: {error:?}");
            assert!(!data.join("engine.sqlite").exists());
        }
        let (mut entries, index) = baseline();
        entries.insert("archive.json".into(), serde_json::to_vec(&index).unwrap());
        assert!(OpenedSource::open_archive_bytes(&stored_zip(&entries)).is_ok());
    }

    #[tokio::test]
    async fn standalone_starts_the_same_human_flow_from_json_manifest_and_archive() {
        let temp = tempfile::tempdir().unwrap();
        let workspace = temp.path().join("workspace");
        std::fs::create_dir(&workspace).unwrap();
        let project = temp.path().join("project");
        std::fs::create_dir(&project).unwrap();
        let policy = temp.path().join("policy.json");
        let approval = include_bytes!("../../examples/fixtures/policy.json");
        std::fs::write(&policy, approval).unwrap();
        let source = temp.path().join("human.json");
        let mut doc: Value =
            serde_json::from_slice(include_bytes!("../../examples/fixtures/human-only.json"))
                .unwrap();
        doc["contract"] = json!("agsdl-0.4.0");
        std::fs::write(&source, serde_json::to_vec(&doc).unwrap()).unwrap();
        doc["projectFormat"] = json!("agsdl-project-1");
        let manifest = serde_json::to_vec(&doc).unwrap();
        std::fs::write(project.join("agsdl.json"), &manifest).unwrap();
        let archive = temp.path().join("human.agsdl");
        std::fs::write(&archive, stored_zip(&BTreeMap::from([
            ("archive.json".into(), serde_json::to_vec(
                &json!({"archiveFormat":"agsdl-archive-1","files":{"agsdl.json":row(&manifest)}}),
            ).unwrap()),
            ("project/agsdl.json".into(), manifest),
        ]))).unwrap();
        let mut flow = None;
        for (i, source) in [source, project.join("agsdl.json"), archive.clone()]
            .iter()
            .enumerate()
        {
            let data = temp.path().join(format!("data-{i}"));
            let host = crate::standalone::Host::open(
                &data,
                &workspace,
                source,
                &policy,
                &sha256(approval),
            )
            .await
            .unwrap();
            if source == &archive {
                std::fs::remove_file(&archive).unwrap();
            }
            let entry = crate::persistence::dto::EngineEntry {
                delivery_id: "entry".into(),
                message_json: "{}".into(),
            };
            let start = host
                .start("start".into(), Some(entry), false)
                .await
                .unwrap();
            let plan: Value = serde_json::from_str(&start.capture.compiled_plan_json).unwrap();
            let steps =
                json!({"entry":plan["entry"],"steps":plan["steps"],"routes":plan["routes"]});
            assert_eq!(flow.get_or_insert(steps.clone()), &steps);
            assert!(start.receipt.enter.is_some());
            host.shutdown(&host.active_runs().await.unwrap())
                .await
                .unwrap();
            host.close().await.unwrap();
        }
    }

    #[test]
    fn portable_names_and_deflate_boundaries() {
        for name in [
            "../file",
            "folder/CON.md",
            "a//b",
            "name.",
            "a/.Env.test",
            "folder/a\u{200b}",
            "a\\b",
            "folder/.git/config",
            "nul .md",
            "con\u{131}n$.md",
            "CON\u{130}N$.md",
        ] {
            assert!(portable(name).is_err(), "{name}");
        }
        // Names the official tool accepts stay accepted.
        for name in ["\u{ff23}\u{ff2f}\u{ff2e}.md", "console.md", "com10.txt"] {
            assert!(portable(name).is_ok(), "{name}");
        }
        // A Unicode Path extra field may only repeat the entry's name.
        let unicode_path = |name: &str| {
            let mut field = vec![0x75, 0x70];
            field.extend(((5 + name.len()) as u16).to_le_bytes());
            field.push(1);
            field.extend(crc32fast::hash(b"project/a.md").to_le_bytes());
            field.extend(name.as_bytes());
            field
        };
        assert!(plain_extras(&unicode_path("project/a.md"), "project/a.md"));
        assert!(!plain_extras(&unicode_path("project/b.md"), "project/a.md"));
        let packed = [0x03, 0x00]; // Empty raw DEFLATE stream.
        assert_eq!(inflate(&packed[..], 2, 0).unwrap(), b"");
        assert!(inflate(&packed[..1], 1, 0).is_err());
        assert!(inflate(&[0x03, 0x00, 0x00][..], 3, 0).is_err());
    }
}
