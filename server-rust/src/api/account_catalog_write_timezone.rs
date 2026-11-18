//! Validate zone names and TZif content with Go time.LoadLocation's file rules.
//! TZif parsing follows Go 1.26.9 time/zoneinfo_read.go; see the sibling LICENSE.

use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

pub(super) fn valid(name: &str) -> bool {
    if matches!(name, "UTC" | "Local") {
        return true;
    }
    if name.is_empty() || name.starts_with(['/', '\\']) || name.contains("..") {
        return false;
    }
    let configured = std::env::var_os("ZONEINFO").map(std::path::PathBuf::from);
    configured
        .into_iter()
        .chain(
            [
                "/usr/share/zoneinfo",
                "/usr/share/lib/zoneinfo",
                "/usr/lib/locale/TZ",
                "/etc/zoneinfo",
            ]
            .map(std::path::PathBuf::from),
        )
        .any(|root| valid_source(&root, name))
        // Admission stores the name, not an offset. Pin Go's embedded fallback
        // catalog so slim system tzdata does not reject legacy aliases like GMT0.
        || include_str!("account_catalog_write_timezone_zones.txt")
            .lines()
            .any(|zone| zone == name)
}

fn valid_source(root: &Path, name: &str) -> bool {
    if root.as_os_str().as_encoded_bytes().ends_with(b".zip") {
        return zip_zone(root, name).is_some_and(|data| valid_tzif(&data));
    }
    // Go's directory reader limits zone files to 10 MiB. Read only one extra
    // byte so a bogus host file cannot force an unbounded allocation.
    const MAX_FILE_SIZE: u64 = 10 << 20;
    let Ok(file) = std::fs::File::open(root.join(name)) else {
        return false;
    };
    let mut data = Vec::new();
    file.take(MAX_FILE_SIZE + 1).read_to_end(&mut data).is_ok()
        && data.len() as u64 <= MAX_FILE_SIZE
        && valid_tzif(&data)
}

fn zip_zone(path: &Path, name: &str) -> Option<Vec<u8>> {
    fn u16_at(data: &[u8], start: usize) -> Option<usize> {
        Some(u16::from_le_bytes(data.get(start..start + 2)?.try_into().ok()?) as usize)
    }
    fn u32_at(data: &[u8], start: usize) -> Option<u64> {
        Some(u64::from(u32::from_le_bytes(
            data.get(start..start + 4)?.try_into().ok()?,
        )))
    }
    let mut file = std::fs::File::open(path).ok()?;
    let file_len = file.metadata().ok()?.len();
    file.seek(SeekFrom::End(-22)).ok()?;
    let mut tail = [0; 22];
    file.read_exact(&mut tail).ok()?;
    if tail[..4] != *b"PK\x05\x06" {
        return None;
    }
    let mut offset = u32_at(&tail, 16)?;
    let end = offset.checked_add(u32_at(&tail, 12)?)?;
    if end > file_len {
        return None;
    }
    for _ in 0..u16_at(&tail, 10)? {
        if offset.checked_add(46)? > end {
            return None;
        }
        file.seek(SeekFrom::Start(offset)).ok()?;
        let mut header = [0; 46];
        file.read_exact(&mut header).ok()?;
        if header[..4] != *b"PK\x01\x02" {
            return None;
        }
        let name_len = u16_at(&header, 28)?;
        offset = offset
            .checked_add(46 + name_len as u64)?
            .checked_add(u16_at(&header, 30)? as u64)?
            .checked_add(u16_at(&header, 32)? as u64)?;
        if offset > end {
            return None;
        }
        let mut entry_name = vec![0; name_len];
        file.read_exact(&mut entry_name).ok()?;
        if entry_name != name.as_bytes() {
            continue;
        }
        // Match Go: only stored entries, exact local method/name, no CRC or
        // compressed-size check. ZIP comments and ZIP64 are not supported.
        if u16_at(&header, 10)? != 0 {
            return None;
        }
        let local_offset = u32_at(&header, 42)?;
        file.seek(SeekFrom::Start(local_offset)).ok()?;
        let mut local = vec![0; 30 + name_len];
        file.read_exact(&mut local).ok()?;
        if local[..4] != *b"PK\x03\x04"
            || u16_at(&local, 8)? != 0
            || u16_at(&local, 26)? != name_len
            || local[30..] != entry_name
        {
            return None;
        }
        let data_offset = local_offset
            .checked_add(30 + name_len as u64)?
            .checked_add(u16_at(&local, 28)? as u64)?;
        let size = u32_at(&header, 24)?;
        if data_offset.checked_add(size)? > file_len {
            return None;
        }
        file.seek(SeekFrom::Start(data_offset)).ok()?;
        let mut data = Vec::new();
        file.take(size).read_to_end(&mut data).ok()?;
        return (data.len() as u64 == size).then_some(data);
    }
    None
}

fn take<'a>(data: &'a [u8], cursor: &mut usize, len: usize) -> Option<&'a [u8]> {
    let end = cursor.checked_add(len)?;
    let slice = data.get(*cursor..end)?;
    *cursor = end;
    Some(slice)
}

fn header(data: &[u8], cursor: &mut usize) -> Option<([usize; 6], bool)> {
    if take(data, cursor, 4)? != b"TZif" {
        return None;
    }
    let version = match take(data, cursor, 16)?[0] {
        0 => false,
        b'2' | b'3' => true,
        _ => return None,
    };
    let mut counts = [0; 6];
    for count in &mut counts {
        *count = u32::from_be_bytes(take(data, cursor, 4)?.try_into().ok()?) as usize;
    }
    Some((counts, version))
}

fn valid_tzif(data: &[u8]) -> bool {
    fn parse(data: &[u8]) -> Option<()> {
        let mut cursor = 0;
        let (mut counts, version) = header(data, &mut cursor)?;
        if version {
            let [utc, standard, leap, times, zones, chars] = counts;
            let skip = times
                .checked_mul(5)?
                .checked_add(zones.checked_mul(6)?)?
                .checked_add(chars)?
                .checked_add(leap.checked_mul(8)?)?
                .checked_add(standard)?
                .checked_add(utc)?;
            take(data, &mut cursor, skip)?;
            // Go ignores the version and magic in the second header, but still
            // requires all 20 header bytes and six new count fields.
            take(data, &mut cursor, 20)?;
            for count in &mut counts {
                *count = u32::from_be_bytes(take(data, &mut cursor, 4)?.try_into().ok()?) as usize;
            }
        }
        let [utc, standard, leap, times, zones, chars] = counts;
        if zones == 0 {
            return None;
        }
        take(
            data,
            &mut cursor,
            times.checked_mul(if version { 8 } else { 4 })?,
        )?;
        let transition_zones = take(data, &mut cursor, times)?;
        let zone_data = take(data, &mut cursor, zones.checked_mul(6)?)?;
        let abbreviations = take(data, &mut cursor, chars)?;
        take(
            data,
            &mut cursor,
            leap.checked_mul(if version { 12 } else { 8 })?,
        )?;
        take(data, &mut cursor, standard)?;
        take(data, &mut cursor, utc)?;
        if transition_zones
            .iter()
            .any(|&index| usize::from(index) >= zones)
            || zone_data
                .chunks_exact(6)
                .any(|zone| usize::from(zone[5]) >= abbreviations.len())
        {
            return None;
        }
        Some(())
    }
    parse(data).is_some()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timezone_names_and_non_zone_files() {
        for name in [
            "UTC",
            "Local",
            "Asia/Ho_Chi_Minh",
            "Etc/GMT+1",
            "./UTC",
            "GMT0",
            "Asia/Saigon",
        ] {
            assert!(valid(name), "{name}");
        }
        for name in [
            "../../etc/passwd",
            "Unknown/City",
            "/etc/localtime",
            "\\UTC",
            "zone.tab",
            "tzdata.zi",
        ] {
            assert!(!valid(name), "{name}");
        }
        let mut tzif = vec![0; 44];
        tzif[..4].copy_from_slice(b"TZif");
        tzif[39] = 1; // one zone
        tzif[43] = 1; // one abbreviation byte
        tzif.extend_from_slice(&[0, 0, 0, 0, 0, 0, 0]);
        assert!(valid_tzif(&tzif));
        tzif.truncate(46);
        assert!(!valid_tzif(&tzif));
    }

    #[test]
    fn custom_zoneinfo_zip() {
        let cases: Vec<serde_json::Value> = serde_json::from_str(include_str!(
            "../../tests/account_catalog_write_timezone_zip_oracle.json"
        ))
        .unwrap();
        let path = std::env::current_dir().unwrap().join(format!(
            "account-catalog-write-{}.zip",
            uuid::Uuid::new_v4()
        ));
        struct Cleanup(std::path::PathBuf);
        impl Drop for Cleanup {
            fn drop(&mut self) {
                let _ = std::fs::remove_file(&self.0);
            }
        }
        let _cleanup = Cleanup(path.clone());
        for case in cases {
            let archive = hex::decode(case["archive_hex"].as_str().unwrap()).unwrap();
            std::fs::write(&path, archive).unwrap();
            assert_eq!(
                valid_source(&path, case["name"].as_str().unwrap()),
                case["accepted"].as_bool().unwrap(),
                "{}",
                case["case"],
            );
        }
    }
}
