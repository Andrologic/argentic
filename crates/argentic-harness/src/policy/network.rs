//! Normalize network rules and destinations before matching host aliases.

use std::net::IpAddr;

pub fn normalize_network_host(host: &str) -> Option<String> {
    if let Ok(ip) = host.parse::<IpAddr>() {
        return Some(ip.to_string());
    }
    let ascii = idna::domain_to_ascii(host).ok()?;
    let ascii = ascii.strip_suffix('.').unwrap_or(&ascii);
    valid_domain(ascii, false).then(|| ascii.to_owned())
}

pub fn normalize_network_pattern(pattern: &str) -> Result<String, &'static str> {
    const INVALID: &str = "Expected a network host pattern without a scheme, port, or path";
    if !pattern.contains('*') {
        return normalize_network_host(pattern).ok_or(INVALID);
    }
    if pattern.bytes().filter(|&byte| byte == b':').count() >= 2
        && pattern
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() || b":.*".contains(&byte))
    {
        // IPv6 globs match textual aliases, including compressed spellings.
        return Ok(pattern.to_ascii_lowercase());
    }
    let ascii = idna::domain_to_ascii(pattern).map_err(|_| INVALID)?;
    let ascii = ascii.strip_suffix('.').unwrap_or(&ascii);
    // Punycode encodes whole labels. A glob within an IDN label has no
    // unambiguous ASCII spelling. Whole labels on either side are fine.
    let ambiguous = pattern
        .split(['.', '\u{3002}', '\u{ff0e}', '\u{ff61}'])
        .any(|label| label.contains('*') && !label.is_ascii());
    if ambiguous || !valid_domain(ascii, true) {
        return Err(INVALID);
    }
    Ok(ascii.to_owned())
}

fn valid_domain(host: &str, wildcard: bool) -> bool {
    host.len() <= 253
        && host.split('.').all(|label| {
            !label.is_empty()
                && label.len() <= 63
                && !label.starts_with('-')
                && !label.ends_with('-')
                && label.bytes().all(|byte| {
                    byte.is_ascii_alphanumeric() || byte == b'-' || (wildcard && byte == b'*')
                })
        })
}

pub fn matches_ipv6_spelling(pattern: &str, ip: std::net::Ipv6Addr) -> bool {
    // Domain-only rules cannot match an IPv6 spelling. Adjacent stars are
    // equivalent to one star, allowing a single epsilon transition per state.
    if !pattern.contains(':') && !pattern.contains('*') {
        return false;
    }
    if !pattern
        .bytes()
        .all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f' | b':' | b'.' | b'*'))
    {
        return false;
    }
    let mut compact = Vec::with_capacity(pattern.len());
    for byte in pattern.bytes() {
        if byte != b'*' || compact.last() != Some(&b'*') {
            compact.push(byte);
        }
    }
    ipv6_glob(&compact, ip)
}

fn glob_advance(pattern: &[u8], states: &[usize], value: &[u8]) -> Vec<usize> {
    fn closure(pattern: &[u8], states: &mut Vec<usize>) {
        let skips: Vec<_> = states
            .iter()
            .copied()
            .filter(|&state| pattern.get(state) == Some(&b'*'))
            .map(|state| state + 1)
            .collect();
        states.extend(skips);
        states.sort_unstable();
        states.dedup();
    }
    let mut states = states.to_vec();
    closure(pattern, &mut states);
    for &byte in value {
        let mut next = Vec::new();
        for &state in &states {
            match pattern.get(state) {
                Some(b'*') => next.push(state),
                Some(&literal) if literal == byte => next.push(state + 1),
                _ => {}
            }
        }
        next.sort_unstable();
        next.dedup();
        closure(pattern, &mut next);
        states = next;
        if states.is_empty() {
            break;
        }
    }
    states
}

fn ipv6_form_matches(
    pattern: &[u8],
    groups: &[u16],
    compressed: Option<(usize, usize)>,
    dotted: Option<&str>,
) -> bool {
    let mut states = glob_advance(pattern, &[0], b"");
    let mut group = 0;
    let mut previous_compressed = false;
    while group < groups.len() {
        if let Some((start, end)) = compressed {
            if group == start {
                states = glob_advance(pattern, &states, b"::");
                group = end;
                previous_compressed = true;
                continue;
            }
        }
        if group > 0 && !previous_compressed {
            states = glob_advance(pattern, &states, b":");
        }
        previous_compressed = false;
        let width = format!("{:x}", groups[group]).len();
        let mut combined = Vec::new();
        for width in width..=4 {
            let text = format!("{:0width$x}", groups[group], width = width);
            combined.extend(glob_advance(pattern, &states, text.as_bytes()));
        }
        combined.sort_unstable();
        combined.dedup();
        states = combined;
        group += 1;
    }
    if let Some(dotted) = dotted {
        if !previous_compressed {
            states = glob_advance(pattern, &states, b":");
        }
        states = glob_advance(pattern, &states, dotted.as_bytes());
    }
    states.contains(&pattern.len())
}

fn ipv6_glob(pattern: &[u8], ip: std::net::Ipv6Addr) -> bool {
    let groups = ip.segments();
    let octets = ip.octets();
    let dotted = format!(
        "{}.{}.{}.{}",
        octets[12], octets[13], octets[14], octets[15]
    );
    for (groups, tail) in [(&groups[..], None), (&groups[..6], Some(dotted.as_str()))] {
        if ipv6_form_matches(pattern, groups, None, tail) {
            return true;
        }
        for start in 0..groups.len() {
            for end in start + 1..=groups.len() {
                if groups[end - 1] != 0 {
                    break;
                }
                if ipv6_form_matches(pattern, groups, Some((start, end)), tail) {
                    return true;
                }
            }
        }
    }
    false
}
