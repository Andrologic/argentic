//! Match textual IPv6 aliases without enumerating all padded strings.

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
