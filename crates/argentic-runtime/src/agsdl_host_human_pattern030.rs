//! Flagless ECMA-262 patterns for flat MCP forms, evaluated without backtracking.
use jsonschema::{paths::Location, Keyword, ValidationError};
use regex::{Regex, RegexBuilder};
use serde_json::{Map, Value};

struct Pattern(Regex);

pub(super) fn compile<'a>(
    _: &'a Map<String, Value>,
    value: &'a Value,
    _: Location,
) -> Result<Box<dyn Keyword>, ValidationError<'a>> {
    let pattern = value.as_str().expect("form pattern is a string");
    let matcher = translate(pattern).and_then(|translated| {
        RegexBuilder::new(&translated)
            .size_limit(256 * 1024)
            .dfa_size_limit(256 * 1024)
            .build()
            .ok()
    });
    matcher
        .map(|regex| Box::new(Pattern(regex)) as Box<dyn Keyword>)
        .ok_or_else(|| {
            ValidationError::custom(format!(
                "Unsupported or invalid MCP form pattern: {pattern}"
            ))
        })
}

impl Keyword for Pattern {
    fn validate<'a>(&self, instance: &'a Value) -> Result<(), ValidationError<'a>> {
        if !self.is_valid(instance) {
            return Err(ValidationError::custom(
                "String does not match the MCP form pattern",
            ));
        }
        Ok(())
    }

    fn is_valid(&self, instance: &Value) -> bool {
        instance.as_str().is_none_or(|text| {
            if !text.is_ascii() {
                let units: String = text.encode_utf16().map(regex_char).collect();
                self.0.is_match(&units)
            } else {
                self.0.is_match(text)
            }
        })
    }
}

// Keep all UTF-16 code units in order, skipping the surrogate gap in Rust's
// char representation. That preserves even ranges crossing the gap and lets
// a dot or quantifier count UTF-16 code units as ECMA-262 does without flags.
fn regex_char(unit: u16) -> char {
    let scalar = u32::from(unit) + if unit >= 0xd800 { 0x800 } else { 0 };
    char::from_u32(scalar).expect("mapped UTF-16 unit is a Unicode scalar")
}

// U+FEFF is U+106FF after the UTF-16 mapping.
const SPACE: &str = r"\t\n\x0B\x0C\r \x{A0}\x{1680}\x{2000}-\x{200A}\x{2028}\x{2029}\x{202F}\x{205F}\x{3000}\x{106FF}";

fn translate(pattern: &str) -> Option<String> {
    let mut units = pattern.encode_utf16().map(regex_char).peekable();
    let mut out = String::with_capacity(pattern.len());
    let mut in_class = false;
    let mut can_repeat = false;
    while let Some(ch) = units.next() {
        match ch {
            '\\' => {
                let escaped = units.next()?;
                let replacement = match escaped {
                    'b' if in_class => r"\x08",
                    'b' if !in_class => r"(?-u:\b)",
                    'B' if !in_class => r"(?-u:\B)",
                    'd' => "[0-9]",
                    'D' => "[^0-9]",
                    'w' => "[A-Za-z0-9_]",
                    'W' => "[^A-Za-z0-9_]",
                    's' => {
                        out.push('[');
                        out.push_str(SPACE);
                        "]"
                    }
                    'S' => {
                        out.push_str("[^");
                        out.push_str(SPACE);
                        "]"
                    }
                    'v' => r"\x0B",
                    '0' if !units.peek().is_some_and(char::is_ascii_digit) => r"\x00",
                    'x' | 'u' => {
                        let mut code = 0_u16;
                        for _ in 0..if escaped == 'x' { 2 } else { 4 } {
                            code = code * 16 + units.next()?.to_digit(16)? as u16;
                        }
                        use std::fmt::Write as _;
                        write!(out, "\\x{{{:x}}}", regex_char(code) as u32).ok()?;
                        ""
                    }
                    'n' | 'r' | 't' | 'f' | '^' | '$' | '\\' | '.' | '*' | '+' | '?' | '('
                    | ')' | '[' | ']' | '{' | '}' | '|' | '/' => {
                        out.push('\\');
                        out.push(escaped);
                        ""
                    }
                    '-' if in_class => r"\-",
                    _ => return None,
                };
                out.push_str(replacement);
                can_repeat = in_class || !matches!(escaped, 'b' | 'B');
            }
            '[' if !in_class => {
                out.push(ch);
                in_class = true;
            }
            '[' => return None,
            ']' if in_class => {
                out.push(ch);
                in_class = false;
                can_repeat = true;
            }
            ch if in_class => match ch {
                // Do not let Rust interpret ECMA-262 class literals as set operators.
                '&' => out.push_str(r"\x26"),
                '~' => out.push_str(r"\x7E"),
                '-' if units.peek() == Some(&'-') => return None,
                _ => out.push(ch),
            },
            '.' => {
                out.push_str(r"[^\r\n\x{2028}\x{2029}]");
                can_repeat = true;
            }
            '(' => {
                out.push(ch);
                if units.next_if_eq(&'?').is_some() {
                    units.next_if_eq(&':')?;
                    out.push_str("?:");
                }
                can_repeat = false;
            }
            '*' | '+' | '?' | '{' if can_repeat => {
                out.push(ch);
                if ch == '{' {
                    out.push(units.next_if(char::is_ascii_digit)?);
                    while let Some(digit) = units.next_if(char::is_ascii_digit) {
                        out.push(digit);
                    }
                    if units.next_if_eq(&',').is_some() {
                        out.push(',');
                        while let Some(digit) = units.next_if(char::is_ascii_digit) {
                            out.push(digit);
                        }
                    }
                    out.push(units.next_if_eq(&'}')?);
                }
                if units.next_if_eq(&'?').is_some() {
                    out.push('?');
                }
                can_repeat = false;
            }
            '*' | '+' | '?' | '{' | '}' | ']' => return None,
            _ => {
                out.push(ch);
                can_repeat = !matches!(ch, '^' | '$' | '|');
            }
        }
    }
    (!in_class).then_some(out)
}
