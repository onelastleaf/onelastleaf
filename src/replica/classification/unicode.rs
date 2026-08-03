use base64::{Engine as _, engine::general_purpose::STANDARD_NO_PAD};

pub(super) fn decode_utf32(bytes: &[u8], little_endian: bool) -> Option<String> {
    if !bytes.len().is_multiple_of(4) {
        return None;
    }
    bytes
        .as_chunks::<4>()
        .0
        .iter()
        .map(|unit| {
            let scalar = if little_endian {
                u32::from_le_bytes(*unit)
            } else {
                u32::from_be_bytes(*unit)
            };
            char::from_u32(scalar)
        })
        .collect()
}

pub(super) fn encode_utf32(text: &str, little_endian: bool) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(text.chars().count() * 4);
    for character in text.chars() {
        let scalar = u32::from(character);
        let encoded = if little_endian {
            scalar.to_le_bytes()
        } else {
            scalar.to_be_bytes()
        };
        bytes.extend_from_slice(&encoded);
    }
    bytes
}

pub(super) fn encode_utf16(text: &str, little_endian: bool) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(text.len() * 2);
    for unit in text.encode_utf16() {
        let encoded = if little_endian {
            unit.to_le_bytes()
        } else {
            unit.to_be_bytes()
        };
        bytes.extend_from_slice(&encoded);
    }
    bytes
}

pub(super) fn decode_utf7(bytes: &[u8]) -> Option<(String, bool)> {
    if bytes.iter().any(|byte| !byte.is_ascii()) {
        return None;
    }
    let mut text = String::new();
    let mut saw_non_ascii_shift = false;
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] != b'+' {
            let byte = bytes[index];
            if !(matches!(byte, b'\t' | b'\n' | b'\r' | 0x20..=0x7E)) {
                return None;
            }
            text.push(char::from(byte));
            index += 1;
            continue;
        }
        if bytes.get(index + 1) == Some(&b'-') {
            text.push('+');
            index += 2;
            continue;
        }
        let start = index + 1;
        let mut end = start;
        while bytes
            .get(end)
            .is_some_and(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'+' | b'/'))
        {
            end += 1;
        }
        if end == start {
            return None;
        }
        let utf16_bytes = STANDARD_NO_PAD.decode(&bytes[start..end]).ok()?;
        if !utf16_bytes.len().is_multiple_of(2) {
            return None;
        }
        let utf16 = utf16_bytes
            .as_chunks::<2>()
            .0
            .iter()
            .map(|unit| u16::from_be_bytes([unit[0], unit[1]]))
            .collect::<Vec<_>>();
        let shifted = String::from_utf16(&utf16).ok()?;
        saw_non_ascii_shift |= !shifted.is_ascii();
        text.push_str(&shifted);
        index = end + usize::from(bytes.get(end) == Some(&b'-'));
    }
    Some((text, saw_non_ascii_shift))
}

pub(super) fn encode_utf7(text: &str) -> Vec<u8> {
    fn direct(character: char) -> bool {
        character.is_ascii_alphanumeric()
            || matches!(
                character,
                '\t' | '\n' | '\r' | ' ' | '\'' | '(' | ')' | ',' | '-' | '.' | '/' | ':' | '?'
            )
    }

    fn flush_shifted(shifted: &mut Vec<u8>, output: &mut Vec<u8>) {
        if shifted.is_empty() {
            return;
        }
        output.push(b'+');
        output.extend_from_slice(STANDARD_NO_PAD.encode(&*shifted).as_bytes());
        output.push(b'-');
        shifted.clear();
    }

    let mut output = Vec::new();
    let mut shifted = Vec::new();
    for character in text.chars() {
        if character == '+' {
            flush_shifted(&mut shifted, &mut output);
            output.extend_from_slice(b"+-");
        } else if direct(character) {
            flush_shifted(&mut shifted, &mut output);
            output.push(character as u8);
        } else {
            for unit in character.encode_utf16(&mut [0; 2]) {
                shifted.extend_from_slice(&unit.to_be_bytes());
            }
        }
    }
    flush_shifted(&mut shifted, &mut output);
    output
}
