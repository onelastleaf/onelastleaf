use std::{fs, path::Path};

use base64::{Engine as _, engine::general_purpose::STANDARD_NO_PAD};
use chardetng::{EncodingDetector, Iso2022JpDetection, Utf8Detection};
use encoding_rs::{
    Encoding, GB18030, ISO_8859_2, ISO_8859_15, KOI8_R, KOI8_U, MACINTOSH, UTF_8, UTF_16BE,
    UTF_16LE, WINDOWS_1250, WINDOWS_1252,
};
use infer::MatcherType;
use sha2::{Digest, Sha256};

use super::ReplicaError;

#[derive(Clone, Debug)]
pub enum ClassifiedFile {
    Text(DecodedText),
    Binary(BinaryFile),
}

#[derive(Clone, Debug)]
pub struct DecodedText {
    pub text: String,
    pub encoding: String,
    pub has_byte_order_mark: bool,
    pub media_type: String,
    pub size_bytes: u64,
}

#[derive(Clone, Debug)]
pub struct BinaryFile {
    pub bytes: Vec<u8>,
    pub sha256: String,
    pub media_type: String,
}

#[derive(Clone, Copy)]
enum TextEncoding {
    EncodingRs(&'static Encoding),
    Utf32Le,
    Utf32Be,
    Utf7,
    UtfEbcdic,
    Ibm037,
}

impl TextEncoding {
    fn for_label(label: &str) -> Option<Self> {
        let label = label.trim();
        if label.eq_ignore_ascii_case("utf-32le") || label.eq_ignore_ascii_case("utf32le") {
            Some(Self::Utf32Le)
        } else if label.eq_ignore_ascii_case("utf-32be") || label.eq_ignore_ascii_case("utf32be") {
            Some(Self::Utf32Be)
        } else if label.eq_ignore_ascii_case("utf-7") || label.eq_ignore_ascii_case("utf7") {
            Some(Self::Utf7)
        } else if label.eq_ignore_ascii_case("utf-ebcdic") {
            Some(Self::UtfEbcdic)
        } else if label.eq_ignore_ascii_case("ibm037")
            || label.eq_ignore_ascii_case("ibm-037")
            || label.eq_ignore_ascii_case("cp037")
            || label.eq_ignore_ascii_case("ebcdic-cp-us")
            || label.eq_ignore_ascii_case("ebcdic-us")
        {
            Some(Self::Ibm037)
        } else {
            Encoding::for_label(label.as_bytes()).map(Self::EncodingRs)
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::EncodingRs(encoding) => encoding.name(),
            Self::Utf32Le => "UTF-32LE",
            Self::Utf32Be => "UTF-32BE",
            Self::Utf7 => "UTF-7",
            Self::UtfEbcdic => "UTF-EBCDIC",
            Self::Ibm037 => "IBM037",
        }
    }

    fn decode(self, bytes: &[u8]) -> Option<String> {
        match self {
            Self::EncodingRs(encoding) => encoding
                .decode_without_bom_handling_and_without_replacement(bytes)
                .map(|text| text.into_owned()),
            Self::Utf32Le => decode_utf32(bytes, true),
            Self::Utf32Be => decode_utf32(bytes, false),
            Self::Utf7 => decode_utf7(bytes).map(|(text, _)| text),
            Self::UtfEbcdic => decode_utf_ebcdic(bytes).map(|(text, _)| text),
            Self::Ibm037 => Some(decode_ibm037(bytes)),
        }
    }

    fn encode(self, text: &str) -> Option<Vec<u8>> {
        match self {
            Self::EncodingRs(encoding) => {
                if encoding == UTF_16LE {
                    return Some(encode_utf16(text, true));
                }
                if encoding == UTF_16BE {
                    return Some(encode_utf16(text, false));
                }
                let (bytes, _, had_errors) = encoding.encode(text);
                (!had_errors).then(|| bytes.into_owned())
            }
            Self::Utf32Le => Some(encode_utf32(text, true)),
            Self::Utf32Be => Some(encode_utf32(text, false)),
            Self::Utf7 => Some(encode_utf7(text)),
            Self::UtfEbcdic => Some(encode_utf_ebcdic(text)),
            Self::Ibm037 => encode_ibm037(text),
        }
    }

    fn bom(self) -> Option<&'static [u8]> {
        match self {
            Self::EncodingRs(encoding) if encoding == UTF_8 => Some(&[0xEF, 0xBB, 0xBF]),
            Self::EncodingRs(encoding) if encoding == UTF_16LE => Some(&[0xFF, 0xFE]),
            Self::EncodingRs(encoding) if encoding == UTF_16BE => Some(&[0xFE, 0xFF]),
            Self::Utf32Le => Some(&[0xFF, 0xFE, 0x00, 0x00]),
            Self::Utf32Be => Some(&[0x00, 0x00, 0xFE, 0xFF]),
            Self::Utf7 => Some(b"+/v8-"),
            Self::UtfEbcdic => Some(&[0xDD, 0x73, 0x66, 0x73]),
            _ => None,
        }
    }
}

pub fn classify_path(path: &Path) -> Result<ClassifiedFile, ReplicaError> {
    let bytes =
        fs::read(path).map_err(|error| ReplicaError::io("read working-tree file", error))?;
    classify_bytes(bytes)
}

pub fn classify_bytes(bytes: Vec<u8>) -> Result<ClassifiedFile, ReplicaError> {
    let inferred = infer::get(&bytes);
    if let Some(kind) = inferred
        && kind.matcher_type() != MatcherType::Text
    {
        return Ok(binary(bytes, kind.mime_type().to_owned()));
    }

    let size_bytes = u64::try_from(bytes.len())
        .map_err(|_| ReplicaError::InvalidArgument("file size does not fit in u64".to_owned()))?;
    let decoded = decode_bom_text(&bytes)
        .or_else(|| decode_bomless_unicode(&bytes))
        .or_else(|| decode_legacy_text(&bytes));

    if let Some((text, encoding, has_byte_order_mark)) = decoded {
        let inferred_media_type = inferred
            .filter(|kind| kind.matcher_type() == MatcherType::Text)
            .map(|kind| kind.mime_type());
        return Ok(ClassifiedFile::Text(DecodedText {
            media_type: text_media_type(&text, inferred_media_type),
            text,
            encoding: encoding.name().to_owned(),
            has_byte_order_mark,
            size_bytes,
        }));
    }

    Ok(binary(bytes, "application/octet-stream".to_owned()))
}

pub(crate) fn is_supported_text_encoding(encoding_name: &str) -> bool {
    TextEncoding::for_label(encoding_name).is_some()
}

pub fn encode_text(
    text: &str,
    encoding_name: &str,
    has_byte_order_mark: bool,
) -> Result<(Vec<u8>, bool), ReplicaError> {
    let encoding = TextEncoding::for_label(encoding_name).ok_or_else(|| {
        ReplicaError::CorruptStore("document has an unknown text encoding".to_owned())
    })?;
    let Some(mut bytes) = encoding.encode(text) else {
        return Ok((text.as_bytes().to_vec(), true));
    };
    if has_byte_order_mark {
        let bom = encoding.bom().ok_or_else(|| {
            ReplicaError::CorruptStore(
                "document declares a BOM for an encoding without a supported BOM".to_owned(),
            )
        })?;
        let mut with_bom = Vec::with_capacity(bom.len() + bytes.len());
        with_bom.extend_from_slice(bom);
        with_bom.append(&mut bytes);
        bytes = with_bom;
    }
    Ok((bytes, false))
}

fn decode_bom_text(bytes: &[u8]) -> Option<(String, TextEncoding, bool)> {
    let signatures: &[(&[u8], TextEncoding)] = &[
        (&[0xFF, 0xFE, 0x00, 0x00], TextEncoding::Utf32Le),
        (&[0x00, 0x00, 0xFE, 0xFF], TextEncoding::Utf32Be),
        (&[0xEF, 0xBB, 0xBF], TextEncoding::EncodingRs(UTF_8)),
        (&[0xFF, 0xFE], TextEncoding::EncodingRs(UTF_16LE)),
        (&[0xFE, 0xFF], TextEncoding::EncodingRs(UTF_16BE)),
        (b"+/v8-", TextEncoding::Utf7),
        (&[0xDD, 0x73, 0x66, 0x73], TextEncoding::UtfEbcdic),
    ];
    signatures.iter().find_map(|(bom, encoding)| {
        let payload = bytes.strip_prefix(*bom)?;
        decoded_candidate(*encoding, payload, true)
    })
}

fn decode_bomless_unicode(bytes: &[u8]) -> Option<(String, TextEncoding, bool)> {
    for encoding in [TextEncoding::Utf32Le, TextEncoding::Utf32Be] {
        if looks_like_bomless_utf32(bytes, matches!(encoding, TextEncoding::Utf32Le))
            && let Some(candidate) = decoded_candidate(encoding, bytes, false)
        {
            return Some(candidate);
        }
    }
    for encoding in [
        TextEncoding::EncodingRs(UTF_16LE),
        TextEncoding::EncodingRs(UTF_16BE),
    ] {
        if looks_like_bomless_utf16(
            bytes,
            matches!(encoding, TextEncoding::EncodingRs(value) if value == UTF_16LE),
        ) && let Some(candidate) = decoded_candidate(encoding, bytes, false)
        {
            return Some(candidate);
        }
    }

    if let Some((text, saw_non_ascii_shift)) = decode_utf7(bytes)
        && saw_non_ascii_shift
        && is_plausible_text(&text)
        && TextEncoding::Utf7.encode(&text).as_deref() == Some(bytes)
    {
        return Some((text, TextEncoding::Utf7, false));
    }
    if let Some((text, saw_multibyte)) = decode_utf_ebcdic(bytes)
        && saw_multibyte
        && is_plausible_text(&text)
        && TextEncoding::UtfEbcdic.encode(&text).as_deref() == Some(bytes)
    {
        return Some((text, TextEncoding::UtfEbcdic, false));
    }
    if let Ok(text) = std::str::from_utf8(bytes)
        && is_plausible_text(text)
    {
        return Some((text.to_owned(), TextEncoding::EncodingRs(UTF_8), false));
    }
    None
}

fn decode_legacy_text(bytes: &[u8]) -> Option<(String, TextEncoding, bool)> {
    if contains_gb18030_four_byte_sequence(bytes)
        && let Some(candidate) = decoded_candidate(TextEncoding::EncodingRs(GB18030), bytes, false)
    {
        return Some(candidate);
    }

    let mut detector = EncodingDetector::new(Iso2022JpDetection::Allow);
    detector.feed(bytes, true);
    let mut detected_encoding = detector.guess(None, Utf8Detection::Allow);
    if detected_encoding == KOI8_U
        && !bytes
            .iter()
            .any(|byte| matches!(byte, 0xA4 | 0xA6 | 0xA7 | 0xAD | 0xB4 | 0xB6 | 0xB7 | 0xBD))
    {
        detected_encoding = KOI8_R;
    }
    let detected = decoded_candidate(TextEncoding::EncodingRs(detected_encoding), bytes, false);

    let western_or_central = detected_encoding == WINDOWS_1252
        || detected_encoding == WINDOWS_1250
        || detected_encoding == ISO_8859_2;
    let iso_8859_15 = (western_or_central && looks_like_iso_8859_15(bytes))
        .then(|| decoded_candidate(TextEncoding::EncodingRs(ISO_8859_15), bytes, false))
        .flatten();
    let macintosh = (detected_encoding == WINDOWS_1252 && looks_like_macroman(bytes))
        .then(|| decoded_candidate(TextEncoding::EncodingRs(MACINTOSH), bytes, false))
        .flatten();
    let ebcdic = looks_like_ebcdic(bytes)
        .then(|| decoded_candidate(TextEncoding::Ibm037, bytes, false))
        .flatten();

    if let Some(candidate) = iso_8859_15.or(macintosh) {
        return Some(candidate);
    }
    if let Some(candidate) = ebcdic {
        let detected_score = detected
            .as_ref()
            .map(|current| text_score(&current.0))
            .unwrap_or(i64::MIN);
        if text_score(&candidate.0) >= detected_score {
            return Some(candidate);
        }
    }
    detected
}

fn decoded_candidate(
    encoding: TextEncoding,
    bytes: &[u8],
    has_byte_order_mark: bool,
) -> Option<(String, TextEncoding, bool)> {
    let text = encoding.decode(bytes)?;
    if !is_plausible_text(&text) || encoding.encode(&text).as_deref() != Some(bytes) {
        return None;
    }
    Some((text, encoding, has_byte_order_mark))
}

fn text_media_type(text: &str, inferred: Option<&str>) -> String {
    inferred
        .or_else(|| {
            infer::get(text.as_bytes())
                .filter(|kind| kind.matcher_type() == MatcherType::Text)
                .map(|kind| kind.mime_type())
        })
        .unwrap_or("text/plain")
        .to_owned()
}

fn is_plausible_text(text: &str) -> bool {
    if text.contains('\0') {
        return false;
    }
    let mut total = 0_usize;
    let mut forbidden_controls = 0_usize;
    for character in text.chars() {
        total += 1;
        if character.is_control()
            && !matches!(character, '\t' | '\n' | '\r' | '\u{000C}' | '\u{0085}')
        {
            forbidden_controls += 1;
        }
    }
    forbidden_controls == 0 || (total >= 100 && forbidden_controls * 100 <= total)
}

fn text_score(text: &str) -> i64 {
    text.chars()
        .map(|character| {
            if character.is_alphanumeric() {
                5
            } else if character.is_whitespace() {
                4
            } else if character.is_control() {
                -20
            } else if character.is_ascii_punctuation() {
                2
            } else {
                3
            }
        })
        .sum()
}

fn looks_like_bomless_utf16(bytes: &[u8], little_endian: bool) -> bool {
    if bytes.len() < 4 || !bytes.len().is_multiple_of(2) {
        return false;
    }
    let units = bytes.len() / 2;
    let high_lane = usize::from(little_endian);
    let byte_units = bytes.as_chunks::<2>().0;
    let high_zeroes = byte_units
        .iter()
        .filter(|unit| unit[high_lane] == 0)
        .count();
    let low_zeroes = byte_units
        .iter()
        .filter(|unit| unit[1 - high_lane] == 0)
        .count();
    high_zeroes * 4 >= units * 3 && low_zeroes * 4 <= units
}

fn looks_like_bomless_utf32(bytes: &[u8], little_endian: bool) -> bool {
    if bytes.len() < 8 || !bytes.len().is_multiple_of(4) {
        return false;
    }
    let units = bytes.len() / 4;
    let high_zeroes = bytes
        .as_chunks::<4>()
        .0
        .iter()
        .map(|unit| {
            if little_endian {
                usize::from(unit[2] == 0) + usize::from(unit[3] == 0)
            } else {
                usize::from(unit[0] == 0) + usize::from(unit[1] == 0)
            }
        })
        .sum::<usize>();
    high_zeroes * 4 >= units * 2 * 3
}

fn contains_gb18030_four_byte_sequence(bytes: &[u8]) -> bool {
    bytes.windows(4).any(|sequence| {
        matches!(sequence[0], 0x81..=0xFE)
            && sequence[1].is_ascii_digit()
            && matches!(sequence[2], 0x81..=0xFE)
            && sequence[3].is_ascii_digit()
    })
}

fn looks_like_iso_8859_15(bytes: &[u8]) -> bool {
    bytes
        .iter()
        .filter(|byte| matches!(byte, 0xA4 | 0xA6 | 0xA8 | 0xB4 | 0xB8 | 0xBC | 0xBD | 0xBE))
        .count()
        >= 2
}

fn looks_like_macroman(bytes: &[u8]) -> bool {
    bytes
        .iter()
        .filter(|byte| matches!(byte, 0x81 | 0x8D | 0x8F | 0x90 | 0x9D))
        .count()
        >= 2
}

fn looks_like_ebcdic(bytes: &[u8]) -> bool {
    if bytes.len() < 4 {
        return false;
    }
    let alphanumeric = bytes
        .iter()
        .filter(|byte| {
            matches!(
                byte,
                0x81..=0x89
                    | 0x91..=0x99
                    | 0xA2..=0xA9
                    | 0xC1..=0xC9
                    | 0xD1..=0xD9
                    | 0xE2..=0xE9
                    | 0xF0..=0xF9
            )
        })
        .count();
    let structural = bytes
        .iter()
        .filter(|byte| {
            matches!(
                byte,
                0x40
                    | 0x15
                    | 0x25
                    | 0x4B..=0x50
                    | 0x5A..=0x61
                    | 0x6B..=0x7F
                    | 0x81..=0x89
                    | 0x91..=0x99
                    | 0xA2..=0xA9
                    | 0xC1..=0xC9
                    | 0xD1..=0xD9
                    | 0xE2..=0xE9
                    | 0xF0..=0xF9
            )
        })
        .count();
    alphanumeric >= 3 && structural * 5 >= bytes.len() * 3
}

fn decode_utf32(bytes: &[u8], little_endian: bool) -> Option<String> {
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

fn encode_utf32(text: &str, little_endian: bool) -> Vec<u8> {
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

fn encode_utf16(text: &str, little_endian: bool) -> Vec<u8> {
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

fn decode_utf7(bytes: &[u8]) -> Option<(String, bool)> {
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

fn encode_utf7(text: &str) -> Vec<u8> {
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

fn decode_utf_ebcdic(bytes: &[u8]) -> Option<(String, bool)> {
    let i8_bytes = bytes
        .iter()
        .map(|byte| UTF_EBCDIC_TO_I8[*byte as usize])
        .collect::<Vec<_>>();
    let mut text = String::new();
    let mut saw_multibyte = false;
    let mut index = 0;
    while index < i8_bytes.len() {
        let first = i8_bytes[index];
        let (length, mut scalar) = match first {
            0x00..=0x9F => (1, u32::from(first)),
            0xC5..=0xDF => (2, u32::from(first & 0x1F)),
            0xE1..=0xEF => (3, u32::from(first & 0x0F)),
            0xF0..=0xF7 => (4, u32::from(first & 0x07)),
            0xF8..=0xF9 => (5, u32::from(first & 0x01)),
            _ => return None,
        };
        if index + length > i8_bytes.len() {
            return None;
        }
        for trailing in &i8_bytes[index + 1..index + length] {
            if !matches!(trailing, 0xA0..=0xBF) {
                return None;
            }
            scalar = (scalar << 5) | u32::from(trailing & 0x1F);
        }
        let minimum = match length {
            1 => 0,
            2 => 0xA0,
            3 => 0x400,
            4 => 0x4000,
            5 => 0x40000,
            _ => unreachable!(),
        };
        if scalar < minimum {
            return None;
        }
        text.push(char::from_u32(scalar)?);
        saw_multibyte |= length > 1;
        index += length;
    }
    Some((text, saw_multibyte))
}

fn encode_utf_ebcdic(text: &str) -> Vec<u8> {
    let mut bytes = Vec::new();
    for character in text.chars() {
        let scalar = u32::from(character);
        let mut i8 = [0_u8; 5];
        let encoded: &[u8] = if scalar <= 0x9F {
            i8[0] = scalar as u8;
            &i8[..1]
        } else if scalar <= 0x3FF {
            i8[0] = 0xC0 | ((scalar >> 5) as u8);
            i8[1] = 0xA0 | ((scalar & 0x1F) as u8);
            &i8[..2]
        } else if scalar <= 0x3FFF {
            i8[0] = 0xE0 | ((scalar >> 10) as u8);
            i8[1] = 0xA0 | (((scalar >> 5) & 0x1F) as u8);
            i8[2] = 0xA0 | ((scalar & 0x1F) as u8);
            &i8[..3]
        } else if scalar <= 0x3FFFF {
            i8[0] = 0xF0 | ((scalar >> 15) as u8);
            i8[1] = 0xA0 | (((scalar >> 10) & 0x1F) as u8);
            i8[2] = 0xA0 | (((scalar >> 5) & 0x1F) as u8);
            i8[3] = 0xA0 | ((scalar & 0x1F) as u8);
            &i8[..4]
        } else {
            i8[0] = 0xF8 | ((scalar >> 20) as u8);
            i8[1] = 0xA0 | (((scalar >> 15) & 0x1F) as u8);
            i8[2] = 0xA0 | (((scalar >> 10) & 0x1F) as u8);
            i8[3] = 0xA0 | (((scalar >> 5) & 0x1F) as u8);
            i8[4] = 0xA0 | ((scalar & 0x1F) as u8);
            &i8[..5]
        };
        bytes.extend(encoded.iter().map(|byte| I8_TO_UTF_EBCDIC[*byte as usize]));
    }
    bytes
}

fn decode_ibm037(bytes: &[u8]) -> String {
    bytes
        .iter()
        .map(|byte| char::from(IBM037_TO_LATIN1[*byte as usize]))
        .collect()
}

fn encode_ibm037(text: &str) -> Option<Vec<u8>> {
    text.chars()
        .map(|character| {
            let scalar = u32::from(character);
            (scalar <= 0xFF).then(|| LATIN1_TO_IBM037[scalar as usize])
        })
        .collect()
}

fn binary(bytes: Vec<u8>, media_type: String) -> ClassifiedFile {
    let digest = Sha256::digest(&bytes);
    let sha256 = super::lower_hex(&digest);
    ClassifiedFile::Binary(BinaryFile {
        bytes,
        sha256,
        media_type,
    })
}

// UTR #16 tables 2 and 3: the normative reversible I8/UTF-EBCDIC byte map.
const I8_TO_UTF_EBCDIC: [u8; 256] = [
    0x00, 0x01, 0x02, 0x03, 0x37, 0x2D, 0x2E, 0x2F, 0x16, 0x05, 0x15, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F,
    0x10, 0x11, 0x12, 0x13, 0x3C, 0x3D, 0x32, 0x26, 0x18, 0x19, 0x3F, 0x27, 0x1C, 0x1D, 0x1E, 0x1F,
    0x40, 0x5A, 0x7F, 0x7B, 0x5B, 0x6C, 0x50, 0x7D, 0x4D, 0x5D, 0x5C, 0x4E, 0x6B, 0x60, 0x4B, 0x61,
    0xF0, 0xF1, 0xF2, 0xF3, 0xF4, 0xF5, 0xF6, 0xF7, 0xF8, 0xF9, 0x7A, 0x5E, 0x4C, 0x7E, 0x6E, 0x6F,
    0x7C, 0xC1, 0xC2, 0xC3, 0xC4, 0xC5, 0xC6, 0xC7, 0xC8, 0xC9, 0xD1, 0xD2, 0xD3, 0xD4, 0xD5, 0xD6,
    0xD7, 0xD8, 0xD9, 0xE2, 0xE3, 0xE4, 0xE5, 0xE6, 0xE7, 0xE8, 0xE9, 0xAD, 0xE0, 0xBD, 0x5F, 0x6D,
    0x79, 0x81, 0x82, 0x83, 0x84, 0x85, 0x86, 0x87, 0x88, 0x89, 0x91, 0x92, 0x93, 0x94, 0x95, 0x96,
    0x97, 0x98, 0x99, 0xA2, 0xA3, 0xA4, 0xA5, 0xA6, 0xA7, 0xA8, 0xA9, 0xC0, 0x4F, 0xD0, 0xA1, 0x07,
    0x20, 0x21, 0x22, 0x23, 0x24, 0x25, 0x06, 0x17, 0x28, 0x29, 0x2A, 0x2B, 0x2C, 0x09, 0x0A, 0x1B,
    0x30, 0x31, 0x1A, 0x33, 0x34, 0x35, 0x36, 0x08, 0x38, 0x39, 0x3A, 0x3B, 0x04, 0x14, 0x3E, 0xFF,
    0x41, 0x42, 0x43, 0x44, 0x45, 0x46, 0x47, 0x48, 0x49, 0x4A, 0x51, 0x52, 0x53, 0x54, 0x55, 0x56,
    0x57, 0x58, 0x59, 0x62, 0x63, 0x64, 0x65, 0x66, 0x67, 0x68, 0x69, 0x6A, 0x70, 0x71, 0x72, 0x73,
    0x74, 0x75, 0x76, 0x77, 0x78, 0x80, 0x8A, 0x8B, 0x8C, 0x8D, 0x8E, 0x8F, 0x90, 0x9A, 0x9B, 0x9C,
    0x9D, 0x9E, 0x9F, 0xA0, 0xAA, 0xAB, 0xAC, 0xAE, 0xAF, 0xB0, 0xB1, 0xB2, 0xB3, 0xB4, 0xB5, 0xB6,
    0xB7, 0xB8, 0xB9, 0xBA, 0xBB, 0xBC, 0xBE, 0xBF, 0xCA, 0xCB, 0xCC, 0xCD, 0xCE, 0xCF, 0xDA, 0xDB,
    0xDC, 0xDD, 0xDE, 0xDF, 0xE1, 0xEA, 0xEB, 0xEC, 0xED, 0xEE, 0xEF, 0xFA, 0xFB, 0xFC, 0xFD, 0xFE,
];

const UTF_EBCDIC_TO_I8: [u8; 256] = [
    0x00, 0x01, 0x02, 0x03, 0x9C, 0x09, 0x86, 0x7F, 0x97, 0x8D, 0x8E, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F,
    0x10, 0x11, 0x12, 0x13, 0x9D, 0x0A, 0x08, 0x87, 0x18, 0x19, 0x92, 0x8F, 0x1C, 0x1D, 0x1E, 0x1F,
    0x80, 0x81, 0x82, 0x83, 0x84, 0x85, 0x17, 0x1B, 0x88, 0x89, 0x8A, 0x8B, 0x8C, 0x05, 0x06, 0x07,
    0x90, 0x91, 0x16, 0x93, 0x94, 0x95, 0x96, 0x04, 0x98, 0x99, 0x9A, 0x9B, 0x14, 0x15, 0x9E, 0x1A,
    0x20, 0xA0, 0xA1, 0xA2, 0xA3, 0xA4, 0xA5, 0xA6, 0xA7, 0xA8, 0xA9, 0x2E, 0x3C, 0x28, 0x2B, 0x7C,
    0x26, 0xAA, 0xAB, 0xAC, 0xAD, 0xAE, 0xAF, 0xB0, 0xB1, 0xB2, 0x21, 0x24, 0x2A, 0x29, 0x3B, 0x5E,
    0x2D, 0x2F, 0xB3, 0xB4, 0xB5, 0xB6, 0xB7, 0xB8, 0xB9, 0xBA, 0xBB, 0x2C, 0x25, 0x5F, 0x3E, 0x3F,
    0xBC, 0xBD, 0xBE, 0xBF, 0xC0, 0xC1, 0xC2, 0xC3, 0xC4, 0x60, 0x3A, 0x23, 0x40, 0x27, 0x3D, 0x22,
    0xC5, 0x61, 0x62, 0x63, 0x64, 0x65, 0x66, 0x67, 0x68, 0x69, 0xC6, 0xC7, 0xC8, 0xC9, 0xCA, 0xCB,
    0xCC, 0x6A, 0x6B, 0x6C, 0x6D, 0x6E, 0x6F, 0x70, 0x71, 0x72, 0xCD, 0xCE, 0xCF, 0xD0, 0xD1, 0xD2,
    0xD3, 0x7E, 0x73, 0x74, 0x75, 0x76, 0x77, 0x78, 0x79, 0x7A, 0xD4, 0xD5, 0xD6, 0x5B, 0xD7, 0xD8,
    0xD9, 0xDA, 0xDB, 0xDC, 0xDD, 0xDE, 0xDF, 0xE0, 0xE1, 0xE2, 0xE3, 0xE4, 0xE5, 0x5D, 0xE6, 0xE7,
    0x7B, 0x41, 0x42, 0x43, 0x44, 0x45, 0x46, 0x47, 0x48, 0x49, 0xE8, 0xE9, 0xEA, 0xEB, 0xEC, 0xED,
    0x7D, 0x4A, 0x4B, 0x4C, 0x4D, 0x4E, 0x4F, 0x50, 0x51, 0x52, 0xEE, 0xEF, 0xF0, 0xF1, 0xF2, 0xF3,
    0x5C, 0xF4, 0x53, 0x54, 0x55, 0x56, 0x57, 0x58, 0x59, 0x5A, 0xF5, 0xF6, 0xF7, 0xF8, 0xF9, 0xFA,
    0x30, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38, 0x39, 0xFB, 0xFC, 0xFD, 0xFE, 0xFF, 0x9F,
];

// IBM code page 037 is a bijection over ISO-8859-1 scalar values.
const IBM037_TO_LATIN1: [u8; 256] = [
    0x00, 0x01, 0x02, 0x03, 0x9C, 0x09, 0x86, 0x7F, 0x97, 0x8D, 0x8E, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F,
    0x10, 0x11, 0x12, 0x13, 0x9D, 0x85, 0x08, 0x87, 0x18, 0x19, 0x92, 0x8F, 0x1C, 0x1D, 0x1E, 0x1F,
    0x80, 0x81, 0x82, 0x83, 0x84, 0x0A, 0x17, 0x1B, 0x88, 0x89, 0x8A, 0x8B, 0x8C, 0x05, 0x06, 0x07,
    0x90, 0x91, 0x16, 0x93, 0x94, 0x95, 0x96, 0x04, 0x98, 0x99, 0x9A, 0x9B, 0x14, 0x15, 0x9E, 0x1A,
    0x20, 0xA0, 0xE2, 0xE4, 0xE0, 0xE1, 0xE3, 0xE5, 0xE7, 0xF1, 0xA2, 0x2E, 0x3C, 0x28, 0x2B, 0x7C,
    0x26, 0xE9, 0xEA, 0xEB, 0xE8, 0xED, 0xEE, 0xEF, 0xEC, 0xDF, 0x21, 0x24, 0x2A, 0x29, 0x3B, 0xAC,
    0x2D, 0x2F, 0xC2, 0xC4, 0xC0, 0xC1, 0xC3, 0xC5, 0xC7, 0xD1, 0xA6, 0x2C, 0x25, 0x5F, 0x3E, 0x3F,
    0xF8, 0xC9, 0xCA, 0xCB, 0xC8, 0xCD, 0xCE, 0xCF, 0xCC, 0x60, 0x3A, 0x23, 0x40, 0x27, 0x3D, 0x22,
    0xD8, 0x61, 0x62, 0x63, 0x64, 0x65, 0x66, 0x67, 0x68, 0x69, 0xAB, 0xBB, 0xF0, 0xFD, 0xFE, 0xB1,
    0xB0, 0x6A, 0x6B, 0x6C, 0x6D, 0x6E, 0x6F, 0x70, 0x71, 0x72, 0xAA, 0xBA, 0xE6, 0xB8, 0xC6, 0xA4,
    0xB5, 0x7E, 0x73, 0x74, 0x75, 0x76, 0x77, 0x78, 0x79, 0x7A, 0xA1, 0xBF, 0xD0, 0xDD, 0xDE, 0xAE,
    0x5E, 0xA3, 0xA5, 0xB7, 0xA9, 0xA7, 0xB6, 0xBC, 0xBD, 0xBE, 0x5B, 0x5D, 0xAF, 0xA8, 0xB4, 0xD7,
    0x7B, 0x41, 0x42, 0x43, 0x44, 0x45, 0x46, 0x47, 0x48, 0x49, 0xAD, 0xF4, 0xF6, 0xF2, 0xF3, 0xF5,
    0x7D, 0x4A, 0x4B, 0x4C, 0x4D, 0x4E, 0x4F, 0x50, 0x51, 0x52, 0xB9, 0xFB, 0xFC, 0xF9, 0xFA, 0xFF,
    0x5C, 0xF7, 0x53, 0x54, 0x55, 0x56, 0x57, 0x58, 0x59, 0x5A, 0xB2, 0xD4, 0xD6, 0xD2, 0xD3, 0xD5,
    0x30, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38, 0x39, 0xB3, 0xDB, 0xDC, 0xD9, 0xDA, 0x9F,
];

const LATIN1_TO_IBM037: [u8; 256] = [
    0x00, 0x01, 0x02, 0x03, 0x37, 0x2D, 0x2E, 0x2F, 0x16, 0x05, 0x25, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F,
    0x10, 0x11, 0x12, 0x13, 0x3C, 0x3D, 0x32, 0x26, 0x18, 0x19, 0x3F, 0x27, 0x1C, 0x1D, 0x1E, 0x1F,
    0x40, 0x5A, 0x7F, 0x7B, 0x5B, 0x6C, 0x50, 0x7D, 0x4D, 0x5D, 0x5C, 0x4E, 0x6B, 0x60, 0x4B, 0x61,
    0xF0, 0xF1, 0xF2, 0xF3, 0xF4, 0xF5, 0xF6, 0xF7, 0xF8, 0xF9, 0x7A, 0x5E, 0x4C, 0x7E, 0x6E, 0x6F,
    0x7C, 0xC1, 0xC2, 0xC3, 0xC4, 0xC5, 0xC6, 0xC7, 0xC8, 0xC9, 0xD1, 0xD2, 0xD3, 0xD4, 0xD5, 0xD6,
    0xD7, 0xD8, 0xD9, 0xE2, 0xE3, 0xE4, 0xE5, 0xE6, 0xE7, 0xE8, 0xE9, 0xBA, 0xE0, 0xBB, 0xB0, 0x6D,
    0x79, 0x81, 0x82, 0x83, 0x84, 0x85, 0x86, 0x87, 0x88, 0x89, 0x91, 0x92, 0x93, 0x94, 0x95, 0x96,
    0x97, 0x98, 0x99, 0xA2, 0xA3, 0xA4, 0xA5, 0xA6, 0xA7, 0xA8, 0xA9, 0xC0, 0x4F, 0xD0, 0xA1, 0x07,
    0x20, 0x21, 0x22, 0x23, 0x24, 0x15, 0x06, 0x17, 0x28, 0x29, 0x2A, 0x2B, 0x2C, 0x09, 0x0A, 0x1B,
    0x30, 0x31, 0x1A, 0x33, 0x34, 0x35, 0x36, 0x08, 0x38, 0x39, 0x3A, 0x3B, 0x04, 0x14, 0x3E, 0xFF,
    0x41, 0xAA, 0x4A, 0xB1, 0x9F, 0xB2, 0x6A, 0xB5, 0xBD, 0xB4, 0x9A, 0x8A, 0x5F, 0xCA, 0xAF, 0xBC,
    0x90, 0x8F, 0xEA, 0xFA, 0xBE, 0xA0, 0xB6, 0xB3, 0x9D, 0xDA, 0x9B, 0x8B, 0xB7, 0xB8, 0xB9, 0xAB,
    0x64, 0x65, 0x62, 0x66, 0x63, 0x67, 0x9E, 0x68, 0x74, 0x71, 0x72, 0x73, 0x78, 0x75, 0x76, 0x77,
    0xAC, 0x69, 0xED, 0xEE, 0xEB, 0xEF, 0xEC, 0xBF, 0x80, 0xFD, 0xFE, 0xFB, 0xFC, 0xAD, 0xAE, 0x59,
    0x44, 0x45, 0x42, 0x46, 0x43, 0x47, 0x9C, 0x48, 0x54, 0x51, 0x52, 0x53, 0x58, 0x55, 0x56, 0x57,
    0x8C, 0x49, 0xCD, 0xCE, 0xCB, 0xCF, 0xCC, 0xE1, 0x70, 0xDD, 0xDE, 0xDB, 0xDC, 0x8D, 0x8E, 0xDF,
];

#[cfg(test)]
mod tests {
    use encoding_rs::{
        BIG5, EUC_JP, EUC_KR, GBK, ISO_2022_JP, ISO_8859_2, ISO_8859_5, SHIFT_JIS, WINDOWS_874,
        WINDOWS_1250, WINDOWS_1251, WINDOWS_1252,
    };

    use super::*;

    fn assert_text(bytes: Vec<u8>, expected_text: &str, expected_encoding: &str) {
        let original = bytes.clone();
        let classified = classify_bytes(bytes).unwrap();
        let ClassifiedFile::Text(text) = classified else {
            panic!("{expected_encoding} input was classified as binary")
        };
        assert_eq!(text.text, expected_text);
        assert_eq!(text.encoding, expected_encoding);
        let (reencoded, promoted) =
            encode_text(&text.text, &text.encoding, text.has_byte_order_mark).unwrap();
        assert!(!promoted, "{expected_encoding} unexpectedly promoted");
        assert_eq!(
            reencoded, original,
            "{expected_encoding} did not re-encode exactly"
        );
    }

    fn encoded(text: &str, encoding: &'static Encoding) -> Vec<u8> {
        let (bytes, _, had_errors) = encoding.encode(text);
        assert!(!had_errors);
        bytes.into_owned()
    }

    #[test]
    fn infer_text_signatures_remain_text_and_supply_media_types() {
        for (source, media_type) in [
            ("<!DOCTYPE HTML><title>oll</title>\n", "text/html"),
            ("<?xml version=\"1.0\"?><oll/>\n", "text/xml"),
            ("#!/bin/sh\nprintf '%s\\n' oll\n", "text/x-shellscript"),
        ] {
            let ClassifiedFile::Text(text) = classify_bytes(source.as_bytes().to_vec()).unwrap()
            else {
                panic!("{media_type} was classified as binary")
            };
            assert_eq!(text.text, source);
            assert_eq!(text.media_type, media_type);
        }
    }

    #[test]
    fn recognizes_unicode_encoding_families_and_reencodes_exactly() {
        assert_text(
            b"plain ASCII text\n".to_vec(),
            "plain ASCII text\n",
            "UTF-8",
        );
        assert_text("UTF-8 叶子\n".as_bytes().to_vec(), "UTF-8 叶子\n", "UTF-8");

        for (encoding, bom, expected_name) in [
            (
                TextEncoding::EncodingRs(UTF_16LE),
                &[0xFF, 0xFE][..],
                "UTF-16LE",
            ),
            (
                TextEncoding::EncodingRs(UTF_16BE),
                &[0xFE, 0xFF][..],
                "UTF-16BE",
            ),
            (
                TextEncoding::Utf32Le,
                &[0xFF, 0xFE, 0x00, 0x00][..],
                "UTF-32LE",
            ),
            (
                TextEncoding::Utf32Be,
                &[0x00, 0x00, 0xFE, 0xFF][..],
                "UTF-32BE",
            ),
        ] {
            let mut bytes = bom.to_vec();
            bytes.extend(encoding.encode("Unicode 叶子\n").unwrap());
            assert_text(bytes, "Unicode 叶子\n", expected_name);
        }

        assert_text(
            encode_utf32("BOM-less UTF-32LE", true),
            "BOM-less UTF-32LE",
            "UTF-32LE",
        );
        assert_text(
            encode_utf32("BOM-less UTF-32BE", false),
            "BOM-less UTF-32BE",
            "UTF-32BE",
        );
        assert_text(
            encode_utf16("BOM-less UTF-16LE", true),
            "BOM-less UTF-16LE",
            "UTF-16LE",
        );
        assert_text(
            encode_utf16("BOM-less UTF-16BE", false),
            "BOM-less UTF-16BE",
            "UTF-16BE",
        );

        assert_text(b"+ZeVnLIqe-".to_vec(), "日本語", "UTF-7");
        assert_text(vec![0xDD, 0x73, 0x66, 0x73, 0xC1], "A", "UTF-EBCDIC");
        assert_text(vec![0x88, 0x85, 0x93, 0x93, 0x96], "hello", "IBM037");

        for (text, encoding, has_bom) in [
            ("日本語", "UTF-7", false),
            ("UTF-EBCDIC 叶子", "UTF-EBCDIC", true),
            ("IBM037 café", "IBM037", false),
            ("UTF-32 叶子", "UTF-32LE", true),
        ] {
            let (bytes, promoted) = encode_text(text, encoding, has_bom).unwrap();
            assert!(!promoted);
            let ClassifiedFile::Text(decoded) = classify_bytes(bytes.clone()).unwrap() else {
                panic!("{encoding} round trip became binary")
            };
            assert_eq!(decoded.text, text);
            assert_eq!(decoded.has_byte_order_mark, has_bom);
            assert_eq!(encode_text(text, encoding, has_bom).unwrap().0, bytes);
        }
    }

    #[test]
    fn recognizes_required_legacy_encoding_matrix() {
        let cases = [
            // GB2312 is a byte-compatible subset and is recorded canonically as GBK.
            ("这是一个字符编码测试。", GBK, "GBK"),
            ("GBK扩展镕字", GBK, "GBK"),
            ("GB18030 🍃 编码测试", GB18030, "gb18030"),
            ("這是一個字符編碼測試。", BIG5, "Big5"),
            ("これは文字実験です。", SHIFT_JIS, "Shift_JIS"),
            ("これは文字実験です。", EUC_JP, "EUC-JP"),
            ("日本語", ISO_2022_JP, "ISO-2022-JP"),
            ("이것은 문자 인코딩 테스트입니다.", EUC_KR, "EUC-KR"),
            // TIS-620 is recorded as its reversible windows-874 superset.
            ("นี่คือการทดสอบการเข้ารหัสอักขระ", WINDOWS_874, "windows-874"),
            // ISO-8859-1 bytes without C1 distinctions canonicalize as windows-1252.
            (
                "Este é um teste de codificação de caracteres.",
                WINDOWS_1252,
                "windows-1252",
            ),
            (
                "To jest test kodowania znaków. W przypadku niektórych języków, które używają znaków łacińskich, potrzebujemy więcej danych, aby podjąć decyzję.",
                ISO_8859_2,
                "ISO-8859-2",
            ),
            ("Это тест кодировки символов.", ISO_8859_5, "ISO-8859-5"),
            (
                "To jest test kodowania znaków. W przypadku niektórych języków, które używają znaków łacińskich, potrzebujemy więcej danych, aby podjąć decyzję.",
                WINDOWS_1250,
                "windows-1250",
            ),
            ("Это тест кодировки символов.", WINDOWS_1251, "windows-1251"),
            ("Это тест кодировки символов.", KOI8_R, "KOI8-R"),
            ("Це тест на кодування символів.", KOI8_U, "KOI8-U"),
        ];
        for (text, encoding, expected_name) in cases {
            assert_text(encoded(text, encoding), text, expected_name);
        }

        let iso_8859_15 = "€ Š Ž Œ œ Ÿ";
        assert_text(
            encoded(iso_8859_15, ISO_8859_15),
            iso_8859_15,
            "ISO-8859-15",
        );
        let macroman = "Å å ç é ñ ö ü";
        assert_text(encoded(macroman, MACINTOSH), macroman, "macintosh");
    }

    #[test]
    fn binary_signatures_and_nul_rich_data_remain_binary() {
        for bytes in [
            vec![137, 80, 78, 71, 13, 10, 26, 10],
            b"%PDF-1.7\n".to_vec(),
            vec![0, 1, 2, 3, 0, 4, 5, 6],
        ] {
            assert!(matches!(
                classify_bytes(bytes).unwrap(),
                ClassifiedFile::Binary(_)
            ));
        }
    }

    #[test]
    fn invalid_bom_and_unrepresentable_legacy_text_are_rejected_or_promoted() {
        assert!(matches!(
            encode_text("hello", "windows-1252", true),
            Err(ReplicaError::CorruptStore(_))
        ));
        assert!(encode_text("not EBCDIC 🍃", "IBM037", false).unwrap().1);
        assert!(matches!(
            classify_bytes(vec![0xFF, 0xFE, 0x00]),
            Ok(ClassifiedFile::Binary(_))
        ));
    }
}
