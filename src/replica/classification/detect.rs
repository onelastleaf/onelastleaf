use chardetng::{EncodingDetector, Iso2022JpDetection, Utf8Detection};
use encoding_rs::{
    GB18030, ISO_8859_2, ISO_8859_15, KOI8_R, KOI8_U, MACINTOSH, UTF_8, UTF_16BE, UTF_16LE,
    WINDOWS_1250, WINDOWS_1252,
};
use infer::MatcherType;

use super::{
    ebcdic::{decode_utf_ebcdic, looks_like_ebcdic},
    encoding::TextEncoding,
    unicode::decode_utf7,
};

pub(super) fn decode_text(bytes: &[u8]) -> Option<(String, TextEncoding, bool)> {
    decode_bom_text(bytes)
        .or_else(|| decode_bomless_unicode(bytes))
        .or_else(|| decode_legacy_text(bytes))
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

pub(super) fn text_media_type(text: &str, inferred: Option<&str>) -> String {
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
