use encoding_rs::{Encoding, UTF_8, UTF_16BE, UTF_16LE};

use super::{
    ReplicaError,
    ebcdic::{decode_ibm037, decode_utf_ebcdic, encode_ibm037, encode_utf_ebcdic},
    unicode::{decode_utf7, decode_utf32, encode_utf7, encode_utf16, encode_utf32},
};

#[derive(Clone, Copy)]
pub(super) enum TextEncoding {
    EncodingRs(&'static Encoding),
    Utf32Le,
    Utf32Be,
    Utf7,
    UtfEbcdic,
    Ibm037,
}

impl TextEncoding {
    pub(super) fn for_label(label: &str) -> Option<Self> {
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

    pub(super) fn name(self) -> &'static str {
        match self {
            Self::EncodingRs(encoding) => encoding.name(),
            Self::Utf32Le => "UTF-32LE",
            Self::Utf32Be => "UTF-32BE",
            Self::Utf7 => "UTF-7",
            Self::UtfEbcdic => "UTF-EBCDIC",
            Self::Ibm037 => "IBM037",
        }
    }

    pub(super) fn decode(self, bytes: &[u8]) -> Option<String> {
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

    pub(super) fn encode(self, text: &str) -> Option<Vec<u8>> {
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
