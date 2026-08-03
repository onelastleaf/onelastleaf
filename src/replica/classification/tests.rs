use encoding_rs::{
    BIG5, EUC_JP, EUC_KR, Encoding, GB18030, GBK, ISO_2022_JP, ISO_8859_2, ISO_8859_5, ISO_8859_15,
    KOI8_R, KOI8_U, MACINTOSH, SHIFT_JIS, UTF_16BE, UTF_16LE, WINDOWS_874, WINDOWS_1250,
    WINDOWS_1251, WINDOWS_1252,
};

use super::{
    ClassifiedFile, ReplicaError, classify_bytes, encode_text,
    encoding::TextEncoding,
    unicode::{encode_utf16, encode_utf32},
};

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
        let ClassifiedFile::Text(text) = classify_bytes(source.as_bytes().to_vec()).unwrap() else {
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
