use std::collections::BTreeSet;

use super::super::PackageError;

const MAX_LITERAL_DEPTH: usize = 128;

#[derive(Clone, Debug)]
pub(super) enum LiteralValue {
    String(String),
    Boolean(bool),
    Integer(i64),
    Table(LiteralTable),
}

#[derive(Clone, Debug)]
pub(super) struct LiteralTable {
    fields: Vec<LiteralField>,
}

#[derive(Clone, Debug)]
enum LiteralField {
    Named(String, LiteralValue),
    Sequence(LiteralValue),
}

impl LiteralValue {
    pub(super) fn parse_module(source: &str) -> Result<Self, PackageError> {
        LiteralParser::new(source).parse_module()
    }

    pub(super) fn canonical_module(&self) -> String {
        let mut output = String::from("return ");
        self.write_lua(&mut output);
        output.push('\n');
        output
    }

    fn write_lua(&self, output: &mut String) {
        match self {
            Self::String(value) => output.push_str(&super::lua_string(value)),
            Self::Boolean(value) => output.push_str(if *value { "true" } else { "false" }),
            Self::Integer(value) => output.push_str(&value.to_string()),
            Self::Table(table) => table.write_lua(output),
        }
    }
}

impl LiteralTable {
    fn write_lua(&self, output: &mut String) {
        output.push('{');
        for field in &self.fields {
            match field {
                LiteralField::Named(key, value) => {
                    output.push('[');
                    output.push_str(&super::lua_string(key));
                    output.push_str("]=");
                    value.write_lua(output);
                }
                LiteralField::Sequence(value) => value.write_lua(output),
            }
            output.push(',');
        }
        output.push('}');
    }
}

struct LiteralParser<'a> {
    source: &'a str,
    bytes: &'a [u8],
    cursor: usize,
}

impl<'a> LiteralParser<'a> {
    fn new(source: &'a str) -> Self {
        Self {
            source,
            bytes: source.as_bytes(),
            cursor: 0,
        }
    }

    fn parse_module(mut self) -> Result<LiteralValue, PackageError> {
        self.skip_trivia()?;
        self.keyword("return")?;
        self.skip_trivia()?;
        let value = self.value(0)?;
        self.skip_trivia()?;
        self.take(b';');
        self.skip_trivia()?;
        if self.cursor != self.bytes.len() {
            return Err(self.syntax("plugins.lua contains content after its return value"));
        }
        Ok(value)
    }

    fn value(&mut self, depth: usize) -> Result<LiteralValue, PackageError> {
        if depth > MAX_LITERAL_DEPTH {
            return Err(self.schema("plugins.lua literal nesting is too deep"));
        }
        self.skip_trivia()?;
        match self.bytes.get(self.cursor).copied() {
            Some(b'\'' | b'"') => self.string().map(LiteralValue::String),
            Some(b'{') => self.table(depth).map(LiteralValue::Table),
            Some(b'-' | b'0'..=b'9') => self.integer().map(LiteralValue::Integer),
            Some(_) if self.at_keyword("true") => {
                self.cursor += 4;
                Ok(LiteralValue::Boolean(true))
            }
            Some(_) if self.at_keyword("false") => {
                self.cursor += 5;
                Ok(LiteralValue::Boolean(false))
            }
            _ => Err(self
                .syntax("literal values may only be strings, booleans, integers, lists, or maps")),
        }
    }

    fn table(&mut self, depth: usize) -> Result<LiteralTable, PackageError> {
        self.byte(b'{')?;
        let mut fields = Vec::new();
        let mut named_keys = BTreeSet::new();
        loop {
            self.skip_trivia()?;
            if self.take(b'}') {
                break;
            }
            let field = if self.take(b'[') {
                self.skip_trivia()?;
                let key = match self.value(depth + 1)? {
                    LiteralValue::String(key) => key,
                    _ => return Err(self.schema("literal map keys must be strings")),
                };
                self.skip_trivia()?;
                self.byte(b']')?;
                self.skip_trivia()?;
                self.byte(b'=')?;
                let value = self.value(depth + 1)?;
                self.check_duplicate_key(depth, &mut named_keys, &key)?;
                LiteralField::Named(key, value)
            } else if self.identifier_ahead() {
                let checkpoint = self.cursor;
                let key = self.identifier()?;
                self.skip_trivia()?;
                if self.take(b'=') {
                    let value = self.value(depth + 1)?;
                    self.check_duplicate_key(depth, &mut named_keys, &key)?;
                    LiteralField::Named(key, value)
                } else {
                    self.cursor = checkpoint;
                    LiteralField::Sequence(self.value(depth + 1)?)
                }
            } else {
                LiteralField::Sequence(self.value(depth + 1)?)
            };
            fields.push(field);
            self.skip_trivia()?;
            if self.take(b',') || self.take(b';') {
                continue;
            }
            if self.bytes.get(self.cursor) != Some(&b'}') {
                return Err(self.syntax("table entries must be separated by ',' or ';'"));
            }
        }
        Ok(LiteralTable { fields })
    }

    fn check_duplicate_key(
        &self,
        depth: usize,
        seen: &mut BTreeSet<String>,
        key: &str,
    ) -> Result<(), PackageError> {
        if seen.insert(key.to_owned()) {
            return Ok(());
        }
        let code = if depth == 0 {
            "plugin_config_duplicate"
        } else {
            "plugin_config_schema"
        };
        Err(self.error(code, format!("plugins.lua repeats map key {key}")))
    }

    fn integer(&mut self) -> Result<i64, PackageError> {
        let start = self.cursor;
        self.take(b'-');
        let digits = self.cursor;
        while self.bytes.get(self.cursor).is_some_and(u8::is_ascii_digit) {
            self.cursor += 1;
        }
        if self.cursor == digits {
            return Err(self.syntax("expected a decimal integer"));
        }
        if self
            .bytes
            .get(self.cursor)
            .is_some_and(|byte| byte.is_ascii_alphanumeric() || matches!(*byte, b'.' | b'_'))
        {
            return Err(self.syntax("only decimal integer literals are allowed"));
        }
        self.source[start..self.cursor]
            .parse()
            .map_err(|_| self.schema("integer literal is outside the signed 64-bit range"))
    }

    fn identifier_ahead(&self) -> bool {
        self.bytes
            .get(self.cursor)
            .is_some_and(|byte| byte.is_ascii_alphabetic() || *byte == b'_')
    }

    fn identifier(&mut self) -> Result<String, PackageError> {
        let start = self.cursor;
        if !self.identifier_ahead() {
            return Err(self.syntax("expected an identifier"));
        }
        self.cursor += 1;
        while self
            .bytes
            .get(self.cursor)
            .is_some_and(|byte| byte.is_ascii_alphanumeric() || *byte == b'_')
        {
            self.cursor += 1;
        }
        Ok(self.source[start..self.cursor].to_owned())
    }

    fn string(&mut self) -> Result<String, PackageError> {
        let quote = self
            .bytes
            .get(self.cursor)
            .copied()
            .filter(|byte| matches!(byte, b'\'' | b'"'))
            .ok_or_else(|| self.syntax("expected a quoted string"))?;
        self.cursor += 1;
        let mut output = Vec::new();
        loop {
            let byte = self
                .bytes
                .get(self.cursor)
                .copied()
                .ok_or_else(|| self.syntax("unterminated string"))?;
            self.cursor += 1;
            match byte {
                current if current == quote => break,
                b'\n' | b'\r' => {
                    return Err(self.syntax("quoted strings cannot contain a raw newline"));
                }
                b'\\' => self.escape(&mut output)?,
                other => output.push(other),
            }
        }
        String::from_utf8(output)
            .map_err(|_| self.schema("plugins.lua strings must be valid UTF-8"))
    }

    fn escape(&mut self, output: &mut Vec<u8>) -> Result<(), PackageError> {
        let escaped = self
            .bytes
            .get(self.cursor)
            .copied()
            .ok_or_else(|| self.syntax("unterminated string escape"))?;
        self.cursor += 1;
        match escaped {
            b'a' => output.push(0x07),
            b'b' => output.push(0x08),
            b'f' => output.push(0x0c),
            b'n' => output.push(b'\n'),
            b'r' => output.push(b'\r'),
            b't' => output.push(b'\t'),
            b'v' => output.push(0x0b),
            b'\\' | b'\'' | b'"' => output.push(escaped),
            b'\n' => output.push(b'\n'),
            b'\r' => {
                if self.bytes.get(self.cursor) == Some(&b'\n') {
                    self.cursor += 1;
                }
                output.push(b'\n');
            }
            b'x' => {
                let high = self.hex_digit()?;
                let low = self.hex_digit()?;
                output.push(high * 16 + low);
            }
            digit if digit.is_ascii_digit() => {
                let mut value = u16::from(digit - b'0');
                for _ in 0..2 {
                    if let Some(next) = self.bytes.get(self.cursor).copied()
                        && next.is_ascii_digit()
                    {
                        self.cursor += 1;
                        value = value * 10 + u16::from(next - b'0');
                    }
                }
                if value > u16::from(u8::MAX) {
                    return Err(self.syntax("decimal string escape exceeds one byte"));
                }
                output.push(value as u8);
            }
            _ => return Err(self.syntax("unsupported string escape")),
        }
        Ok(())
    }

    fn hex_digit(&mut self) -> Result<u8, PackageError> {
        let byte = self
            .bytes
            .get(self.cursor)
            .copied()
            .ok_or_else(|| self.syntax("truncated hexadecimal escape"))?;
        self.cursor += 1;
        match byte {
            b'0'..=b'9' => Ok(byte - b'0'),
            b'a'..=b'f' => Ok(byte - b'a' + 10),
            b'A'..=b'F' => Ok(byte - b'A' + 10),
            _ => Err(self.syntax("invalid hexadecimal string escape")),
        }
    }

    fn skip_trivia(&mut self) -> Result<(), PackageError> {
        loop {
            while self
                .bytes
                .get(self.cursor)
                .is_some_and(u8::is_ascii_whitespace)
            {
                self.cursor += 1;
            }
            if self.bytes.get(self.cursor..self.cursor + 2) != Some(b"--") {
                return Ok(());
            }
            self.cursor += 2;
            if let Some((equals, content_start)) = self.long_bracket(self.cursor) {
                self.cursor = content_start;
                loop {
                    let Some(close) = self
                        .bytes
                        .get(self.cursor..)
                        .and_then(|tail| tail.iter().position(|byte| *byte == b']'))
                    else {
                        return Err(self.syntax("unterminated block comment"));
                    };
                    self.cursor += close;
                    if self.long_bracket_close(self.cursor, equals) {
                        self.cursor += equals + 2;
                        break;
                    }
                    self.cursor += 1;
                }
            } else {
                while self
                    .bytes
                    .get(self.cursor)
                    .is_some_and(|byte| !matches!(*byte, b'\n' | b'\r'))
                {
                    self.cursor += 1;
                }
            }
        }
    }

    fn long_bracket(&self, start: usize) -> Option<(usize, usize)> {
        if self.bytes.get(start) != Some(&b'[') {
            return None;
        }
        let mut cursor = start + 1;
        while self.bytes.get(cursor) == Some(&b'=') {
            cursor += 1;
        }
        (self.bytes.get(cursor) == Some(&b'[')).then_some((cursor - start - 1, cursor + 1))
    }

    fn long_bracket_close(&self, start: usize, equals: usize) -> bool {
        self.bytes.get(start) == Some(&b']')
            && self.bytes.get(start + 1..start + 1 + equals) == Some(vec![b'='; equals].as_slice())
            && self.bytes.get(start + equals + 1) == Some(&b']')
    }

    fn keyword(&mut self, keyword: &str) -> Result<(), PackageError> {
        if self.at_keyword(keyword) {
            self.cursor += keyword.len();
            Ok(())
        } else {
            Err(self.syntax("plugins.lua must contain exactly one return statement"))
        }
    }

    fn at_keyword(&self, keyword: &str) -> bool {
        self.bytes.get(self.cursor..self.cursor + keyword.len()) == Some(keyword.as_bytes())
            && !self
                .bytes
                .get(self.cursor + keyword.len())
                .is_some_and(|byte| byte.is_ascii_alphanumeric() || *byte == b'_')
    }

    fn byte(&mut self, expected: u8) -> Result<(), PackageError> {
        if self.take(expected) {
            Ok(())
        } else {
            Err(self.syntax(format!("expected '{}'", char::from(expected))))
        }
    }

    fn take(&mut self, expected: u8) -> bool {
        if self.bytes.get(self.cursor) == Some(&expected) {
            self.cursor += 1;
            true
        } else {
            false
        }
    }

    fn syntax(&self, message: impl Into<String>) -> PackageError {
        self.error("plugin_config_syntax", message)
    }

    fn schema(&self, message: impl Into<String>) -> PackageError {
        self.error("plugin_config_schema", message)
    }

    fn error(&self, code: &'static str, message: impl Into<String>) -> PackageError {
        PackageError::new(
            code,
            "declaration",
            format!("{} at byte {}", message.into(), self.cursor),
        )
    }
}
