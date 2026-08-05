use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
    str::FromStr,
};

use mlua::{Lua, LuaOptions, MultiValue, StdLib, Table, Value, chunk::ChunkMode};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{cli::GitRemote, node::identity::atomic_write, plugin::PluginId};

use super::PackageError;

mod literal;

use literal::LiteralValue;

pub const PLUGINS_FILENAME: &str = "plugins.lua";

const MAX_DECODE_DEPTH: usize = 128;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DeclarationMode {
    Source,
    Release,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "value")]
pub enum GitSelection {
    Default,
    Branch(String),
    Revision(String),
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PluginDeclaration {
    pub remote: String,
    pub mode: DeclarationMode,
    pub selection: GitSelection,
    pub release: Option<String>,
}

impl PluginDeclaration {
    pub fn validate(&self) -> Result<(), PackageError> {
        GitRemote::from_str(&self.remote)
            .map_err(|message| PackageError::new("git_remote_invalid", "declaration", message))?;
        match (&self.mode, &self.release) {
            (DeclarationMode::Source, Some(_)) => Err(PackageError::new(
                "plugin_config_schema",
                "declaration",
                "release is forbidden for a source declaration",
            )),
            (DeclarationMode::Release, Some(release)) if release.is_empty() => {
                Err(PackageError::new(
                    "plugin_config_schema",
                    "declaration",
                    "release ID must not be empty",
                ))
            }
            _ => Ok(()),
        }
    }

    pub fn normalized_sha256(&self) -> [u8; 32] {
        let encoded = serde_json::to_vec(self)
            .expect("a validated plugin declaration always serializes to JSON");
        Sha256::digest(encoded).into()
    }

    pub fn sanitized_remote(&self) -> String {
        self.remote
            .parse::<GitRemote>()
            .map(|remote| remote.to_string())
            .unwrap_or_else(|_| "<invalid-remote>".to_owned())
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PluginDeclarations {
    entries: BTreeMap<PluginId, PluginDeclaration>,
}

impl PluginDeclarations {
    pub fn get(&self, plugin_id: &PluginId) -> Option<&PluginDeclaration> {
        self.entries.get(plugin_id)
    }

    pub fn insert(
        &mut self,
        plugin_id: PluginId,
        declaration: PluginDeclaration,
    ) -> Option<PluginDeclaration> {
        self.entries.insert(plugin_id, declaration)
    }

    pub fn remove(&mut self, plugin_id: &PluginId) -> Option<PluginDeclaration> {
        self.entries.remove(plugin_id)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&PluginId, &PluginDeclaration)> {
        self.entries.iter()
    }

    pub fn ids(&self) -> impl Iterator<Item = &PluginId> {
        self.entries.keys()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn parse(source: &str) -> Result<Self, PackageError> {
        let literal = LiteralValue::parse_module(source)?;
        let canonical = literal.canonical_module();
        let lua = Lua::new_with(StdLib::NONE, LuaOptions::default()).map_err(|_| {
            PackageError::new(
                "plugin_config_syntax",
                "declaration",
                "cannot initialize isolated plugins.lua evaluator",
            )
        })?;
        let values: MultiValue = lua
            .load(canonical)
            .set_name("=plugins.lua literal")
            .set_mode(ChunkMode::Text)
            .eval()
            .map_err(|_| {
                PackageError::new(
                    "plugin_config_syntax",
                    "declaration",
                    "validated plugins.lua literal could not be evaluated",
                )
            })?;
        if values.len() != 1 {
            return Err(schema_error("plugins.lua must return exactly one value"));
        }
        let value = values
            .into_iter()
            .next()
            .expect("one checked plugins.lua return value");
        let decoded = decode_lua_literal(value, &mut BTreeSet::new(), 0)?;
        decode_declarations(decoded)
    }

    pub fn to_lua(&self) -> String {
        let mut output = String::from("return {\n");
        for (plugin_id, declaration) in &self.entries {
            output.push_str("    [");
            output.push_str(&lua_string(plugin_id.as_str()));
            output.push_str("] = {\n        remote = ");
            output.push_str(&lua_string(&declaration.remote));
            output.push_str(",\n        mode = ");
            output.push_str(&lua_string(match declaration.mode {
                DeclarationMode::Source => "source",
                DeclarationMode::Release => "release",
            }));
            output.push_str(",\n");
            match &declaration.selection {
                GitSelection::Default => {}
                GitSelection::Branch(branch) => {
                    output.push_str("        branch = ");
                    output.push_str(&lua_string(branch));
                    output.push_str(",\n");
                }
                GitSelection::Revision(revision) => {
                    output.push_str("        rev = ");
                    output.push_str(&lua_string(revision));
                    output.push_str(",\n");
                }
            }
            if let Some(release) = &declaration.release {
                output.push_str("        release = ");
                output.push_str(&lua_string(release));
                output.push_str(",\n");
            }
            output.push_str("    },\n");
        }
        output.push_str("}\n");
        output
    }
}

pub fn read_plugin_declarations(config_root: &Path) -> Result<PluginDeclarations, PackageError> {
    let path = config_root.join(PLUGINS_FILENAME);
    let bytes = fs::read(&path).map_err(|error| {
        let code = if error.kind() == std::io::ErrorKind::NotFound {
            "plugin_config_missing"
        } else {
            "plugin_config_syntax"
        };
        PackageError::io(code, "declaration", "cannot read plugins.lua", error)
    })?;
    let source = String::from_utf8(bytes).map_err(|_| {
        PackageError::new(
            "plugin_config_syntax",
            "declaration",
            "plugins.lua must be valid UTF-8 source",
        )
    })?;
    PluginDeclarations::parse(&source)
}

pub fn write_plugin_declarations(
    config_root: &Path,
    declarations: &PluginDeclarations,
) -> Result<(), PackageError> {
    let path = config_root.join(PLUGINS_FILENAME);
    atomic_write(&path, declarations.to_lua().as_bytes()).map_err(|_error| {
        PackageError::new(
            "plugin_config_schema",
            "declaration",
            "cannot atomically replace plugins.lua",
        )
    })
}

pub fn plugins_file_sha256(config_root: &Path) -> Result<[u8; 32], PackageError> {
    let path = config_root.join(PLUGINS_FILENAME);
    fs::read(path)
        .map(|bytes| Sha256::digest(bytes).into())
        .map_err(|error| {
            PackageError::io(
                "plugin_config_syntax",
                "declaration",
                "cannot read plugins.lua for compare-and-set",
                error,
            )
        })
}

#[derive(Debug)]
enum DecodedLiteral {
    String(String),
    Boolean,
    Integer,
    List(Vec<DecodedLiteral>),
    Map(BTreeMap<String, DecodedLiteral>),
}

impl DecodedLiteral {
    fn description(&self) -> String {
        match self {
            Self::String(_) => "string".to_owned(),
            Self::Boolean => "boolean".to_owned(),
            Self::Integer => "integer".to_owned(),
            Self::List(values) => format!("list with {} entries", values.len()),
            Self::Map(_) => "map".to_owned(),
        }
    }
}

fn decode_lua_literal(
    value: Value,
    active_tables: &mut BTreeSet<usize>,
    depth: usize,
) -> Result<DecodedLiteral, PackageError> {
    if depth > MAX_DECODE_DEPTH {
        return Err(schema_error("plugins.lua literal nesting is too deep"));
    }
    match value {
        Value::String(value) => value
            .to_str()
            .map(|value| DecodedLiteral::String(value.to_owned()))
            .map_err(|_| schema_error("plugins.lua strings must be valid UTF-8")),
        Value::Boolean(_) => Ok(DecodedLiteral::Boolean),
        Value::Integer(_) => Ok(DecodedLiteral::Integer),
        Value::Number(value)
            if value.is_finite()
                && value.fract() == 0.0
                && value >= i64::MIN as f64
                && value <= i64::MAX as f64 =>
        {
            Ok(DecodedLiteral::Integer)
        }
        Value::Table(table) => decode_lua_table(table, active_tables, depth),
        other => Err(schema_error(format!(
            "plugins.lua literal contains unsupported {} value",
            other.type_name()
        ))),
    }
}

fn decode_lua_table(
    table: Table,
    active_tables: &mut BTreeSet<usize>,
    depth: usize,
) -> Result<DecodedLiteral, PackageError> {
    let identity = table.to_pointer() as usize;
    if !active_tables.insert(identity) {
        return Err(schema_error("plugins.lua literal contains a cyclic table"));
    }
    let decoded = (|| {
        let mut list = BTreeMap::new();
        let mut map = BTreeMap::new();
        for pair in table.pairs::<Value, Value>() {
            let (key, value) = pair.map_err(|_| {
                schema_error("plugins.lua literal table could not be recursively converted")
            })?;
            let value = decode_lua_literal(value, active_tables, depth + 1)?;
            match key {
                Value::String(key) => {
                    let key = key
                        .to_str()
                        .map_err(|_| schema_error("plugins.lua map keys must be valid UTF-8"))?
                        .to_owned();
                    map.insert(key, value);
                }
                Value::Integer(index) if index > 0 => {
                    let index = usize::try_from(index)
                        .map_err(|_| schema_error("plugins.lua list index is too large"))?;
                    list.insert(index, value);
                }
                Value::Number(index)
                    if index.is_finite()
                        && index.fract() == 0.0
                        && index > 0.0
                        && index <= usize::MAX as f64 =>
                {
                    list.insert(index as usize, value);
                }
                _ => return Err(schema_error("plugins.lua table keys must be UTF-8 strings")),
            }
        }
        if !list.is_empty() && !map.is_empty() {
            return Err(schema_error(
                "plugins.lua tables cannot mix list entries and map fields",
            ));
        }
        if !map.is_empty() || list.is_empty() {
            return Ok(DecodedLiteral::Map(map));
        }
        let expected = list.len();
        if list.keys().copied().ne(1..=expected) {
            return Err(schema_error(
                "plugins.lua list indexes must be contiguous from one",
            ));
        }
        Ok(DecodedLiteral::List(list.into_values().collect()))
    })();
    active_tables.remove(&identity);
    decoded
}

fn decode_declarations(value: DecodedLiteral) -> Result<PluginDeclarations, PackageError> {
    let DecodedLiteral::Map(entries) = value else {
        return Err(schema_error(format!(
            "plugins.lua return value must be a map, not {}",
            value.description()
        )));
    };
    let mut declarations = BTreeMap::new();
    for (raw_id, value) in entries {
        let plugin_id = raw_id.parse::<PluginId>().map_err(|error| {
            schema_error(format!("plugins.lua contains invalid PluginId: {error}"))
        })?;
        let declaration = decode_declaration(value)?;
        if declarations.insert(plugin_id, declaration).is_some() {
            return Err(PackageError::new(
                "plugin_config_duplicate",
                "declaration",
                "plugins.lua repeats a PluginId",
            ));
        }
    }
    Ok(PluginDeclarations {
        entries: declarations,
    })
}

fn decode_declaration(value: DecodedLiteral) -> Result<PluginDeclaration, PackageError> {
    let DecodedLiteral::Map(mut fields) = value else {
        return Err(schema_error(format!(
            "plugin declaration must be a map, not {}",
            value.description()
        )));
    };
    for field in fields.keys() {
        if !matches!(
            field.as_str(),
            "remote" | "mode" | "branch" | "rev" | "release"
        ) {
            return Err(schema_error(format!(
                "declaration contains unknown field {field}"
            )));
        }
    }
    let remote = take_required_string(&mut fields, "remote")?;
    let mode = match take_optional_string(&mut fields, "mode")?.as_deref() {
        None | Some("source") => DeclarationMode::Source,
        Some("release") => DeclarationMode::Release,
        Some(_) => {
            return Err(schema_error("declaration mode must be source or release"));
        }
    };
    let branch = take_optional_string(&mut fields, "branch")?;
    let revision = take_optional_string(&mut fields, "rev")?;
    let selection = match (branch, revision) {
        (None, None) => GitSelection::Default,
        (Some(branch), None) if !branch.is_empty() => GitSelection::Branch(branch),
        (None, Some(revision)) if !revision.is_empty() => GitSelection::Revision(revision),
        (Some(_), Some(_)) => {
            return Err(schema_error(
                "declaration branch and rev are mutually exclusive",
            ));
        }
        _ => {
            return Err(schema_error("declaration branch and rev must not be empty"));
        }
    };
    let declaration = PluginDeclaration {
        remote,
        mode,
        selection,
        release: take_optional_string(&mut fields, "release")?,
    };
    declaration.validate()?;
    Ok(declaration)
}

fn take_required_string(
    fields: &mut BTreeMap<String, DecodedLiteral>,
    name: &str,
) -> Result<String, PackageError> {
    take_optional_string(fields, name)?
        .ok_or_else(|| schema_error(format!("declaration requires {name}")))
}

fn take_optional_string(
    fields: &mut BTreeMap<String, DecodedLiteral>,
    name: &str,
) -> Result<Option<String>, PackageError> {
    match fields.remove(name) {
        None => Ok(None),
        Some(DecodedLiteral::String(value)) => Ok(Some(value)),
        Some(value) => Err(schema_error(format!(
            "declaration field {name} must be a string, not {}",
            value.description()
        ))),
    }
}

fn schema_error(message: impl Into<String>) -> PackageError {
    PackageError::new("plugin_config_schema", "declaration", message)
}

fn lua_string(input: &str) -> String {
    let mut output = String::with_capacity(input.len() + 2);
    output.push('"');
    for character in input.chars() {
        match character {
            '\\' => output.push_str("\\\\"),
            '"' => output.push_str("\\\""),
            '\n' => output.push_str("\\n"),
            '\r' => output.push_str("\\r"),
            '\t' => output.push_str("\\t"),
            '\u{0000}'..='\u{001f}' | '\u{007f}' => {
                output.push_str(&format!("\\{:03}", character as u32));
            }
            _ => output.push(character),
        }
    }
    output.push('"');
    output
}

pub fn mask_path(config_root: &Path, plugin_id: &PluginId) -> PathBuf {
    config_root
        .join("plugin-masks")
        .join(format!("{}.toml", plugin_id.as_str()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn literal_parser_round_trips_stable_plugin_id_order_and_branch() {
        let input = r#"
-- user declarations
return {
  ["oll.pdf"] = { remote = "https://example.test/pdf.git", rev = "abc" },
  ["oll.anki"] = {
    remote = "git@example.test:anki.git",
    mode = "release",
    branch = "releases",
    release = "v0.3.1",
  },
}
"#;
        let parsed = PluginDeclarations::parse(input).unwrap();
        assert_eq!(
            parsed.get(&"oll.anki".parse().unwrap()).unwrap().selection,
            GitSelection::Branch("releases".to_owned())
        );
        let encoded = parsed.to_lua();
        assert_eq!(encoded.matches("branch =").count(), 1);
        assert!(encoded.find("oll.anki").unwrap() < encoded.find("oll.pdf").unwrap());
        assert_eq!(PluginDeclarations::parse(&encoded).unwrap(), parsed);
    }

    #[test]
    fn generic_nested_literals_reach_schema_validation() {
        let error = PluginDeclarations::parse(
            r#"return {
                ["oll.test"] = {
                    remote = "https://example.test/test.git",
                    extra = { true, -7, { nested = "value" } },
                },
            }"#,
        )
        .unwrap_err();
        assert_eq!(error.code(), "plugin_config_schema");
        assert!(error.message().contains("unknown field extra"));
    }

    #[test]
    fn malicious_source_is_rejected_before_it_can_execute() {
        let directory = tempfile::TempDir::new().unwrap();
        let marker = directory.path().join("must-not-exist");
        let source = format!(
            r#"return {{ ["oll.test"] = {{
                remote = (os.execute("touch {}") and "https://example.test/test.git"),
            }} }}"#,
            marker.display()
        );
        let error = PluginDeclarations::parse(&source).unwrap_err();
        assert_eq!(error.code(), "plugin_config_syntax");
        assert!(!marker.exists());
    }

    #[test]
    fn calls_operators_and_non_return_statements_are_rejected() {
        for source in [
            "return require('x')",
            r#"return { ["oll.test"] = { remote = "https://e/x" .. "y" } }"#,
            "local value = {}; return value",
            "return setmetatable({}, {})",
            "return function() end",
        ] {
            assert_eq!(
                PluginDeclarations::parse(source).unwrap_err().code(),
                "plugin_config_syntax",
                "source unexpectedly passed literal syntax gate: {source}"
            );
        }
    }

    #[test]
    fn duplicate_ids_and_fields_are_rejected_before_evaluation() {
        let duplicate_id = PluginDeclarations::parse(
            r#"return {
                ["oll.test"] = { remote = "https://e/x" },
                ["oll.test"] = { remote = "https://e/y" },
            }"#,
        )
        .unwrap_err();
        assert_eq!(duplicate_id.code(), "plugin_config_duplicate");

        let duplicate_field = PluginDeclarations::parse(
            r#"return { ["oll.test"] = {
                remote = "https://e/x",
                ["remote"] = "https://e/y",
            } }"#,
        )
        .unwrap_err();
        assert_eq!(duplicate_field.code(), "plugin_config_schema");
    }

    #[test]
    fn string_escapes_are_decoded_then_serialized_canonically() {
        let declarations = PluginDeclarations::parse(
            r#"return { ["oll.test"] = {
                remote = "https:\x2f\x2fexample.test\x2frepo.git",
                branch = "release\047v1",
            } }"#,
        )
        .unwrap();
        let declaration = declarations.get(&"oll.test".parse().unwrap()).unwrap();
        assert_eq!(declaration.remote, "https://example.test/repo.git");
        assert_eq!(
            declaration.selection,
            GitSelection::Branch("release/v1".to_owned())
        );
        assert_eq!(
            PluginDeclarations::parse(&declarations.to_lua()).unwrap(),
            declarations
        );
    }

    #[test]
    fn cyclic_construction_and_bytecode_are_impossible_inputs() {
        let cyclic = "local value = {}; value.self = value; return value";
        assert_eq!(
            PluginDeclarations::parse(cyclic).unwrap_err().code(),
            "plugin_config_syntax"
        );
        assert_eq!(
            PluginDeclarations::parse("\u{001b}Lua\0\0\0")
                .unwrap_err()
                .code(),
            "plugin_config_syntax"
        );
    }

    #[test]
    fn desired_state_is_not_a_declaration_field() {
        let result = PluginDeclarations::parse(
            r#"return { ["oll.test"] = { remote = "https://e/x", desired_state = "running" } }"#,
        );
        assert_eq!(result.unwrap_err().code(), "plugin_config_schema");
    }
}
