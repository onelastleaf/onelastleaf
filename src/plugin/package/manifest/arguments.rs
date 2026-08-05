use std::path::Path;

use super::{PackageError, RecipeStep};

#[derive(Clone, Debug)]
pub struct ExpansionPaths<'a> {
    pub source: Option<&'a Path>,
    pub staging: Option<&'a Path>,
    pub install: &'a Path,
    pub mask_dir: &'a Path,
}

pub(super) fn validate_placeholders(
    steps: &[RecipeStep],
    source_allowed: bool,
) -> Result<(), PackageError> {
    for step in steps {
        for value in &step.argv {
            scan_placeholders(value, source_allowed)?;
        }
    }
    Ok(())
}

pub(super) fn validate_runtime_placeholders(argv: &[String]) -> Result<(), PackageError> {
    for value in argv {
        scan_placeholders(value, false)?;
    }
    Ok(())
}

fn scan_placeholders(value: &str, source_allowed: bool) -> Result<(), PackageError> {
    let bytes = value.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            b'{' => {
                let end = bytes[index + 1..]
                    .iter()
                    .position(|byte| *byte == b'}')
                    .map(|offset| index + 1 + offset)
                    .ok_or_else(|| PackageError::manifest("unterminated argv placeholder"))?;
                let name = &value[index + 1..end];
                let allowed = matches!(name, "install" | "mask_dir")
                    || (source_allowed && matches!(name, "source" | "staging"));
                if !allowed {
                    return Err(PackageError::manifest(format!(
                        "unknown or unavailable argv placeholder {{{name}}}"
                    )));
                }
                let tail = &value[end + 1..];
                if !tail.is_empty() && !tail.starts_with(std::path::MAIN_SEPARATOR) {
                    return Err(PackageError::manifest(format!(
                        "path placeholder {{{name}}} must be followed by a path separator or end of argv value"
                    )));
                }
                if tail
                    .split(std::path::MAIN_SEPARATOR)
                    .any(|component| component == "..")
                {
                    return Err(PackageError::manifest(format!(
                        "path placeholder {{{name}}} cannot escape through '..'"
                    )));
                }
                index = end + 1;
            }
            b'}' => return Err(PackageError::manifest("unmatched '}' in argv value")),
            _ => index += 1,
        }
    }
    Ok(())
}

pub(super) fn expand_argv(
    argv: &[String],
    paths: &ExpansionPaths<'_>,
    source_allowed: bool,
) -> Result<Vec<String>, PackageError> {
    argv.iter()
        .map(|value| {
            scan_placeholders(value, source_allowed)?;
            let mut expanded = value.clone();
            for (name, path) in [
                ("install", Some(paths.install)),
                ("mask_dir", Some(paths.mask_dir)),
                ("source", paths.source.filter(|_| source_allowed)),
                ("staging", paths.staging.filter(|_| source_allowed)),
            ] {
                if expanded.contains(&format!("{{{name}}}")) {
                    let path = path.ok_or_else(|| {
                        PackageError::manifest(format!("placeholder {{{name}}} has no path"))
                    })?;
                    let path = path.to_str().ok_or_else(|| {
                        PackageError::manifest(format!("placeholder {{{name}}} is not UTF-8"))
                    })?;
                    expanded = expanded.replace(&format!("{{{name}}}"), path);
                }
            }
            Ok(expanded)
        })
        .collect()
}
