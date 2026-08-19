use std::path::Path;

use super::{PackageError, SourceCheckout};

#[derive(Clone, Debug)]
pub struct ExpansionPaths<'a> {
    pub source: Option<&'a Path>,
    pub install: Option<&'a Path>,
    pub generation: Option<&'a Path>,
    pub mask_dir: &'a Path,
}

#[derive(Clone, Copy)]
pub(super) enum PlaceholderScope {
    Step(SourceCheckout),
    Runtime(SourceCheckout),
    MaskStep,
    MaskRuntime,
}

pub(super) fn validate_step_placeholders(
    steps: &[Vec<String>],
    checkout: SourceCheckout,
) -> Result<(), PackageError> {
    for step in steps {
        for value in step {
            scan_placeholders(value, PlaceholderScope::Step(checkout))?;
        }
    }
    Ok(())
}

pub(super) fn validate_runtime_placeholders(
    argv: &[String],
    checkout: SourceCheckout,
) -> Result<(), PackageError> {
    for value in argv {
        scan_placeholders(value, PlaceholderScope::Runtime(checkout))?;
    }
    Ok(())
}

pub(super) fn validate_mask_step_placeholders(steps: &[Vec<String>]) -> Result<(), PackageError> {
    for step in steps {
        for value in step {
            scan_placeholders(value, PlaceholderScope::MaskStep)?;
        }
    }
    Ok(())
}

pub(super) fn validate_mask_runtime_placeholders(argv: &[String]) -> Result<(), PackageError> {
    for value in argv {
        scan_placeholders(value, PlaceholderScope::MaskRuntime)?;
    }
    Ok(())
}

fn scan_placeholders(value: &str, scope: PlaceholderScope) -> Result<(), PackageError> {
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
                let allowed = match scope {
                    PlaceholderScope::Step(SourceCheckout::Source) => {
                        matches!(name, "source" | "install" | "mask_dir")
                    }
                    PlaceholderScope::Step(SourceCheckout::Install)
                    | PlaceholderScope::Runtime(SourceCheckout::Source)
                    | PlaceholderScope::Runtime(SourceCheckout::Install) => {
                        matches!(name, "install" | "mask_dir")
                    }
                    PlaceholderScope::Step(SourceCheckout::Generation)
                    | PlaceholderScope::Runtime(SourceCheckout::Generation) => {
                        matches!(name, "generation" | "mask_dir")
                    }
                    PlaceholderScope::MaskStep => {
                        matches!(name, "source" | "install" | "generation" | "mask_dir")
                    }
                    PlaceholderScope::MaskRuntime => {
                        matches!(name, "install" | "generation" | "mask_dir")
                    }
                };
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
    scope: PlaceholderScope,
) -> Result<Vec<String>, PackageError> {
    argv.iter()
        .map(|value| {
            scan_placeholders(value, scope)?;
            let mut expanded = value.clone();
            for (name, path) in [
                ("install", paths.install),
                ("mask_dir", Some(paths.mask_dir)),
                ("source", paths.source),
                ("generation", paths.generation),
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
