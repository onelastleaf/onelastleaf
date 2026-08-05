use std::{
    fs::File,
    io::{self, BufRead, BufReader, Write},
    path::Path,
};

use crate::{
    cli::PluginLogTarget,
    node::runtime::NodeError,
    plugin::{PluginSelector, package::validate_local_package_config},
};

pub(super) fn validate_package_configuration(config_root: &Path) -> Result<(), NodeError> {
    validate_local_package_config(config_root).map_err(|error| {
        let mut message = format!(
            "plugin package configuration is invalid [{}:{}]: {}",
            error.phase(),
            error.code(),
            error.message()
        );
        if let Some(hint) = error.hint() {
            message.push_str("; ");
            message.push_str(hint);
        }
        NodeError::Config(message)
    })?;
    println!("plugin package configuration is valid");
    Ok(())
}

pub(super) fn show_plugin_log(path: &Path, target: PluginLogTarget) -> Result<(), NodeError> {
    let selector = plugin_log_selector(target)?;
    let file = File::open(path).map_err(|error| NodeError::io("open plugin log", error))?;
    let mut stdout = io::stdout().lock();
    filter_plugin_log(BufReader::new(file), &mut stdout, selector)?;
    stdout
        .flush()
        .map_err(|error| NodeError::io("flush plugin log output", error))
}

fn plugin_log_selector(
    target: PluginLogTarget,
) -> Result<Option<(&'static str, String)>, NodeError> {
    match target {
        PluginLogTarget::All => Ok(None),
        PluginLogTarget::Plugin(value) => {
            let parsed = value.parse::<PluginSelector>().map_err(|error| {
                NodeError::Operation(format!("invalid plugin selector `{value}`: {error}"))
            })?;
            Ok(Some((
                match parsed {
                    PluginSelector::Id(_) => "plugin_id",
                    PluginSelector::Name(_) => "plugin_name",
                },
                value,
            )))
        }
    }
}

fn filter_plugin_log(
    mut reader: impl BufRead,
    writer: &mut impl Write,
    selector: Option<(&'static str, String)>,
) -> Result<(), NodeError> {
    let mut line = String::new();
    let mut index = 0_usize;
    loop {
        line.clear();
        let count = reader
            .read_line(&mut line)
            .map_err(|error| NodeError::io("read plugin log", error))?;
        if count == 0 {
            break;
        }
        index += 1;
        if !line.ends_with('\n') {
            // The daemon may be appending this record concurrently. A later
            // invocation will see it after the JSONL writer completes it.
            break;
        }
        line.pop();
        if line.ends_with('\r') {
            line.pop();
        }
        let include = if let Some((field, value)) = selector.as_ref() {
            let record: serde_json::Value = serde_json::from_str(&line).map_err(|error| {
                NodeError::Operation(format!(
                    "plugin log contains invalid JSON on line {index}: {error}"
                ))
            })?;
            record.get(*field).and_then(serde_json::Value::as_str) == Some(value.as_str())
        } else {
            true
        };
        if include {
            writer
                .write_all(line.as_bytes())
                .and_then(|()| writer.write_all(b"\n"))
                .map_err(|error| NodeError::io("write plugin log output", error))?;
        }
    }
    Ok(())
}

#[cfg(test)]
pub(super) fn filter_plugin_log_for_test(
    reader: impl BufRead,
    writer: &mut impl Write,
    target: PluginLogTarget,
) -> Result<(), NodeError> {
    filter_plugin_log(reader, writer, plugin_log_selector(target)?)
}
