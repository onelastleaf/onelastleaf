mod job;
mod plugin;

use std::io::{self, Write};

use base64::{Engine as _, engine::general_purpose::STANDARD_NO_PAD};
use serde_json::{Map, Number, Value, json};

use crate::{
    cli::GitRemote,
    node::runtime::NodeError,
    protocol::oll::{self, config_value},
};

pub(super) use job::{show_job_info, show_job_list, show_started_job, show_stopped_job};
pub(super) use plugin::{
    installation_result_status, show_desired_state, show_installation_results, show_plugin_info,
    show_plugin_list, show_plugin_releases, show_remove, show_restart,
};

#[cfg(test)]
pub(super) use job::details_json as job_details_json;
#[cfg(test)]
pub(super) use plugin::{
    details_json as plugin_details_json, installation_result_json,
    summary_json as plugin_summary_json,
};

const ANSI_RESET: &str = "\x1b[0m";
const ANSI_BOLD_CYAN: &str = "\x1b[1;36m";
const ANSI_GREEN: &str = "\x1b[32m";
const ANSI_YELLOW: &str = "\x1b[33m";
const ANSI_RED: &str = "\x1b[31m";

pub(super) fn write_json(value: &Value) -> Result<(), NodeError> {
    let stdout = io::stdout();
    let mut stdout = stdout.lock();
    serde_json::to_writer(&mut stdout, value).map_err(|error| {
        NodeError::Internal(format!("cannot serialize command output: {error}"))
    })?;
    stdout
        .write_all(b"\n")
        .and_then(|()| stdout.flush())
        .map_err(|error| NodeError::io("write JSON output", error))
}

pub(super) fn write_human(value: &str) -> Result<(), NodeError> {
    let mut stdout = anstream::AutoStream::auto(io::stdout()).lock();
    stdout
        .write_all(value.as_bytes())
        .and_then(|()| stdout.flush())
        .map_err(|error| NodeError::io("write command output", error))
}

pub(super) fn required_id<'a>(
    value: Option<&'a oll::PluginId>,
    field: &'static str,
) -> Result<&'a str, NodeError> {
    value
        .map(|value| value.value.as_str())
        .filter(|value| !value.is_empty())
        .ok_or_else(|| NodeError::Internal(format!("daemon omitted {field}")))
}

pub(super) fn required_name<'a>(
    value: Option<&'a oll::PluginName>,
    field: &'static str,
) -> Result<&'a str, NodeError> {
    value
        .map(|value| value.value.as_str())
        .filter(|value| !value.is_empty())
        .ok_or_else(|| NodeError::Internal(format!("daemon omitted {field}")))
}

pub(super) fn diagnostic_json(value: &oll::PluginDiagnostic) -> Value {
    json!({
        "code": value.code,
        "phase": value.phase,
        "message": value.message,
        "plugin_id": value.plugin_id.as_ref().map(|value| value.value.as_str()),
        "plugin_name": value.plugin_name.as_ref().map(|value| value.value.as_str()),
        "sanitized_remote": diagnostic_remote(value.sanitized_remote.as_deref()),
        "branch": value.branch,
        "revision": value.revision,
        "release_id": value.release_id,
        "target": value.target,
        "hint": value.hint,
        "build_log_path": value.build_log_path,
    })
}

pub(super) fn diagnostic_human(value: &oll::PluginDiagnostic) -> String {
    use std::fmt::Write as _;

    let mut output = format!("[{}:{}] {}", value.phase, value.code, value.message);
    if let Some(remote) = diagnostic_remote(value.sanitized_remote.as_deref()) {
        write!(output, " (remote: {remote})").expect("writing to String cannot fail");
    }
    if let Some(branch) = value.branch.as_deref() {
        write!(output, " (branch: {branch})").expect("writing to String cannot fail");
    }
    if let Some(revision) = value.revision.as_deref() {
        write!(output, " (revision: {revision})").expect("writing to String cannot fail");
    }
    if let Some(release_id) = value.release_id.as_deref() {
        write!(output, " (release: {release_id})").expect("writing to String cannot fail");
    }
    if let Some(target) = value.target.as_deref() {
        write!(output, " (target: {target})").expect("writing to String cannot fail");
    }
    if let Some(hint) = value.hint.as_deref() {
        write!(output, " (hint: {hint})").expect("writing to String cannot fail");
    }
    if let Some(build_log_path) = value.build_log_path.as_deref() {
        write!(output, " (build log: {build_log_path})").expect("writing to String cannot fail");
    }
    output
}

fn diagnostic_remote(value: Option<&str>) -> Option<String> {
    value.map(|remote| {
        remote
            .parse::<GitRemote>()
            .map(|remote| remote.to_string())
            .unwrap_or_else(|_| "<invalid-remote>".to_owned())
    })
}

pub(super) fn desired_state_name(value: i32) -> Result<&'static str, NodeError> {
    match oll::PluginDesiredState::try_from(value).unwrap_or(oll::PluginDesiredState::Unspecified) {
        oll::PluginDesiredState::Running => Ok("running"),
        oll::PluginDesiredState::Stopped => Ok("stopped"),
        oll::PluginDesiredState::Unspecified => Err(NodeError::Internal(
            "daemon returned an unspecified plugin desired state".to_owned(),
        )),
    }
}

pub(super) fn process_state_name(value: i32) -> Result<&'static str, NodeError> {
    match oll::PluginProcessState::try_from(value).unwrap_or(oll::PluginProcessState::Unspecified) {
        oll::PluginProcessState::Starting => Ok("starting"),
        oll::PluginProcessState::Ready => Ok("ready"),
        oll::PluginProcessState::Stopping => Ok("stopping"),
        oll::PluginProcessState::Exited => Ok("exited"),
        oll::PluginProcessState::Failed => Ok("failed"),
        oll::PluginProcessState::Unspecified => Err(NodeError::Internal(
            "daemon returned an unspecified plugin process state".to_owned(),
        )),
    }
}

pub(super) fn job_state_name(value: i32) -> Result<&'static str, NodeError> {
    match oll::PluginAdminJobState::try_from(value).unwrap_or(oll::PluginAdminJobState::Unspecified)
    {
        oll::PluginAdminJobState::Dispatching => Ok("dispatching"),
        oll::PluginAdminJobState::Running => Ok("running"),
        oll::PluginAdminJobState::Cancelling => Ok("cancelling"),
        oll::PluginAdminJobState::Succeeded => Ok("succeeded"),
        oll::PluginAdminJobState::Failed => Ok("failed"),
        oll::PluginAdminJobState::Cancelled => Ok("cancelled"),
        oll::PluginAdminJobState::TimedOut => Ok("timed_out"),
        oll::PluginAdminJobState::Unspecified => Err(NodeError::Internal(
            "daemon returned an unspecified plugin job state".to_owned(),
        )),
    }
}

pub(super) fn timestamp_json(
    value: Option<&prost_types::Timestamp>,
    field: &'static str,
) -> Result<Value, NodeError> {
    value
        .map(format_timestamp)
        .transpose()?
        .map(Value::String)
        .ok_or_else(|| NodeError::Internal(format!("daemon omitted {field}")))
}

pub(super) fn optional_timestamp_json(
    value: Option<&prost_types::Timestamp>,
) -> Result<Value, NodeError> {
    value
        .map(format_timestamp)
        .transpose()
        .map(|value| value.map_or(Value::Null, Value::String))
}

fn format_timestamp(timestamp: &prost_types::Timestamp) -> Result<String, NodeError> {
    let nanos = u32::try_from(timestamp.nanos).map_err(|_| {
        NodeError::Internal("daemon returned a timestamp with invalid nanoseconds".to_owned())
    })?;
    let time = time::OffsetDateTime::from_unix_timestamp(timestamp.seconds)
        .and_then(|time| time.replace_nanosecond(nanos))
        .map_err(|_| NodeError::Internal("daemon returned an invalid timestamp".to_owned()))?;
    time.format(&time::format_description::well_known::Rfc3339)
        .map_err(|_| NodeError::Internal("cannot format daemon timestamp".to_owned()))
}

pub(super) fn encode_hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;

    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(&mut encoded, "{byte:02x}").expect("writing to String cannot fail");
    }
    encoded
}

pub(super) fn config_value_json(value: &oll::ConfigValue) -> Result<Value, NodeError> {
    let Some(kind) = value.kind.as_ref() else {
        return Err(NodeError::Internal(
            "daemon returned ConfigValue without a kind".to_owned(),
        ));
    };
    match kind {
        config_value::Kind::NullValue(value)
            if *value == prost_types::NullValue::NullValue as i32 =>
        {
            Ok(Value::Null)
        }
        config_value::Kind::NullValue(_) => Err(NodeError::Internal(
            "daemon returned an unknown ConfigValue null value".to_owned(),
        )),
        config_value::Kind::BoolValue(value) => Ok(Value::Bool(*value)),
        config_value::Kind::IntegerValue(value) => Ok(Value::Number((*value).into())),
        config_value::Kind::NumberValue(value) => Number::from_f64(*value)
            .map(Value::Number)
            .ok_or_else(|| NodeError::Internal("daemon returned a non-finite number".to_owned())),
        config_value::Kind::StringValue(value) => Ok(Value::String(value.clone())),
        config_value::Kind::BytesValue(value) => Ok(json!({
            "encoding": "base64",
            "value": STANDARD_NO_PAD.encode(value),
        })),
        config_value::Kind::ListValue(value) => value
            .values
            .iter()
            .map(config_value_json)
            .collect::<Result<Vec<_>, _>>()
            .map(Value::Array),
        config_value::Kind::MapValue(value) => value
            .entries
            .iter()
            .map(|(key, value)| config_value_json(value).map(|value| (key.clone(), value)))
            .collect::<Result<Map<_, _>, _>>()
            .map(Value::Object),
        config_value::Kind::FunctionValue(value) => Ok(json!({
            "session_id": value.session_id,
            "function_id": value.function_id,
        })),
        config_value::Kind::TimestampValue(value) => format_timestamp(value).map(Value::String),
        config_value::Kind::DurationValue(value) => Ok(json!({
            "seconds": value.seconds,
            "nanos": value.nanos,
        })),
    }
}

pub(super) fn protocol_error_json(value: &oll::ProtocolError) -> Value {
    let code = oll::ErrorCode::try_from(value.code)
        .unwrap_or(oll::ErrorCode::Unspecified)
        .as_str_name()
        .trim_start_matches("ERROR_CODE_")
        .to_ascii_lowercase();
    let details = value
        .details
        .iter()
        .map(|detail| {
            json!({
                "type_url": detail.type_url,
                "value": STANDARD_NO_PAD.encode(&detail.value),
            })
        })
        .collect::<Vec<_>>();
    json!({
        "code": code,
        "message": value.message,
        "retryable": value.retryable,
        "metadata": value.metadata,
        "details": details,
    })
}

#[derive(Clone, Copy)]
pub(super) enum Tone {
    Plain,
    Success,
    Warning,
    Error,
}

pub(super) struct Cell {
    text: String,
    tone: Tone,
}

impl Cell {
    pub(super) fn plain(value: impl Into<String>) -> Self {
        Self {
            text: value.into(),
            tone: Tone::Plain,
        }
    }

    pub(super) fn toned(value: impl Into<String>, tone: Tone) -> Self {
        Self {
            text: value.into(),
            tone,
        }
    }
}

pub(super) fn table(headers: &[&str], rows: &[Vec<Cell>]) -> String {
    let mut widths = headers
        .iter()
        .map(|header| header.len())
        .collect::<Vec<_>>();
    for row in rows {
        for (index, cell) in row.iter().enumerate() {
            if let Some(width) = widths.get_mut(index) {
                *width = (*width).max(cell.text.chars().count());
            }
        }
    }

    let mut output = String::new();
    output.push_str(ANSI_BOLD_CYAN);
    append_row(
        &mut output,
        &headers
            .iter()
            .map(|value| Cell::plain(*value))
            .collect::<Vec<_>>(),
        &widths,
    );
    output.push_str(ANSI_RESET);
    for row in rows {
        append_row(&mut output, row, &widths);
    }
    output
}

pub(super) fn details_table(title: &str, value: &Value) -> String {
    let mut rows = Vec::new();
    flatten_details("", value, &mut rows);
    let rows = rows
        .into_iter()
        .map(|(field, value)| vec![Cell::plain(field), Cell::plain(value)])
        .collect::<Vec<_>>();
    let mut output = format!("{ANSI_BOLD_CYAN}{title}{ANSI_RESET}\n");
    output.push_str(&table(&["FIELD", "VALUE"], &rows));
    output
}

fn flatten_details(prefix: &str, value: &Value, output: &mut Vec<(String, String)>) {
    match value {
        Value::Object(fields) => {
            for (key, value) in fields {
                let field = if prefix.is_empty() {
                    key.clone()
                } else {
                    format!("{prefix}.{key}")
                };
                flatten_details(&field, value, output);
            }
        }
        Value::Null => output.push((prefix.to_owned(), "-".to_owned())),
        Value::String(value) => output.push((prefix.to_owned(), value.clone())),
        Value::Bool(value) => output.push((prefix.to_owned(), value.to_string())),
        Value::Number(value) => output.push((prefix.to_owned(), value.to_string())),
        Value::Array(value) => output.push((
            prefix.to_owned(),
            serde_json::to_string(value).expect("JSON value serialization cannot fail"),
        )),
    }
}

fn append_row(output: &mut String, row: &[Cell], widths: &[usize]) {
    use std::fmt::Write as _;

    for (index, cell) in row.iter().enumerate() {
        if index != 0 {
            output.push_str("  ");
        }
        let style = match cell.tone {
            Tone::Plain => "",
            Tone::Success => ANSI_GREEN,
            Tone::Warning => ANSI_YELLOW,
            Tone::Error => ANSI_RED,
        };
        output.push_str(style);
        output.push_str(&cell.text);
        if !style.is_empty() {
            output.push_str(ANSI_RESET);
        }
        if index + 1 < row.len() {
            let padding = widths[index].saturating_sub(cell.text.chars().count());
            write!(output, "{:padding$}", "").expect("writing to String cannot fail");
        }
    }
    output.push('\n');
}
