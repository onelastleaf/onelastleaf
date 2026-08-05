use std::{
    collections::HashMap,
    io::{self, Write},
    sync::mpsc::{self, SyncSender, TrySendError},
    thread,
    time::{Duration, Instant},
};

use anstream::{AutoStream, ColorChoice};
use anstyle::{AnsiColor, Effects, Style};
use serde_json::Value;

use super::LogLevel;

const CONSOLE_QUEUE_CAPACITY: usize = 256;
const REPEAT_WINDOW: Duration = Duration::from_secs(30);
const MAX_REPEAT_KEYS: usize = 128;

pub(super) struct ForegroundConsole {
    sender: SyncSender<Vec<u8>>,
}

impl ForegroundConsole {
    pub(super) fn start(color: ColorChoice) -> Option<Self> {
        let (sender, receiver) = mpsc::sync_channel(CONSOLE_QUEUE_CAPACITY);
        match thread::Builder::new()
            .name("oll-foreground-output".to_owned())
            .spawn(move || {
                let output = AutoStream::new(io::stdout(), color).lock();
                if let Err(error) = ConsolePresenter::new(output).run(receiver) {
                    eprintln!("oll foreground output stopped: {error}");
                }
            }) {
            Ok(_) => Some(Self { sender }),
            Err(error) => {
                eprintln!("oll could not start foreground output: {error}");
                None
            }
        }
    }

    pub(super) fn enqueue(&self, level: LogLevel, event: &str, encoded: &[u8]) {
        if level < LogLevel::Warn && (level != LogLevel::Info || !visible_info_event(event)) {
            return;
        }
        match self.sender.try_send(encoded.to_vec()) {
            Ok(()) | Err(TrySendError::Full(_)) | Err(TrySendError::Disconnected(_)) => {}
        }
    }
}

struct ConsolePresenter<W> {
    output: W,
    repeats: HashMap<String, RepeatState>,
}

struct RepeatState {
    last_printed: Instant,
    suppressed: u64,
}

struct ConsoleLine {
    timestamp: String,
    level: LogLevel,
    component: String,
    suppression_scope: String,
    text: String,
}

impl<W> ConsolePresenter<W>
where
    W: Write,
{
    fn new(output: W) -> Self {
        Self {
            output,
            repeats: HashMap::new(),
        }
    }

    fn run(&mut self, receiver: mpsc::Receiver<Vec<u8>>) -> io::Result<()> {
        for encoded in receiver {
            let Ok(record) = serde_json::from_slice::<Value>(&encoded) else {
                continue;
            };
            self.present(&record, Instant::now())?;
        }
        self.output.flush()
    }

    fn present(&mut self, record: &Value, now: Instant) -> io::Result<()> {
        let Some(mut line) = ConsoleLine::from_record(record) else {
            return Ok(());
        };
        if clears_failures(record) {
            self.repeats
                .retain(|key, _| !key.starts_with(&line.suppression_scope));
        }
        if line.level >= LogLevel::Warn {
            let key = line.signature();
            if let Some(state) = self.repeats.get_mut(&key) {
                if now.duration_since(state.last_printed) < REPEAT_WINDOW {
                    state.suppressed = state.suppressed.saturating_add(1);
                    return Ok(());
                }
                if state.suppressed != 0 {
                    line.text.push_str(&format!(
                        " ({} equivalent events suppressed)",
                        state.suppressed
                    ));
                }
                state.last_printed = now;
                state.suppressed = 0;
            } else {
                if self.repeats.len() >= MAX_REPEAT_KEYS
                    && let Some(oldest) = self
                        .repeats
                        .iter()
                        .min_by_key(|(_, state)| state.last_printed)
                        .map(|(key, _)| key.clone())
                {
                    self.repeats.remove(&oldest);
                }
                self.repeats.insert(
                    key,
                    RepeatState {
                        last_printed: now,
                        suppressed: 0,
                    },
                );
            }
        }
        write_line(&mut self.output, &line)?;
        self.output.flush()
    }
}

impl ConsoleLine {
    fn from_record(record: &Value) -> Option<Self> {
        let level = match record.get("level")?.as_str()? {
            "INFO" => LogLevel::Info,
            "WARN" => LogLevel::Warn,
            "ERROR" => LogLevel::Error,
            _ => return None,
        };
        let event = record.get("event")?.as_str()?;
        if level == LogLevel::Info && !visible_info_event(event) {
            return None;
        }
        let target = record.get("target")?.as_str()?;
        let component = target
            .strip_prefix("oll::")
            .and_then(|target| target.split("::").next())
            .unwrap_or(target)
            .to_owned();
        let timestamp = record
            .get("timestamp")
            .and_then(Value::as_str)
            .and_then(|timestamp| timestamp.get(11..19))
            .map(|timestamp| format!("{timestamp}Z"))
            .unwrap_or_else(|| "--:--:--Z".to_owned());
        let mut text = headline(record, event);
        if let Some(message) = record.get("message").and_then(Value::as_str)
            && !message.is_empty()
        {
            let message = terminal_text(message);
            if !text.contains(&message) {
                text.push_str(" — ");
                text.push_str(&message);
            }
        }
        let context = context(record);
        if !context.is_empty() {
            text.push_str("  [");
            text.push_str(&context.join(" · "));
            text.push(']');
        }
        Some(Self {
            timestamp,
            level,
            suppression_scope: suppression_scope(record, &component),
            component,
            text,
        })
    }

    fn signature(&self) -> String {
        format!(
            "{}{}\0{}",
            self.suppression_scope,
            self.level.as_str(),
            self.text
        )
    }
}

fn suppression_scope(record: &Value, component: &str) -> String {
    let discriminator = match component {
        "sync" => scalar(record.get("connect_target")),
        "plugin" => scalar(record.get("plugin_id")).or_else(|| scalar(record.get("plugin_name"))),
        _ => None,
    };
    format!("{component}\0{}\0", discriminator.unwrap_or_default())
}

fn visible_info_event(event: &str) -> bool {
    matches!(
        event,
        "node_starting"
            | "node_ready"
            | "node_identity_updated"
            | "node_shutdown_requested"
            | "node_shutdown_completed"
            | "replica_identity_updated"
            | "replica_projection_recovery_started"
            | "replica_projection_recovery_completed"
            | "snapshot_export_started"
            | "snapshot_export_completed"
            | "snapshot_import_started"
            | "snapshot_import_completed"
            | "sync_session_ready"
            | "sync_session_waiting_for_replica"
            | "sync_session_closed"
            | "sync_bootstrap_started"
            | "sync_bootstrap_completed"
            | "sync_bootstrap_cancelled"
            | "sync_round_started"
            | "sync_round_completed"
            | "plugin_system_ready"
            | "plugin_system_stopped"
            | "plugin_desired_state_changed"
            | "plugin_process_starting"
            | "plugin_process_ready"
            | "plugin_process_exited"
            | "plugin_process_stopped"
            | "plugin_restart_requested"
            | "plugin_restart_scheduled"
            | "plugin_shutdown_requested"
            | "plugin_package_operation_started"
            | "plugin_package_operation_completed"
            | "plugin_artifact_published"
            | "plugin_job_dispatch_started"
            | "plugin_job_terminal_update"
    )
}

fn clears_failures(record: &Value) -> bool {
    record
        .get("event")
        .and_then(Value::as_str)
        .is_some_and(|event| {
            matches!(
                event,
                "node_ready"
                    | "node_shutdown_completed"
                    | "sync_session_ready"
                    | "sync_session_waiting_for_replica"
                    | "sync_bootstrap_completed"
                    | "sync_round_completed"
                    | "snapshot_export_completed"
                    | "snapshot_import_completed"
                    | "plugin_process_ready"
                    | "plugin_process_stopped"
            )
        })
}

fn headline(record: &Value, event: &str) -> String {
    match event {
        "node_starting" => format!(
            "starting oll as {}",
            field(record, "node_name").unwrap_or_else(|| "unknown node".to_owned())
        ),
        "node_ready" => format!(
            "oll is ready as {}",
            field(record, "node_name").unwrap_or_else(|| "unknown node".to_owned())
        ),
        "node_shutdown_requested" => "shutdown requested".to_owned(),
        "node_shutdown_completed" => "shutdown completed".to_owned(),
        "weak_network_key_configured" => {
            "network key is shorter than 32 bytes; generate a stronger key with `oll psk`"
                .to_owned()
        }
        "sync_connect_failed" => "could not connect to sync peer".to_owned(),
        "sync_session_failed" => "sync handshake failed".to_owned(),
        "sync_session_ready" => match field(record, "remote_node_name") {
            Some(peer) => format!("connected to {peer}"),
            None => "sync session ready".to_owned(),
        },
        "sync_session_waiting_for_replica" => match field(record, "remote_node_name") {
            Some(peer) => format!("connected to {peer}; waiting for a replica"),
            None => "connected to sync peer; waiting for a replica".to_owned(),
        },
        "sync_session_closed" => match field(record, "remote_node_name") {
            Some(peer) => format!("connection to {peer} closed"),
            None => "sync session closed".to_owned(),
        },
        _ => humanize_event(event),
    }
}

fn humanize_event(event: &str) -> String {
    let event = ["node_", "replica_", "snapshot_", "sync_", "plugin_"]
        .into_iter()
        .find_map(|prefix| event.strip_prefix(prefix))
        .unwrap_or(event);
    event.replace('_', " ")
}

fn context(record: &Value) -> Vec<String> {
    let mut context = Vec::new();
    for (field_name, label) in [
        ("connect_target", "peer"),
        ("plugin_name", "plugin"),
        ("plugin_id", "plugin_id"),
        ("job_id", "job"),
        ("path", "path"),
        ("config_root", "config"),
        ("configured_listen_address", "listen"),
        ("process_id", "pid"),
        ("reason", "reason"),
        ("error_code", "error"),
        ("error_kind", "error_kind"),
        ("replica_id", "replica"),
        ("duration_ms", "duration_ms"),
    ] {
        if let Some(value) = scalar(record.get(field_name)) {
            context.push(format!("{label} {value}"));
        }
    }
    context
}

fn field(record: &Value, name: &str) -> Option<String> {
    record.get(name).and_then(Value::as_str).map(terminal_text)
}

fn scalar(value: Option<&Value>) -> Option<String> {
    match value? {
        Value::String(value) if !value.is_empty() => Some(terminal_text(value)),
        Value::Number(value) => Some(value.to_string()),
        _ => None,
    }
}

fn terminal_text(value: &str) -> String {
    let Ok(encoded) = serde_json::to_string(value) else {
        return "<unprintable>".to_owned();
    };
    encoded[1..encoded.len() - 1].to_owned()
}

fn write_line(output: &mut impl Write, line: &ConsoleLine) -> io::Result<()> {
    let timestamp = Style::new().dimmed();
    let component = AnsiColor::Cyan.on_default().effects(Effects::BOLD);
    let level = match line.level {
        LogLevel::Info => AnsiColor::Green.on_default(),
        LogLevel::Warn => AnsiColor::Yellow.on_default().effects(Effects::BOLD),
        LogLevel::Error => AnsiColor::Red.on_default().effects(Effects::BOLD),
        LogLevel::Trace | LogLevel::Debug => Style::new(),
    };
    writeln!(
        output,
        "{timestamp}{}{timestamp:#}  {level}{:<5}{level:#}  {component}{:<8}{component:#}  {}",
        line.timestamp,
        line.level.as_str(),
        line.component,
        line.text
    )
}

#[cfg(test)]
mod tests;
