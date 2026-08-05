use serde_json::{Map, Value};

use crate::protocol::oll;

pub(super) fn insert_trace_fields(fields: &mut Map<String, Value>, trace: &oll::TraceContext) {
    fields.insert(
        "correlation_id".to_owned(),
        Value::String(trace.correlation_id.clone()),
    );
    fields.insert("parent_call_id".to_owned(), trace.parent_call_id.into());
    fields.insert("call_depth".to_owned(), trace.call_depth.into());
    fields.insert("causal_depth".to_owned(), trace.causal_depth.into());
    fields.insert("task_id".to_owned(), trace.task_id.clone().into());
    fields.insert(
        "task_group_id".to_owned(),
        trace.task_group_id.clone().into(),
    );
}
