use time::{Duration, OffsetDateTime};

use super::super::PluginError;
use super::{PluginId, PluginInstanceId, PluginJobId, PluginOperationId};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JobState {
    Dispatching,
    Running,
    Cancelling,
    Succeeded,
    Failed,
    Cancelled,
    TimedOut,
}

impl JobState {
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Succeeded | Self::Failed | Self::Cancelled | Self::TimedOut
        )
    }

    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Dispatching => "dispatching",
            Self::Running => "running",
            Self::Cancelling => "cancelling",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
            Self::TimedOut => "timed_out",
        }
    }

    pub(crate) fn parse(value: &str) -> Result<Self, PluginError> {
        match value {
            "dispatching" => Ok(Self::Dispatching),
            "running" => Ok(Self::Running),
            "cancelling" => Ok(Self::Cancelling),
            "succeeded" => Ok(Self::Succeeded),
            "failed" => Ok(Self::Failed),
            "cancelled" => Ok(Self::Cancelled),
            "timed_out" => Ok(Self::TimedOut),
            _ => Err(PluginError::CorruptStore(
                "plugin job state is invalid".to_owned(),
            )),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JobCancellationReason {
    UserRequest,
    Deadline,
}

impl JobCancellationReason {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::UserRequest => "user_request",
            Self::Deadline => "deadline",
        }
    }

    pub(crate) fn parse(value: &str) -> Result<Self, PluginError> {
        match value {
            "user_request" => Ok(Self::UserRequest),
            "deadline" => Ok(Self::Deadline),
            _ => Err(PluginError::CorruptStore(
                "plugin job cancellation reason is invalid".to_owned(),
            )),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum JobDeadline {
    Default24Hours,
    Explicit(OffsetDateTime),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NormalizedJobPayload {
    pub plugin_id: PluginId,
    pub action: String,
    pub arguments: Vec<String>,
    pub deadline: JobDeadline,
}

impl NormalizedJobPayload {
    pub fn new(
        plugin_id: PluginId,
        action: String,
        arguments: Vec<String>,
        deadline: Option<OffsetDateTime>,
    ) -> Result<Self, PluginError> {
        if action.is_empty() {
            return Err(PluginError::InvalidArgument(
                "plugin action must not be empty".to_owned(),
            ));
        }
        Ok(Self {
            plugin_id,
            action,
            arguments,
            deadline: deadline.map_or(JobDeadline::Default24Hours, JobDeadline::Explicit),
        })
    }

    pub fn absolute_deadline(&self, admitted_at: OffsetDateTime) -> OffsetDateTime {
        match self.deadline {
            JobDeadline::Default24Hours => admitted_at + Duration::hours(24),
            JobDeadline::Explicit(deadline) => deadline,
        }
    }

    pub(crate) fn canonical_bytes(&self) -> Vec<u8> {
        fn field(output: &mut Vec<u8>, bytes: &[u8]) {
            output.extend_from_slice(&u64::try_from(bytes.len()).unwrap_or(u64::MAX).to_be_bytes());
            output.extend_from_slice(bytes);
        }
        let mut output = Vec::new();
        field(&mut output, self.plugin_id.as_str().as_bytes());
        field(&mut output, self.action.as_bytes());
        output.extend_from_slice(
            &u64::try_from(self.arguments.len())
                .unwrap_or(u64::MAX)
                .to_be_bytes(),
        );
        for argument in &self.arguments {
            field(&mut output, argument.as_bytes());
        }
        match self.deadline {
            JobDeadline::Default24Hours => output.push(0),
            JobDeadline::Explicit(deadline) => {
                output.push(1);
                output.extend_from_slice(&deadline.unix_timestamp().to_be_bytes());
                output.extend_from_slice(&deadline.nanosecond().to_be_bytes());
            }
        }
        output
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PluginJob {
    pub job_id: PluginJobId,
    pub operation_id: PluginOperationId,
    pub payload: NormalizedJobPayload,
    pub absolute_deadline: OffsetDateTime,
    pub state: JobState,
    pub cancellation_reason: Option<JobCancellationReason>,
    pub plugin_instance_id: PluginInstanceId,
    pub admitted_at: OffsetDateTime,
    pub accepted_at: Option<OffsetDateTime>,
    pub terminal_at: Option<OffsetDateTime>,
    pub updated_at: OffsetDateTime,
    pub correlation_id: String,
    pub result: Option<Vec<u8>>,
    pub error_code: Option<String>,
    pub error_message: Option<String>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PluginJobCounts {
    pub dispatching: u64,
    pub running: u64,
    pub cancelling: u64,
    pub succeeded: u64,
    pub failed: u64,
    pub cancelled: u64,
    pub timed_out: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum JobAdmission {
    Created(PluginJob),
    Existing(PluginJob),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JobCancellation {
    pub job: PluginJob,
    pub send_request: bool,
}

impl JobCancellation {
    pub(crate) fn needs_request_dispatch(&self) -> bool {
        self.send_request && self.job.state == JobState::Cancelling
    }
}
