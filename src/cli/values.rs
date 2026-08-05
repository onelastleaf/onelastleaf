use std::{fmt, net::SocketAddr, str::FromStr};

use clap::ValueEnum;

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct NodeName(String);

impl NodeName {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for NodeName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl FromStr for NodeName {
    type Err = String;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let bytes = input.as_bytes();
        if !(1..=63).contains(&bytes.len()) {
            return Err("node name must be between 1 and 63 bytes".to_owned());
        }

        let is_alphanumeric = |byte: u8| byte.is_ascii_lowercase() || byte.is_ascii_digit();
        if !is_alphanumeric(bytes[0]) || !is_alphanumeric(bytes[bytes.len() - 1]) {
            return Err(
                "node name must start and end with a lowercase ASCII letter or digit".to_owned(),
            );
        }
        if !bytes
            .iter()
            .all(|byte| is_alphanumeric(*byte) || *byte == b'-')
        {
            return Err(
                "node name may contain only lowercase ASCII letters, digits, and hyphens"
                    .to_owned(),
            );
        }

        Ok(Self(input.to_owned()))
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct GitRemote {
    original: String,
    parsed: gix_url::Url,
}

impl GitRemote {
    /// Return the original spelling for Git. Diagnostics should use `Display`,
    /// which removes HTTP(S) user information and redacts other URL passwords.
    pub fn as_str(&self) -> &str {
        &self.original
    }

    fn redacted(&self) -> gix_url::Url {
        let mut parsed = self.parsed.clone();
        if matches!(
            parsed.scheme,
            gix_url::Scheme::Http | gix_url::Scheme::Https
        ) {
            // Access tokens are commonly placed in either user-info slot. The
            // username is useful for SSH diagnostics, but not for HTTP(S), so
            // omit both slots rather than guessing which one is a credential.
            parsed.user = None;
            parsed.password = None;
        }
        parsed
    }
}

impl fmt::Debug for GitRemote {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("GitRemote")
            .field(&format_args!("{}", self.redacted()))
            .finish()
    }
}

impl fmt::Display for GitRemote {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.redacted().fmt(formatter)
    }
}

impl FromStr for GitRemote {
    type Err = String;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        if input.is_empty() {
            return Err("Git remote cannot be empty".to_owned());
        }

        // gix-url parse errors may echo the complete input, including user
        // information. Keep malformed credential-bearing remotes out of CLI
        // and package diagnostics just as strictly as successfully parsed URLs.
        let parsed =
            gix_url::parse(input).map_err(|_| "Git remote syntax is invalid".to_owned())?;
        if !matches!(
            parsed.scheme,
            gix_url::Scheme::Git
                | gix_url::Scheme::Http
                | gix_url::Scheme::Https
                | gix_url::Scheme::Ssh
        ) {
            return Err(
                "Git remote must use git, http, https, ssh, or SCP-style SSH syntax".to_owned(),
            );
        }
        if parsed.host().is_none() {
            return Err("Git remote must include a host".to_owned());
        }
        if parsed.path.is_empty() || parsed.path.as_slice() == b"/" {
            return Err("Git remote must include a repository path".to_owned());
        }

        Ok(Self {
            original: input.to_owned(),
            parsed,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LoopbackAddr(SocketAddr);

impl LoopbackAddr {
    pub fn as_socket_addr(self) -> SocketAddr {
        self.0
    }
}

impl FromStr for LoopbackAddr {
    type Err = String;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let address = input
            .parse::<SocketAddr>()
            .map_err(|error| error.to_string())?;
        if !address.ip().is_loopback() {
            return Err("pingback address must use a loopback IP".to_owned());
        }
        Ok(Self(address))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogFilterDirective {
    pub target: LogTarget,
    pub level: LogFilterLevel,
}

impl FromStr for LogFilterDirective {
    type Err = String;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let Some((target, level)) = input.split_once('=') else {
            return Err("log filter directive must be TARGET=LEVEL".to_owned());
        };
        if level.contains('=') {
            return Err("log filter directive must contain exactly one '='".to_owned());
        }

        Ok(Self {
            target: target.parse()?,
            level: level.parse()?,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogTarget(String);

impl LogTarget {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for LogTarget {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl FromStr for LogTarget {
    type Err = String;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let mut segments = input.split("::");
        if segments.next() != Some("oll") {
            return Err("log target must begin with 'oll'".to_owned());
        }

        for segment in segments {
            let mut bytes = segment.bytes();
            let Some(first) = bytes.next() else {
                return Err("log target contains an empty identifier segment".to_owned());
            };
            if !(first.is_ascii_alphabetic() || first == b'_')
                || !bytes.all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
            {
                return Err(
                    "log target segments must be ASCII identifiers separated by '::'".to_owned(),
                );
            }
        }

        Ok(Self(input.to_owned()))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LogFilterLevel {
    Error,
    Warn,
    Info,
    Debug,
    Trace,
}

impl LogFilterLevel {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Error => "error",
            Self::Warn => "warn",
            Self::Info => "info",
            Self::Debug => "debug",
            Self::Trace => "trace",
        }
    }
}

impl fmt::Display for LogFilterLevel {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for LogFilterLevel {
    type Err = String;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        match input {
            "error" => Ok(Self::Error),
            "warn" => Ok(Self::Warn),
            "info" => Ok(Self::Info),
            "debug" => Ok(Self::Debug),
            "trace" => Ok(Self::Trace),
            _ => Err("log level must be error, warn, info, debug, or trace".to_owned()),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum OutputFormat {
    Text,
    Json,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, ValueEnum)]
pub enum ColorMode {
    #[default]
    Auto,
    Always,
    Never,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, ValueEnum)]
pub enum InitStore {
    #[default]
    Sqlite,
    Postgres,
}
