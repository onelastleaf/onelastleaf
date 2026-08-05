use std::{fmt, io, path::PathBuf};

#[derive(Debug, Eq, PartialEq)]
pub enum ConfigError {
    Read {
        path: PathBuf,
        kind: io::ErrorKind,
    },
    InvalidUtf8 {
        path: PathBuf,
    },
    RuntimeInitialization,
    Evaluation {
        path: PathBuf,
    },
    Schema {
        path: PathBuf,
        field: &'static str,
        problem: &'static str,
    },
}

impl fmt::Display for ConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Read { path, kind } => {
                write!(formatter, "cannot read {}: {kind}", path.display())
            }
            Self::InvalidUtf8 { path } => {
                write!(
                    formatter,
                    "{} is not valid UTF-8 Lua source",
                    path.display()
                )
            }
            Self::RuntimeInitialization => {
                formatter.write_str("cannot initialize the embedded LuaJIT configuration runtime")
            }
            Self::Evaluation { path } => write!(
                formatter,
                "cannot evaluate {}; check its Lua syntax and runtime operations",
                path.display()
            ),
            Self::Schema {
                path,
                field,
                problem,
            } => write!(
                formatter,
                "invalid configuration in {}\n  {field}: {problem}",
                path.display()
            ),
        }
    }
}
