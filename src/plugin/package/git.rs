use std::{
    fs,
    path::{Path, PathBuf},
    process::ExitStatus,
};

use super::{
    GitSelection, PackageError, ProcessCancellation, ProcessOutcome, executable_exists,
    run_process_group,
};

#[derive(Clone, Debug)]
pub struct GitCheckout {
    pub source_root: PathBuf,
    pub commit: String,
}

pub async fn checkout_git_remote(
    remote: &str,
    selection: &GitSelection,
    source_root: &Path,
    build_log_path: &Path,
    cancellation: ProcessCancellation,
) -> Result<GitCheckout, PackageError> {
    fs::create_dir_all(source_root).map_err(|error| {
        PackageError::io(
            "git_fetch_failed",
            "git",
            "cannot create private Git checkout",
            error,
        )
    })?;
    if !executable_exists("git", source_root) {
        return Err(PackageError::new(
            "git_missing",
            "git",
            "the system git executable is not available through PATH",
        )
        .with_hint("Install Git and ensure it is available through PATH."));
    }
    run_git(
        &[
            "git".to_owned(),
            "init".to_owned(),
            "--quiet".to_owned(),
            path_text(source_root)?,
        ],
        source_root,
        build_log_path,
        cancellation.clone(),
    )
    .await?;
    run_git(
        &[
            "git".to_owned(),
            "remote".to_owned(),
            "add".to_owned(),
            "--".to_owned(),
            "origin".to_owned(),
            remote.to_owned(),
        ],
        source_root,
        build_log_path,
        cancellation.clone(),
    )
    .await?;
    let fetch = match selection {
        GitSelection::Default => vec![
            "git".to_owned(),
            "fetch".to_owned(),
            "--depth=1".to_owned(),
            "--".to_owned(),
            "origin".to_owned(),
            "HEAD".to_owned(),
        ],
        GitSelection::Branch(branch) => vec![
            "git".to_owned(),
            "fetch".to_owned(),
            "--depth=1".to_owned(),
            "--".to_owned(),
            "origin".to_owned(),
            branch.clone(),
        ],
        GitSelection::Revision(revision) => vec![
            "git".to_owned(),
            "fetch".to_owned(),
            "--".to_owned(),
            "origin".to_owned(),
            revision.clone(),
        ],
    };
    if let Err(failure) =
        run_git_process(&fetch, source_root, build_log_path, cancellation.clone()).await
    {
        if matches!(failure, GitCommandFailure::Exited(_))
            && selected_ref_is_absent(selection, source_root, build_log_path, cancellation.clone())
                .await
        {
            return Err(PackageError::new(
                "git_selection_not_found",
                "git",
                "Git could not resolve the selected branch",
            )
            .with_build_log(build_log_path.to_owned()));
        }
        return Err(failure.into_package_error(build_log_path));
    }
    run_git(
        &[
            "git".to_owned(),
            "checkout".to_owned(),
            "--quiet".to_owned(),
            "--detach".to_owned(),
            "FETCH_HEAD".to_owned(),
        ],
        source_root,
        build_log_path,
        cancellation,
    )
    .await?;
    let commit = read_detached_head(source_root)?;
    Ok(GitCheckout {
        source_root: source_root.to_owned(),
        commit,
    })
}

async fn run_git<S: AsRef<str>>(
    argv: &[S],
    cwd: &Path,
    build_log_path: &Path,
    cancellation: ProcessCancellation,
) -> Result<(), PackageError> {
    run_git_process(argv, cwd, build_log_path, cancellation)
        .await
        .map_err(|failure| failure.into_package_error(build_log_path))
}

enum GitCommandFailure {
    Exited(ExitStatus),
    Cancelled,
    Process,
}

impl GitCommandFailure {
    fn into_package_error(self, build_log_path: &Path) -> PackageError {
        let message = match self {
            Self::Exited(status) => match status.code() {
                Some(code) => format!("system Git exited unsuccessfully with status {code}"),
                None => "system Git terminated without an exit status".to_owned(),
            },
            Self::Cancelled => "Git operation was cancelled".to_owned(),
            Self::Process => "system Git process could not complete".to_owned(),
        };
        PackageError::new("git_fetch_failed", "git", message)
            .with_build_log(build_log_path.to_owned())
    }
}

async fn run_git_process<S: AsRef<str>>(
    argv: &[S],
    cwd: &Path,
    build_log_path: &Path,
    cancellation: ProcessCancellation,
) -> Result<(), GitCommandFailure> {
    let argv = argv
        .iter()
        .map(|value| value.as_ref().to_owned())
        .collect::<Vec<_>>();
    match run_process_group(&argv, cwd, build_log_path, cancellation)
        .await
        .map_err(|_| GitCommandFailure::Process)?
    {
        ProcessOutcome::Exited(status) if status.success() => Ok(()),
        ProcessOutcome::Exited(status) => Err(GitCommandFailure::Exited(status)),
        ProcessOutcome::Cancelled => Err(GitCommandFailure::Cancelled),
    }
}

async fn selected_ref_is_absent(
    selection: &GitSelection,
    source_root: &Path,
    build_log_path: &Path,
    cancellation: ProcessCancellation,
) -> bool {
    let pattern = match selection {
        GitSelection::Default => "HEAD".to_owned(),
        GitSelection::Branch(branch) => format!("refs/heads/{branch}"),
        GitSelection::Revision(_) => return false,
    };
    let probe = [
        "git".to_owned(),
        "ls-remote".to_owned(),
        "--exit-code".to_owned(),
        "--".to_owned(),
        "origin".to_owned(),
        pattern,
    ];
    matches!(
        run_git_process(&probe, source_root, build_log_path, cancellation).await,
        Err(GitCommandFailure::Exited(status)) if status.code() == Some(2)
    )
}

fn read_detached_head(source_root: &Path) -> Result<String, PackageError> {
    let head = fs::read_to_string(source_root.join(".git/HEAD")).map_err(|error| {
        PackageError::io(
            "git_fetch_failed",
            "git",
            "cannot read selected Git commit",
            error,
        )
    })?;
    let commit = head.trim_end_matches(['\r', '\n']);
    if !matches!(commit.len(), 40 | 64) || !commit.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(PackageError::new(
            "git_fetch_failed",
            "git",
            "selected Git checkout did not produce a detached commit",
        ));
    }
    Ok(commit.to_ascii_lowercase())
}

fn path_text(path: &Path) -> Result<String, PackageError> {
    path.to_str().map(str::to_owned).ok_or_else(|| {
        PackageError::new(
            "git_fetch_failed",
            "git",
            "private Git checkout path is not valid UTF-8",
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::{Command, Stdio};

    #[tokio::test]
    async fn ordinary_fetch_failure_is_not_misreported_as_missing_selection() {
        let directory = tempfile::TempDir::new().unwrap();
        let source_root = directory.path().join("checkout");
        let build_log = directory.path().join("build.log");
        let (_shutdown, cancellation) = ProcessCancellation::channel();

        let error = checkout_git_remote(
            "/definitely/not/an/oll/git/repository",
            &GitSelection::Branch("main".to_owned()),
            &source_root,
            &build_log,
            cancellation,
        )
        .await
        .unwrap_err();

        assert_eq!(error.code(), "git_fetch_failed");
        assert_eq!(error.phase(), "git");
    }

    #[tokio::test]
    async fn exact_revision_fetch_failure_is_not_reported_as_missing_selection() {
        let directory = tempfile::TempDir::new().unwrap();
        let source_root = directory.path().join("checkout");
        let build_log = directory.path().join("build.log");
        let (_shutdown, cancellation) = ProcessCancellation::channel();

        let error = checkout_git_remote(
            "/definitely/not/an/oll/git/repository",
            &GitSelection::Revision("0".repeat(40)),
            &source_root,
            &build_log,
            cancellation,
        )
        .await
        .unwrap_err();

        assert_eq!(error.code(), "git_fetch_failed");
        assert_eq!(error.phase(), "git");
    }

    #[tokio::test]
    async fn option_like_selections_remain_refspecs_instead_of_git_options() {
        let directory = tempfile::TempDir::new().unwrap();
        let remote = directory.path().join("remote.git");
        let status = Command::new("git")
            .args(["init", "--quiet", "--bare"])
            .arg(&remote)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .unwrap();
        assert!(status.success());

        for (name, selection, expected_code) in [
            (
                "help",
                GitSelection::Branch("--help".to_owned()),
                "git_selection_not_found",
            ),
            (
                "config",
                GitSelection::Revision("-cprotocol.version=2".to_owned()),
                "git_fetch_failed",
            ),
        ] {
            let source_root = directory.path().join(format!("checkout-{name}"));
            let build_log = directory.path().join(format!("{name}.log"));
            let (_shutdown, cancellation) = ProcessCancellation::channel();

            let error = checkout_git_remote(
                remote.to_str().unwrap(),
                &selection,
                &source_root,
                &build_log,
                cancellation,
            )
            .await
            .unwrap_err();

            assert_eq!(error.code(), expected_code);
            let output = fs::read_to_string(build_log).unwrap();
            assert!(!output.contains("usage: git fetch"));
            assert!(!output.contains("git fetch [<options>]"));
        }
    }
}
