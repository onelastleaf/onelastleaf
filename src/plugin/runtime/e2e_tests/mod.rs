mod fake_plugin;
mod fixture;

use std::time::Duration;

use time::OffsetDateTime;
use tokio::time::Instant;

use crate::plugin::{
    DesiredPluginState, JobCancellationReason, JobState, ObservedPluginState, PluginOperationId,
};

use self::fixture::RuntimeFixture;

const TEST_DEADLINE: Duration = Duration::from_secs(30);

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn spawned_plugin_exercises_runtime_session_jobs_host_calls_logs_and_shutdown() {
    let deadline = Instant::now() + TEST_DEADLINE;
    tokio::time::timeout_at(deadline, async {
        let fixture = RuntimeFixture::start(fake_plugin::FAKE_PLUGIN_TEST).await;

        fixture
            .assert_desired_stopped_does_not_spawn(Instant::now() + Duration::from_millis(300))
            .await;

        let started = fixture
            .plugins
            .set_desired_state(
                &fixture.selector,
                DesiredPluginState::Running,
                "runtime-e2e-start",
            )
            .await
            .unwrap();
        assert_eq!(started.desired_state, DesiredPluginState::Running);
        let ready = fixture.wait_for_ready(deadline).await;
        assert_eq!(ready.state, ObservedPluginState::Ready);
        let process_id = ready.process_id.expect("spawned process ID");
        fixture
            .wait_for_document_content("updated by fake plugin", deadline)
            .await;

        let first_operation: PluginOperationId = "runtime-e2e-first-job".parse().unwrap();
        let second_operation: PluginOperationId = "runtime-e2e-second-job".parse().unwrap();
        let first = fixture.plugins.start_job(
            &fixture.selector,
            &first_operation,
            "hold".to_owned(),
            vec!["first".to_owned()],
            None,
            "runtime-e2e-first-job",
        );
        let second = fixture.plugins.start_job(
            &fixture.selector,
            &second_operation,
            "hold".to_owned(),
            vec!["second".to_owned()],
            None,
            "runtime-e2e-second-job",
        );
        let (first, second) = tokio::join!(first, second);
        let first = first.unwrap();
        let second = second.unwrap();
        if first.state != JobState::Running || second.state != JobState::Running {
            tokio::time::sleep(Duration::from_millis(100)).await;
            panic!(
                "concurrent jobs were not accepted: first={first:?}; second={second:?}; {}",
                fixture.diagnostic_logs()
            );
        }
        assert_eq!(first.state, JobState::Running, "first job: {first:?}");
        assert_eq!(second.state, JobState::Running, "second job: {second:?}");
        assert!(first.accepted_at.is_some());
        assert!(second.accepted_at.is_some());

        let fast_operation: PluginOperationId = "runtime-e2e-fast-job".parse().unwrap();
        let fast = fixture
            .plugins
            .start_job(
                &fixture.selector,
                &fast_operation,
                "hold".to_owned(),
                vec!["complete-immediately".to_owned()],
                None,
                "runtime-e2e-fast-job",
            )
            .await
            .unwrap();
        assert_eq!(
            fast.state,
            JobState::Running,
            "StartPluginJob must return its accepted snapshot even when completion follows immediately"
        );
        fixture
            .wait_for_job_state(fast.job_id, JobState::Succeeded, deadline)
            .await;
        let cleanup_deadline = Instant::now() + Duration::from_secs(1);
        while fixture
            .plugins
            .has_job_deadline_operation(fast.job_id)
            .await
        {
            assert!(
                Instant::now() < cleanup_deadline,
                "terminal job retained its 24-hour deadline operation"
            );
            tokio::task::yield_now().await;
        }

        let cancelled = fixture
            .plugins
            .stop_job(first.job_id, "runtime-e2e-cancel-first")
            .await
            .unwrap();
        assert!(matches!(
            cancelled.state,
            JobState::Cancelling | JobState::Cancelled
        ));
        fixture
            .wait_for_job_state(first.job_id, JobState::Cancelled, deadline)
            .await;
        let still_running = fixture.plugins.inspect_job(second.job_id).await.unwrap();
        assert_eq!(still_running.job.state, JobState::Running);
        let after_cancel = fixture
            .plugins
            .inspect_plugin(&fixture.selector)
            .await
            .unwrap()
            .process
            .expect("plugin process survived job cancellation");
        assert_eq!(after_cancel.state, ObservedPluginState::Ready);
        assert_eq!(after_cancel.instance_id, ready.instance_id);
        assert_eq!(after_cancel.process_id, Some(process_id));

        let stopped = fixture
            .plugins
            .set_desired_state(
                &fixture.selector,
                DesiredPluginState::Stopped,
                "runtime-e2e-stop",
            )
            .await
            .unwrap();
        assert_eq!(stopped.desired_state, DesiredPluginState::Stopped);
        fixture.wait_for_stopped(deadline).await;
        let stopped = fixture
            .plugins
            .inspect_plugin(&fixture.selector)
            .await
            .unwrap();
        assert_eq!(stopped.installed.desired_state, DesiredPluginState::Stopped);
        assert!(stopped.installed.last_lifecycle_failure.is_none());

        fixture.shutdown_and_verify_logs(deadline).await;
    })
    .await
    .expect("runtime E2E exceeded its absolute deadline");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn job_deadline_cancels_only_that_job_and_preserves_the_plugin_instance() {
    let deadline = Instant::now() + TEST_DEADLINE;
    tokio::time::timeout_at(deadline, async {
        let fixture = RuntimeFixture::start(fake_plugin::FAKE_PLUGIN_TEST).await;
        fixture
            .plugins
            .set_desired_state(
                &fixture.selector,
                DesiredPluginState::Running,
                "runtime-e2e-deadline-start",
            )
            .await
            .unwrap();
        let ready = fixture.wait_for_ready(deadline).await;
        let process_id = ready.process_id.expect("spawned process ID");
        fixture
            .wait_for_document_content("updated by fake plugin", deadline)
            .await;

        let survivor_operation: PluginOperationId =
            "runtime-e2e-deadline-survivor".parse().unwrap();
        let survivor = fixture
            .plugins
            .start_job(
                &fixture.selector,
                &survivor_operation,
                "hold".to_owned(),
                vec!["survivor".to_owned()],
                None,
                "runtime-e2e-deadline-survivor",
            )
            .await
            .unwrap();
        assert_eq!(survivor.state, JobState::Running);

        let expiring_operation: PluginOperationId =
            "runtime-e2e-deadline-expiring".parse().unwrap();
        let expiring = fixture
            .plugins
            .start_job(
                &fixture.selector,
                &expiring_operation,
                "hold".to_owned(),
                vec!["expect-deadline-cancel".to_owned()],
                Some(OffsetDateTime::now_utc() + time::Duration::seconds(2)),
                "runtime-e2e-deadline-expiring",
            )
            .await
            .unwrap();
        assert_eq!(expiring.state, JobState::Running);
        fixture
            .wait_for_job_state(expiring.job_id, JobState::TimedOut, deadline)
            .await;
        let expired = fixture.plugins.inspect_job(expiring.job_id).await.unwrap();
        assert_eq!(expired.job.state, JobState::TimedOut);
        assert_eq!(
            expired.job.cancellation_reason,
            Some(JobCancellationReason::Deadline)
        );
        assert!(expired.job.terminal_at.is_some());
        assert_eq!(expired.job.plugin_instance_id, ready.instance_id);

        let survivor = fixture.plugins.inspect_job(survivor.job_id).await.unwrap();
        assert_eq!(survivor.job.state, JobState::Running);
        let process = fixture
            .plugins
            .inspect_plugin(&fixture.selector)
            .await
            .unwrap()
            .process
            .expect("deadline cancellation must preserve the plugin process");
        assert_eq!(process.state, ObservedPluginState::Ready);
        assert_eq!(process.instance_id, ready.instance_id);
        assert_eq!(process.process_id, Some(process_id));

        fixture
            .plugins
            .set_desired_state(
                &fixture.selector,
                DesiredPluginState::Stopped,
                "runtime-e2e-deadline-stop",
            )
            .await
            .unwrap();
        fixture.wait_for_stopped(deadline).await;
        fixture.shutdown_and_verify_logs(deadline).await;
    })
    .await
    .expect("job-deadline E2E exceeded its absolute deadline");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn admitted_job_continues_after_the_calling_future_is_aborted() {
    let deadline = Instant::now() + TEST_DEADLINE;
    tokio::time::timeout_at(deadline, async {
        let fixture = RuntimeFixture::start(fake_plugin::FAKE_PLUGIN_TEST).await;
        fixture
            .plugins
            .set_desired_state(
                &fixture.selector,
                DesiredPluginState::Running,
                "runtime-e2e-abort-start",
            )
            .await
            .unwrap();
        fixture.wait_for_ready(deadline).await;
        fixture
            .wait_for_document_content("updated by fake plugin", deadline)
            .await;

        let operation_id: PluginOperationId = "runtime-e2e-aborted-job".parse().unwrap();
        let plugins = std::sync::Arc::clone(&fixture.plugins);
        let selector = fixture.selector.clone();
        let task_operation_id = operation_id.clone();
        let caller = tokio::spawn(async move {
            plugins
                .start_job(
                    &selector,
                    &task_operation_id,
                    "hold".to_owned(),
                    vec!["delay-accept".to_owned()],
                    None,
                    "runtime-e2e-aborted-job",
                )
                .await
        });
        loop {
            if fixture
                .store
                .job_by_operation_id(&operation_id)
                .await
                .is_ok_and(|job| job.state == JobState::Dispatching)
            {
                break;
            }
            if caller.is_finished() {
                panic!(
                    "start_job caller ended before the durable Dispatching boundary: {:?}",
                    caller.await
                );
            }
            assert!(Instant::now() < deadline, "job was never durably admitted");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        caller.abort();
        assert!(caller.await.unwrap_err().is_cancelled());
        let acceptance_deadline = (Instant::now() + Duration::from_secs(3)).min(deadline);
        let running = loop {
            let job = fixture.store.job_by_operation_id(&operation_id).await;
            if let Ok(job) = &job
                && job.state == JobState::Running
            {
                break job.clone();
            }
            assert!(
                Instant::now() < acceptance_deadline,
                "durable start continuation did not finish after caller abort; job={job:?}; {}",
                fixture.diagnostic_logs(),
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        };
        assert!(running.accepted_at.is_some());

        fixture
            .plugins
            .set_desired_state(
                &fixture.selector,
                DesiredPluginState::Stopped,
                "runtime-e2e-abort-stop",
            )
            .await
            .unwrap();
        fixture.wait_for_stopped(deadline).await;
        fixture.shutdown_and_verify_logs(deadline).await;
    })
    .await
    .expect("aborted-caller E2E exceeded its absolute deadline");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn held_package_gate_does_not_block_controller_state_changes() {
    let deadline = Instant::now() + TEST_DEADLINE;
    tokio::time::timeout_at(deadline, async {
        let fixture = RuntimeFixture::start(fake_plugin::FAKE_PLUGIN_TEST).await;
        let gate = fixture.hold_package_gate().await;
        fixture
            .plugins
            .set_desired_state(
                &fixture.selector,
                DesiredPluginState::Running,
                "runtime-e2e-gate-running",
            )
            .await
            .unwrap();
        fixture.supervisor_barrier(deadline).await;

        fixture
            .plugins
            .set_desired_state(
                &fixture.selector,
                DesiredPluginState::Stopped,
                "runtime-e2e-gate-stopped",
            )
            .await
            .unwrap();
        fixture.supervisor_barrier(deadline).await;
        assert_eq!(
            fixture
                .plugins
                .inspect_plugin(&fixture.selector)
                .await
                .unwrap()
                .installed
                .desired_state,
            DesiredPluginState::Stopped
        );

        drop(gate);
        fixture.supervisor_barrier(deadline).await;
        fixture.shutdown_without_plugin_process(deadline).await;
    })
    .await
    .expect("package-gate responsiveness E2E exceeded its absolute deadline");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nonreading_host_call_flood_cannot_block_process_teardown() {
    let deadline = Instant::now() + TEST_DEADLINE;
    tokio::time::timeout_at(deadline, async {
        let fixture = RuntimeFixture::start_with_no_read_flood(fake_plugin::FAKE_PLUGIN_TEST).await;
        fixture
            .plugins
            .set_desired_state(
                &fixture.selector,
                DesiredPluginState::Running,
                "runtime-e2e-no-read-running",
            )
            .await
            .unwrap();
        fixture.wait_for_ready(deadline).await;
        fixture
            .wait_for_document_content("updated by fake plugin", deadline)
            .await;
        tokio::time::sleep(Duration::from_millis(250)).await;

        let stop_deadline = (Instant::now() + Duration::from_secs(8)).min(deadline);
        fixture
            .plugins
            .set_desired_state(
                &fixture.selector,
                DesiredPluginState::Stopped,
                "runtime-e2e-no-read-stopped",
            )
            .await
            .unwrap();
        fixture.wait_for_stopped(stop_deadline).await;
        fixture.shutdown_without_plugin_process(deadline).await;
    })
    .await
    .expect("nonreading-plugin teardown E2E exceeded its absolute deadline");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn saturated_instance_work_queue_cannot_block_process_teardown() {
    let deadline = Instant::now() + TEST_DEADLINE;
    tokio::time::timeout_at(deadline, async {
        let fixture = RuntimeFixture::start(fake_plugin::FAKE_PLUGIN_TEST).await;
        fixture
            .plugins
            .set_desired_state(
                &fixture.selector,
                DesiredPluginState::Running,
                "runtime-e2e-saturated-running",
            )
            .await
            .unwrap();
        fixture.wait_for_ready(deadline).await;
        fixture
            .wait_for_document_content("updated by fake plugin", deadline)
            .await;

        let saturation = fixture.saturate_instance_work_queue().await;
        let stop_deadline = (Instant::now() + Duration::from_secs(8)).min(deadline);
        fixture
            .plugins
            .set_desired_state(
                &fixture.selector,
                DesiredPluginState::Stopped,
                "runtime-e2e-saturated-stopped",
            )
            .await
            .unwrap();
        fixture.wait_for_stopped(stop_deadline).await;
        drop(saturation);
        fixture.shutdown_without_plugin_process(deadline).await;
    })
    .await
    .expect("saturated-command-queue teardown E2E exceeded its absolute deadline");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn desired_running_plugin_restarts_with_bounded_backoff_after_unexpected_exit() {
    let deadline = Instant::now() + TEST_DEADLINE;
    tokio::time::timeout_at(deadline, async {
        let fixture =
            RuntimeFixture::start_with_exit_once(fake_plugin::FAKE_PLUGIN_TEST, true).await;
        fixture
            .plugins
            .set_desired_state(
                &fixture.selector,
                DesiredPluginState::Running,
                "runtime-e2e-restart-start",
            )
            .await
            .unwrap();
        let first = fixture.wait_for_ready(deadline).await;
        let restarted = fixture
            .wait_for_restarted_ready(first.instance_id, deadline)
            .await;
        assert_ne!(restarted.instance_id, first.instance_id);

        fixture
            .plugins
            .set_desired_state(
                &fixture.selector,
                DesiredPluginState::Stopped,
                "runtime-e2e-restart-stop",
            )
            .await
            .unwrap();
        fixture.wait_for_stopped(deadline).await;
        fixture.shutdown_and_verify_logs(deadline).await;
    })
    .await
    .expect("unexpected-exit E2E exceeded its absolute deadline");
}
