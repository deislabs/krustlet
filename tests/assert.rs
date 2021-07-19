use futures::TryStreamExt;
use k8s_openapi::api::core::v1::Pod;
use kube::api::{Api, LogParams};

pub async fn pod_log_equals(
    pods: &Api<Pod>,
    pod_name: &str,
    expected_log: &str,
) -> anyhow::Result<()> {
    let mut logs = pods.log_stream(pod_name, &LogParams::default()).await?;

    while let Some(chunk) = logs.try_next().await? {
        assert_eq!(expected_log, String::from_utf8_lossy(&chunk));
    }

    Ok(())
}

pub async fn pod_log_contains(
    pods: &Api<Pod>,
    pod_name: &str,
    expected_log: &str,
) -> anyhow::Result<()> {
    let logs = pods.logs(pod_name, &LogParams::default()).await?;
    assert!(
        logs.contains(expected_log),
        "Expected log containing {} but got {}",
        expected_log,
        logs,
    );
    Ok(())
}

pub async fn pod_container_log_contains(
    pods: &Api<Pod>,
    pod_name: &str,
    container_name: &str,
    expected_log: &str,
) -> anyhow::Result<()> {
    let log_params = LogParams {
        container: Some(container_name.to_owned()),
        ..Default::default()
    };
    let logs = pods.logs(pod_name, &log_params).await?;
    assert!(
        logs.contains(expected_log),
        "Expected log containing {} but got {}",
        expected_log,
        logs
    );
    Ok(())
}

pub async fn pod_exited_successfully(pods: &Api<Pod>, pod_name: &str) -> anyhow::Result<()> {
    let pod = pods.get(pod_name).await?;

    let state = (|| {
        pod.status?.container_statuses[0]
            .state
            .as_ref()?
            .terminated
            .clone()
    })()
    .expect("Could not fetch terminated states");
    if state.exit_code != 0 {
        try_dump_pod_logs(pods, pod_name).await;
        assert_eq!(state.exit_code, 0);
    }

    Ok(())
}

pub async fn pod_exited_with_failure(pods: &Api<Pod>, pod_name: &str) -> anyhow::Result<()> {
    let pod = pods.get(pod_name).await?;

    let phase = (|| pod.status?.phase)().expect("Could not get pod phase");
    assert_eq!(phase, "Failed");

    Ok(())
}

pub async fn pod_reason_contains(
    pods: &Api<Pod>,
    pod_name: &str,
    expected_message: &str,
) -> anyhow::Result<()> {
    let pod = pods.get(pod_name).await?;

    let message = (|| pod.status?.reason)().expect("Could not get pod message.");
    assert!(
        message.contains(expected_message),
        "Expected pod message containing {} but got {}",
        expected_message,
        message,
    );

    Ok(())
}

pub async fn main_container_exited_with_failure(
    pods: &Api<Pod>,
    pod_name: &str,
) -> anyhow::Result<()> {
    let pod = pods.get(pod_name).await?;

    let state = (|| {
        pod.status?.container_statuses[0]
            .state
            .as_ref()?
            .terminated
            .clone()
    })()
    .expect("Could not fetch terminated states");
    assert_eq!(state.exit_code, 1);

    Ok(())
}

pub async fn container_file_contains(
    pod_name: &str,
    pod_namespace: &str,
    container_file_path: &str,
    expected_content: &str,
    file_error: &str,
) -> anyhow::Result<()> {
    let pod_dir_name = format!("{}-{}", pod_name, pod_namespace);
    let file_path_base = data_dir().join("volumes").join(pod_dir_name);
    let container_file_bytes = tokio::fs::read(file_path_base.join(container_file_path))
        .await
        .expect(file_error);
    assert_eq!(
        expected_content.to_owned().into_bytes(),
        container_file_bytes
    );
    Ok(())
}

fn data_dir() -> std::path::PathBuf {
    match std::env::var("KRUSTLET_DATA_DIR") {
        Ok(data_dir) => std::path::PathBuf::from(data_dir),
        _ => {
            let home_dir = dirs::home_dir().expect("Can't get home dir");
            home_dir.join(".krustlet")
        }
    }
}

pub async fn try_dump_pod_logs(pods: &Api<Pod>, pod_name: &str) {
    let logs = pods.logs(pod_name, &LogParams::default()).await;

    match logs {
        Err(e) => {
            println!("Unable to dump logs for {}: {}", pod_name, e);
        }
        Ok(content) => {
            println!("--- BEGIN LOGS for pod {}", pod_name);
            println!("{}", content);
            println!("--- END LOGS for pod {}", pod_name);
        }
    }
}
