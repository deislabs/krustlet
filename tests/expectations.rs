use k8s_openapi::api::core::v1::{ContainerState, ContainerStatus, Pod, PodStatus};
use kube::api::Api;

pub enum ContainerStatusExpectation<'a> {
    InitTerminated(&'a str, &'a str),
    InitNotPresent(&'a str),
    AppTerminated(&'a str, &'a str),
    AppNotPresent(&'a str),
}

impl ContainerStatusExpectation<'_> {
    fn verify_against(&self, pod_status: &PodStatus) -> anyhow::Result<()> {
        let container_statuses = match self {
            Self::InitTerminated(_, _) | Self::InitNotPresent(_) => {
                &pod_status.init_container_statuses
            }
            _ => &pod_status.container_statuses,
        };

        match self {
            Self::InitTerminated(container_name, expected)
            | Self::AppTerminated(container_name, expected) => {
                Self::verify_terminated(container_statuses, container_name, expected)
            }
            Self::InitNotPresent(container_name) | Self::AppNotPresent(container_name) => {
                Self::verify_not_present(container_statuses, container_name)
            }
        }
    }

    fn verify_terminated(
        actual_statuses: &[ContainerStatus],
        container_name: &str,
        expected: &str,
    ) -> anyhow::Result<()> {
        match actual_statuses.iter().find(|s| s.name == container_name) {
            None => Err(anyhow::anyhow!(
                "Expected {} present but it wasn't",
                container_name
            )),
            Some(status) => match &status.state {
                None => Err(anyhow::anyhow!(
                    "Expected {} to have state but it didn't",
                    container_name
                )),
                Some(state) => Self::verify_terminated_state(&state, container_name, expected),
            },
        }
    }

    fn verify_terminated_state(
        actual_state: &ContainerState,
        container_name: &str,
        expected: &str,
    ) -> anyhow::Result<()> {
        match &actual_state.terminated {
            None => Err(anyhow::anyhow!(
                "Expected {} terminated but was not",
                container_name
            )),
            Some(term_state) => match &term_state.message {
                None => Err(anyhow::anyhow!(
                    "Expected {} termination message was not set",
                    container_name
                )),
                Some(message) => {
                    if message == expected {
                        Ok(())
                    } else {
                        Err(anyhow::anyhow!(
                            "Expected {} termination message '{}' but was '{}'",
                            container_name,
                            expected,
                            message
                        ))
                    }
                }
            },
        }
    }

    fn verify_not_present(
        actual_statuses: &[ContainerStatus],
        container_name: &str,
    ) -> anyhow::Result<()> {
        match actual_statuses.iter().find(|s| s.name == container_name) {
            None => Ok(()),
            Some(_) => Err(anyhow::anyhow!(
                "Expected {} not present but it was",
                container_name
            )),
        }
    }
}

pub async fn assert_container_statuses(
    pods: &Api<Pod>,
    pod_name: &str,
    expectations: Vec<ContainerStatusExpectation<'_>>,
) -> anyhow::Result<()> {
    let pod = pods.get(pod_name).await?;

    let status = pod
        .status
        .ok_or_else(|| anyhow::anyhow!("Pod {} had no status", pod_name))?;

    for expectation in expectations {
        if let Err(e) = expectation.verify_against(&status) {
            panic!(
                "Pod {} status expectation failed: {}",
                pod_name,
                e.to_string()
            );
        }
    }

    Ok(())
}
