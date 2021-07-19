//! Container statuses

use super::Pod;
use crate::container::make_initial_container_status;
use k8s_openapi::api::core::v1::ContainerStatus as KubeContainerStatus;
use k8s_openapi::api::core::v1::Pod as KubePod;
use k8s_openapi::api::core::v1::PodCondition as KubePodCondition;
use krator::{Manifest, ObjectStatus};
use kube::api::PatchParams;
use kube::Api;
use tracing::{debug, instrument, warn};

/// Patch Pod status with Kubernetes API.
#[instrument(level = "info", skip(api, name, status), fields(pod_name = name))]
pub async fn patch_status(api: &Api<KubePod>, name: &str, status: Status) {
    let patch = status.json_patch();
    debug!(?patch, "Applying status patch to pod");
    match api
        .patch_status(
            &name,
            &PatchParams::default(),
            &kube::api::Patch::Strategic(patch),
        )
        .await
    {
        Ok(_) => (),
        Err(e) => {
            warn!(error = %e, "Error patching pod status");
        }
    }
}

const MAX_STATUS_INIT_RETRIES: usize = 5;

/// Initializes Pod container status array and wait for Pod reflection to update.
pub async fn initialize_pod_container_statuses(
    name: String,
    pod: Manifest<Pod>,
    api: &Api<KubePod>,
) -> anyhow::Result<()> {
    // NOTE: This loop patches the container statuses of the Pod with and then
    // waits for them to be picked up by the reflector. This is needed for a
    // few reasons:
    // * Kubernetes rewrites an empty array to null, preventing us from
    //   starting with that and appending.
    // * Pod reflection is not updated within a given state, meaning that
    //   container status patching cannot be responsible for initializing this
    //   (this would be a race condition anyway).
    // I'm not sure if we want to loop forever or handle some sort of failure
    // condition (if Kubernetes refuses to accept and propagate this
    // initialization patch.)
    let mut retries = 0;
    'main: loop {
        if retries == MAX_STATUS_INIT_RETRIES {
            let status = make_status(
                Phase::Failed,
                "Timed out while initializing container statuses.",
            );
            patch_status(&api, &name, status).await;
            anyhow::bail!("Timed out while initializing container statuses.")
        }
        let (num_containers, num_init_containers) = {
            let pod = pod.latest();
            patch_status(&api, &name, make_registered_status(&pod)).await;
            let num_containers = pod.containers().len();
            let num_init_containers = pod.init_containers().len();
            (num_containers, num_init_containers)
        };
        for _ in 0..10 {
            let status = pod
                .latest()
                .as_kube_pod()
                .status
                .clone()
                .unwrap_or_default();

            let num_statuses = status.container_statuses.len();
            let num_init_statuses = status.init_container_statuses.len();

            if (num_statuses == num_containers) && (num_init_statuses == num_init_containers) {
                break 'main Ok(());
            } else {
                debug!(pod_name = %name, ?status, "Pod waiting for status to populate");
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
        }
        retries += 1;
    }
}

/// Initialize Pod status.
/// This initializes Pod status to include containers in the correct order as expected by
/// `patch_container_status`.
pub fn make_registered_status(pod: &Pod) -> Status {
    let init_container_statuses: Vec<KubeContainerStatus> = pod
        .init_containers()
        .iter()
        .map(make_initial_container_status)
        .collect();
    let container_statuses: Vec<KubeContainerStatus> = pod
        .containers()
        .iter()
        .map(make_initial_container_status)
        .collect();
    make_status_with_containers(
        Phase::Pending,
        "Registered",
        container_statuses,
        init_container_statuses,
    )
}

/// Create basic Pod status patch.
pub fn make_status(phase: Phase, reason: &str) -> Status {
    StatusBuilder::new()
        .phase(phase)
        .reason(reason)
        .message(reason)
        .build()
}

/// Create basic Pod status patch.
pub fn make_status_with_containers(
    phase: Phase,
    reason: &str,
    container_statuses: Vec<KubeContainerStatus>,
    init_container_statuses: Vec<KubeContainerStatus>,
) -> Status {
    StatusBuilder::new()
        .phase(phase)
        .reason(reason)
        .container_statuses(container_statuses)
        .init_container_statuses(init_container_statuses)
        .build()
}

#[derive(Debug, Default)]
/// Pod Status wrapper.
pub struct Status {
    phase: Option<String>,
    reason: Option<String>,
    message: Option<String>,
    container_statuses: Option<Vec<KubeContainerStatus>>,
    init_container_statuses: Option<Vec<KubeContainerStatus>>,
    conditions: Option<Vec<KubePodCondition>>,
}

#[derive(Default)]
/// Builder for Pod Status wrapper.
pub struct StatusBuilder {
    phase: Option<String>,
    reason: Option<String>,
    message: Option<String>,
    container_statuses: Option<Vec<KubeContainerStatus>>,
    init_container_statuses: Option<Vec<KubeContainerStatus>>,
    conditions: Option<Vec<KubePodCondition>>,
}

impl StatusBuilder {
    /// Create a new status with no fields set.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set Pod phase.
    pub fn phase(mut self, phase: Phase) -> StatusBuilder {
        self.phase = Some(format!("{}", phase));
        self
    }

    /// Set Pod reason.
    pub fn reason(mut self, reason: &str) -> StatusBuilder {
        self.reason = Some(reason.to_string());
        self
    }

    /// Set Pod message.
    pub fn message(mut self, message: &str) -> StatusBuilder {
        self.message = Some(message.to_string());
        self
    }

    /// Set Pod container statuses.
    pub fn container_statuses(
        mut self,
        container_statuses: Vec<KubeContainerStatus>,
    ) -> StatusBuilder {
        self.container_statuses = Some(container_statuses);
        self
    }

    /// Set Pod init container statuses.
    pub fn init_container_statuses(
        mut self,
        init_container_statuses: Vec<KubeContainerStatus>,
    ) -> StatusBuilder {
        self.init_container_statuses = Some(init_container_statuses);
        self
    }

    /// Set Pod conditions.
    pub fn conditions(mut self, conditions: Vec<KubePodCondition>) -> StatusBuilder {
        self.conditions = Some(conditions);
        self
    }

    /// Finalize Pod Status from builder.
    pub fn build(self) -> Status {
        // NOTE: Right now this is basically the same as just implementing it on `Status` (i.e. they
        // have the same fields). We are retaining this builder for future API flexibility where we
        // might want to apply defaults or other transformations
        Status {
            phase: self.phase,
            reason: self.reason,
            message: self.message,
            container_statuses: self.container_statuses,
            init_container_statuses: self.init_container_statuses,
            conditions: self.conditions,
        }
    }
}

/// Describe the lifecycle phase of a workload.
///
/// This is specified by Kubernetes itself.
#[derive(Clone, Debug, serde::Serialize)]
pub enum Phase {
    /// The pod is being created.
    Pending,
    /// The workload is currently executing.
    Running,
    /// The workload has exited with an error.
    Failed,
    /// The workload has exited without error.
    Succeeded,
    /// The lifecycle phase of the workload cannot be determined.
    Unknown,
}

impl std::fmt::Display for Phase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", serde_json::json!(self).as_str().unwrap())
    }
}

impl Default for Phase {
    fn default() -> Self {
        Self::Unknown
    }
}

impl ObjectStatus for Status {
    fn json_patch(&self) -> serde_json::Value {
        let mut status = serde_json::Map::new();
        if let Some(s) = self.phase.clone() {
            status.insert("phase".to_string(), serde_json::Value::String(s));
        }

        if let Some(s) = self.message.clone() {
            status.insert("message".to_string(), serde_json::Value::String(s));
        }

        if let Some(s) = self.reason.clone() {
            status.insert("reason".to_string(), serde_json::Value::String(s));
        }

        // NOTE: We only insert the vecs if specified. Otherwise the merge patch will overwrite
        // things with empty vecs
        if let Some(s) = self.container_statuses.clone() {
            status.insert("containerStatuses".to_string(), serde_json::json!(s));
        }

        if let Some(s) = self.init_container_statuses.clone() {
            status.insert("initContainerStatuses".to_string(), serde_json::json!(s));
        }

        if let Some(c) = self.conditions.clone() {
            status.insert("conditions".to_string(), serde_json::json!(c));
        }

        serde_json::json!(
            {
                "metadata": {
                    "resourceVersion": "",
                },
                "status": serde_json::Value::Object(status)
            }
        )
    }

    fn failed(e: &str) -> Self {
        StatusBuilder::new()
            .phase(Phase::Failed)
            .message(e)
            .reason(e)
            .build()
    }
}
