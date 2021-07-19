//! Functions for running Container state machines.
use crate::container::{patch_container_status, Status};
use crate::container::{Container, ContainerKey};
use crate::pod::Pod;
use chrono::Utc;
use futures::StreamExt;
use k8s_openapi::api::core::v1::Pod as KubePod;
use krator::{Manifest, ObjectState, SharedState, State, Transition};
use kube::api::Api;
use tracing::{debug, error, instrument, warn};
use tracing_futures::Instrument;

/// Prelude for Pod state machines.
pub mod prelude {
    pub use crate::container::{Container, Handle, Status};
    pub use krator::{Manifest, ObjectState, SharedState, State, Transition, TransitionTo};
}

/// Iteratively evaluate state machine until it returns Complete.
#[instrument(
    level = "info", 
    skip(
        client,
        initial_state,
        shared,
        container_state,
        pod,
        container_name
    ),
    fields(
        pod_name,
        namespace,
        container = %container_name
    )
)]
pub async fn run_to_completion<S: ObjectState<Manifest = Container, Status = Status>>(
    client: &kube::Client,
    initial_state: impl State<S>,
    shared: SharedState<S::SharedState>,
    mut container_state: S,
    pod: Manifest<Pod>,
    container_name: ContainerKey,
) -> anyhow::Result<()> {
    let initial_pod = pod.latest();
    let namespace = initial_pod.namespace().to_string();
    let pod_name = initial_pod.name().to_string();
    let api: Api<KubePod> = Api::namespaced(client.clone(), &namespace);

    let mut state: Box<dyn State<S>> = Box::new(initial_state);

    // Forward pod updates as container updates.
    let initial_container = match initial_pod.find_container(&container_name) {
        Some(container) => container,
        None => anyhow::bail!(
            "Unable to locate container {} in pod {} manifest.",
            container_name,
            pod_name
        ),
    };

    let (container_tx, container_rx) = Manifest::new(initial_container, pod.store.clone());
    let mut task_pod = pod.clone();
    let task_container_name = container_name.clone();
    tokio::spawn(
        async move {
            while let Some(latest_pod) = task_pod.next().await {
                let latest_container = match latest_pod.find_container(&task_container_name) {
                    Some(container) => container,
                    None => {
                        error!("Unable to locate container in pod manifest");
                        continue;
                    }
                };

                match container_tx.send(latest_container) {
                    Ok(()) => (),
                    Err(_) => {
                        debug!("Container update receiver hung up, exiting");
                        return;
                    }
                }
            }
        }
        .instrument(
            tracing::trace_span!("manifest_updater", %pod_name, %namespace, %container_name),
        ),
    );

    loop {
        debug!(?state, "Pod container entering state");

        let latest_pod = pod.latest();
        let latest_container = latest_pod.find_container(&container_name).unwrap();

        match state.status(&mut container_state, &latest_container).await {
            Ok(status) => {
                match patch_container_status(&api, &latest_pod, &container_name, &status).await {
                    Ok(_) => (),
                    Err(e) => {
                        warn!(
                            error = %e,
                            "Pod container status patch request returned error"
                        );
                    }
                }
            }
            Err(e) => {
                warn!(
                    error = %e,
                    "Pod container status patch returned error"
                );
            }
        }

        debug!(?state, "Pod container executing state handler");
        let transition = {
            state
                .next(shared.clone(), &mut container_state, container_rx.clone())
                .await
        };

        state = match transition {
            Transition::Next(s) => {
                let state = s.into();
                debug!(?state, "Pod container transitioning to state");
                state
            }
            Transition::Complete(result) => match result {
                Ok(()) => {
                    debug!("Pod container state machine exited without error");
                    break result;
                }
                Err(ref e) => {
                    error!(
                        error = %e,
                        "Pod container state machine exited with error"
                    );
                    let status = Status::Terminated {
                        timestamp: Utc::now(),
                        message: format!("Container exited with error: {:?}.", e),
                        failed: true,
                    };
                    patch_container_status(&api, &latest_pod, &container_name, &status)
                        .await
                        .unwrap();

                    break result;
                }
            },
        };
    }
}
