use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use k8s_openapi::api::core::v1::Pod as KubePod;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use kube::api::Meta;
use kube::Client as KubeClient;
use kube_runtime::watcher::Event;
use log::{debug, error, warn};
use tokio::sync::RwLock;

use crate::pod::{Phase, Pod, PodKey};
use crate::provider::Provider;
use crate::state::{run_to_completion, AsyncDrop};

/// A per-pod queue that takes incoming Kubernetes events and broadcasts them to the correct queue
/// for that pod.
///
/// It will also send a error out on the given sender that can be handled in another process (namely
/// the main kubelet process). This queue will only handle the latest update. So if a modify comes
/// in while it is still handling a create and then another modify comes in after, only the second
/// modify will be handled, which is ok given that each event contains the whole pod object
pub(crate) struct Queue<P> {
    provider: Arc<P>,
    handlers: HashMap<PodKey, tokio::sync::mpsc::Sender<Event<KubePod>>>,
    client: KubeClient,
}

impl<P: 'static + Provider + Sync + Send> Queue<P> {
    pub fn new(provider: Arc<P>, client: KubeClient) -> Self {
        Queue {
            provider,
            handlers: HashMap::new(),
            client,
        }
    }

    async fn run_pod(
        &self,
        initial_event: Event<KubePod>,
    ) -> anyhow::Result<tokio::sync::mpsc::Sender<Event<KubePod>>> {
        let (sender, mut receiver) = tokio::sync::mpsc::channel::<Event<KubePod>>(16);

        let pod_deleted = Arc::new(RwLock::new(false));

        match initial_event {
            Event::Applied(pod) => {
                let pod = Pod::from(pod);
                let pod_state = self.provider.initialize_pod_state(&pod).await?;
                tokio::spawn(start_task::<P>(
                    self.client.clone(),
                    pod,
                    pod_state,
                    Arc::clone(&pod_deleted),
                ));
            }
            _ => return Err(anyhow::anyhow!("Got non-apply event when starting pod")),
        }

        tokio::spawn(async move {
            while let Some(event) = receiver.recv().await {
                // Watch errors are handled before an event ever gets here, so it should always have
                // a pod
                match event {
                    Event::Applied(pod) => {
                        // Not really using this right now but will be useful for detecting changes.
                        let pod = Pod::from(pod);
                        debug!("Pod {} applied.", pod.name());

                        // TODO, detect other changes we want to support, or should this just forward the new pod def to state machine?
                        if let Some(_timestamp) = pod.deletion_timestamp() {
                            *(pod_deleted.write().await) = true;
                        }
                    }
                    Event::Deleted(pod) => {
                        // I'm not sure if this matters, we get notified of pod deletion with a
                        // Modified event, and I think we only get this after *we* delete the pod.
                        // There is the case where someone force deletes, but we want to go through
                        // our normal terminate and deregister flow anyway.
                        debug!("Pod {} deleted.", Pod::from(pod).name());
                        *(pod_deleted.write().await) = true;
                        break;
                    }
                    _ => warn!("Pod got unexpected event, ignoring: {:?}", &event),
                }
            }
        });
        Ok(sender)
    }

    pub async fn enqueue(&mut self, event: Event<KubePod>) -> anyhow::Result<()> {
        match &event {
            Event::Applied(pod) => {
                let key = PodKey::from(pod);
                // We are explicitly not using the entry api here to insert to avoid the need for a
                // mutex
                let sender = match self.handlers.get_mut(&key) {
                    Some(s) => {
                        debug!("Found existing event handler for pod {}", pod.name());
                        s
                    }
                    None => {
                        debug!("Creating event handler for pod {}", pod.name());
                        self.handlers.insert(
                            key.clone(),
                            // TODO Do we want to capture join handles? Worker wasnt using them.
                            // TODO: This ends up sending the initial event twice
                            // TODO How do we drop this sender / handler?
                            self.run_pod(event.clone()).await?,
                        );
                        self.handlers.get_mut(&key).unwrap()
                    }
                };
                match sender.send(event).await {
                    Ok(_) => debug!(
                        "successfully sent event to handler for pod {} in namespace {}",
                        key.name(),
                        key.namespace()
                    ),
                    Err(e) => error!(
                        "error while sending event. Will retry on next event: {:?}",
                        e
                    ),
                }
                Ok(())
            }
            Event::Deleted(pod) => {
                let key = PodKey::from(pod);
                if let Some(mut sender) = self.handlers.remove(&key) {
                    sender.send(event).await?;
                }
                Ok(())
            }
            // Restarted should not be passed to this function, it should be passed to resync instead
            Event::Restarted(_) => {
                warn!("Got a restarted event. Restarted events should be resynced with the queue");
                Ok(())
            }
        }
    }
    /// Resyncs the queue given the list of pods. Pods that exist in the queue but no longer exist
    /// in the list will be deleted
    // TODO: I really don't like having handle the resync at the kubelet level with this function,
    // but we can't do async recursion without boxing, which causes other issues
    pub async fn resync(&mut self, pods: Vec<KubePod>) -> anyhow::Result<()> {
        // First reconcile any deleted pods we might have missed (if it exists in our map, but not
        // in the list)
        let current_pods: HashSet<PodKey> = pods.iter().map(PodKey::from).collect();
        let pods_in_state: HashSet<PodKey> = self.handlers.keys().cloned().collect();
        for pk in pods_in_state.difference(&current_pods) {
            self.enqueue(Event::Deleted(KubePod {
                metadata: ObjectMeta {
                    name: Some(pk.name()),
                    namespace: Some(pk.namespace()),
                    ..Default::default()
                },
                ..Default::default()
            }))
            .await?
        }

        // Now that we've sent off deletes, queue an apply event for all pods
        for pod in pods.into_iter() {
            self.enqueue(Event::Applied(pod)).await?
        }
        Ok(())
    }
}

async fn start_task<P: Provider>(
    task_client: KubeClient,
    pod: Pod,
    mut pod_state: P::PodState,
    check_pod_deleted: Arc<RwLock<bool>>,
) {
    let state: P::InitialState = Default::default();
    let name = pod.name().to_string();

    let check = async {
        loop {
            tokio::time::delay_for(std::time::Duration::from_secs(1)).await;
            if *check_pod_deleted.read().await {
                break;
            }
        }
    };

    tokio::select! {
        result = run_to_completion(&task_client, state, &mut pod_state, &pod) => match result {
            Ok(()) => debug!("Pod {} state machine exited without error", name),
            Err(e) => {
                error!("Pod {} state machine exited with error: {:?}", name, e);
                let api: kube::Api<KubePod> = kube::Api::namespaced(task_client.clone(), pod.namespace());
                let patch = serde_json::json!(
                    {
                        "metadata": {
                            "resourceVersion": "",
                        },
                        "status": {
                            "phase": Phase::Failed,
                            "reason": format!("{:?}", e),
                        }
                    }
                );
                let data = serde_json::to_vec(&patch).unwrap();
                // FIXME: Add retry to this patch
                api.patch_status(&pod.name(), &kube::api::PatchParams::default(), data)
                    .await.unwrap();
            },
        },
        _ = check => {
            let state: P::TerminatedState = Default::default();
            debug!("Pod {} terminated. Jumping to state {:?}.", name, state);
            match run_to_completion(&task_client, state, &mut pod_state, &pod).await {
                Ok(()) => debug!("Pod {} state machine exited without error", name),
                Err(e) => error!("Pod {} state machine exited with error: {:?}", name, e),
            }
        }
    }

    debug!("Pod {} waiting for deregistration.", name);
    loop {
        tokio::time::delay_for(std::time::Duration::from_secs(1)).await;
        if *check_pod_deleted.read().await {
            debug!("Pod {} deleted.", name);
            break;
        }
    }
    pod_state.async_drop().await;

    let pod_client: kube::Api<KubePod> = kube::Api::namespaced(task_client, pod.namespace());
    let dp = kube::api::DeleteParams {
        grace_period_seconds: Some(0),
        ..Default::default()
    };
    match pod_client.delete(&pod.name(), &dp).await {
        Ok(_) => {
            debug!("Pod {} deregistered.", name);
        }
        Err(e) => {
            // This could happen if Pod was force deleted.
            warn!("Unable to deregister {} with Kubernetes API: {:?}", name, e);
        }
    }
}
