mod handle;
mod wasi_runtime;

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use kube::client::APIClient;
use kubelet::pod::Pod;
use kubelet::{Phase, Provider, ProviderError, Status};
use log::{debug, info};
use tokio::fs::File;
use tokio::sync::RwLock;

use handle::RuntimeHandle;
use wasi_runtime::WasiRuntime;

const TARGET_WASM32_WASI: &str = "wasm32-wasi";

// PodStore contains a map of a unique pod key pointing to a map of container
// names to the join handle and logging for their running task
type PodStore = HashMap<String, HashMap<String, RuntimeHandle<File>>>;
/// WasiProvider provides a Kubelet runtime implementation that executes WASM
/// binaries conforming to the WASI spec
#[derive(Clone, Default)]
pub struct WasiProvider {
    handles: Arc<RwLock<PodStore>>,
}

#[async_trait::async_trait]
impl Provider for WasiProvider {
    async fn init(&self) -> anyhow::Result<()> {
        Ok(())
    }

    fn arch(&self) -> String {
        TARGET_WASM32_WASI.to_string()
    }

    fn can_schedule(&self, pod: &Pod) -> bool {
        // If there is a node selector and it has arch set to wasm32-wasi, we can
        // schedule it.
        pod.node_selector()
            .and_then(|i| {
                i.get("beta.kubernetes.io/arch")
                    .map(|v| v.eq(&TARGET_WASM32_WASI))
            })
            .unwrap_or(false)
    }

    async fn add(&self, pod: Pod, client: APIClient) -> anyhow::Result<()> {
        // To run an Add event, we load the WASM, update the pod status to Running,
        // and then execute the WASM, passing in the relevant data.
        // When the pod finishes, we update the status to Succeeded unless it
        // produces an error, in which case we mark it Failed.

        // TODO: Implement this for real.
        //
        // What it should do:
        // - for each volume
        //   - set up the volume map
        // - for each init container:
        //   - set up the runtime
        //   - mount any volumes (preopen)
        //   - run it to completion
        //   - bail with an error if it fails
        // - for each container and ephemeral_container
        //   - set up the runtime
        //   - mount any volumes (popen)
        //   - run it to completion
        //   - bail if it errors
        info!("Starting containers for pod {:?}", pod.name());
        // Wrap this in a block so the write lock goes out of scope when we are done
        {
            // Grab the entry while we are creating things
            let mut handles = self.handles.write().await;
            let entry = handles.entry(key_from_pod(&pod)).or_default();
            for container in pod.containers() {
                let env = self.env_vars(client.clone(), &container, &pod).await;
                let runtime = WasiRuntime::new(
                    PathBuf::from("./testdata/hello-world.wasm"),
                    env,
                    Vec::default(),
                    HashMap::default(),
                    // TODO: Actual log path configuration
                    std::env::current_dir()?,
                )
                .await?;

                debug!("Starting container {} on thread", container.name);
                let handle = runtime.start().await?;
                entry.insert(container.name.clone(), handle);
            }
        }
        info!(
            "All containers started for pod {:?}. Updating status",
            pod.name()
        );
        pod.patch_status(client, &Phase::Running).await;
        Ok(())
    }

    async fn modify(&self, pod: Pod, _client: APIClient) -> anyhow::Result<()> {
        // Modify will be tricky. Not only do we need to handle legitimate modifications, but we
        // need to sift out modifications that simply alter the status. For the time being, we
        // just ignore them, which is the wrong thing to do... except that it demos better than
        // other wrong things.
        info!("Pod modified");
        info!(
            "Modified pod spec: {:#?}",
            pod.as_kube_pod().status.as_ref().unwrap()
        );
        Ok(())
    }

    async fn delete(&self, _pod: Pod, _client: APIClient) -> anyhow::Result<()> {
        // There is currently no way to stop a long running instance, so we are
        // SOL here until there is support for it. See
        // https://github.com/bytecodealliance/wasmtime/issues/860 for more
        // information
        unimplemented!("cannot stop a running wasmtime instance")
    }

    async fn status(&self, pod: Pod, _client: APIClient) -> anyhow::Result<Status> {
        let pod_name = pod.name();
        let mut handles = self.handles.write().await;
        let container_handles =
            handles
                .get_mut(&key_from_pod(&pod))
                .ok_or_else(|| ProviderError::PodNotFound {
                    pod_name: pod_name.to_owned(),
                })?;
        let mut container_statuses = Vec::new();
        for (_, handle) in container_handles.iter_mut() {
            container_statuses.push(handle.status().await?)
        }

        Ok(Status {
            phase: Phase::Running,
            message: None,
            container_statuses,
        })
    }

    async fn logs(
        &self,
        namespace: String,
        pod_name: String,
        container_name: String,
    ) -> anyhow::Result<Vec<u8>> {
        let mut handles = self.handles.write().await;
        let handle = handles
            .get_mut(&pod_key(&namespace, &pod_name))
            .ok_or_else(|| ProviderError::PodNotFound {
                pod_name: pod_name.clone(),
            })?
            .get_mut(&container_name)
            .ok_or_else(|| ProviderError::ContainerNotFound {
                pod_name,
                container_name,
            })?;
        let mut output = Vec::new();
        handle.output(&mut output).await?;
        Ok(output)
    }
}

/// Generates a unique human readable key for storing a handle to a pod
fn key_from_pod(pod: &Pod) -> String {
    pod_key(pod.namespace(), pod.name())
}

fn pod_key<N: AsRef<str>, T: AsRef<str>>(namespace: N, pod_name: T) -> String {
    format!("{}:{}", namespace.as_ref(), pod_name.as_ref())
}

#[cfg(test)]
mod test {
    use super::*;
    use k8s_openapi::api::core::v1::Pod as KubePod;
    use k8s_openapi::api::core::v1::PodSpec;

    #[test]
    fn test_can_schedule() {
        let wp = WasiProvider::default();
        let mock = Default::default();
        assert!(!wp.can_schedule(&mock));

        let mut selector = std::collections::BTreeMap::new();
        selector.insert(
            "beta.kubernetes.io/arch".to_string(),
            "wasm32-wasi".to_string(),
        );
        let mut mock: KubePod = mock.into();
        mock.spec = Some(PodSpec {
            node_selector: Some(selector.clone()),
            ..Default::default()
        });
        let mock = Pod::new(mock);
        assert!(wp.can_schedule(&mock));
        selector.insert("beta.kubernetes.io/arch".to_string(), "amd64".to_string());
        let mut mock: KubePod = mock.into();
        mock.spec = Some(PodSpec {
            node_selector: Some(selector),
            ..Default::default()
        });
        let mock = Pod::new(mock);
        assert!(!wp.can_schedule(&mock));
    }

    #[test]
    fn test_logs() {
        // TODO: Log testing will need to be done in a full integration test as
        // it requires a kube client
    }
}
