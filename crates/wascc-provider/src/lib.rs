use kube::client::APIClient;
use kubelet::pod::pod_status;
use kubelet::{pod::Pod, Phase, Provider, Status};
use log::{debug, info};
use std::collections::HashMap;
use wascc_host::{host, Actor, NativeCapability};

const ACTOR_PUBLIC_KEY: &str = "deislabs.io/wascc-action-key";
const TARGET_WASM32_WASCC: &str = "wasm32-wascc";

/// The name of the HTTP capability.
const HTTP_CAPABILITY: &str = "wascc:http_server";

#[cfg(target_os = "linux")]
const HTTP_LIB: &str = "./lib/libwascc_httpsrv.so";
#[cfg(target_os = "macos")]
const HTTP_LIB: &str = "./lib/libwascc_httpsrv.dylib";

/// Kubernetes' view of environment variables is an unordered map of string to string.
type EnvVars = std::collections::HashMap<String, String>;

/// WasccProvider provides a Kubelet runtime implementation that executes WASM binaries.
///
/// Currently, this runtime uses WASCC as a host, loading the primary container as an actor.
/// TODO: In the future, we will look at loading capabilities using the "sidecar" metaphor
/// from Kubernetes.
#[derive(Clone)]
pub struct WasccProvider {}

#[async_trait::async_trait]
impl Provider for WasccProvider {
    async fn init(&self) -> anyhow::Result<()> {
        tokio::task::spawn_blocking(|| {
            let data = NativeCapability::from_file(HTTP_LIB).map_err(|e| {
                anyhow::anyhow!("Failed to read HTTP capability {}: {}", HTTP_LIB, e)
            })?;
            host::add_native_capability(data)
                .map_err(|e| anyhow::anyhow!("Failed to load HTTP capability: {}", e))
        })
        .await?
    }

    fn arch(&self) -> String {
        TARGET_WASM32_WASCC.to_string()
    }

    fn can_schedule(&self, pod: &Pod) -> bool {
        // If there is a node selector and it has arch set to wasm32-wascc, we can
        // schedule it.
        pod.spec
            .as_ref()
            .and_then(|s| s.node_selector.as_ref())
            .and_then(|i| {
                i.get("beta.kubernetes.io/arch")
                    .map(|v| v.eq(&TARGET_WASM32_WASCC))
            })
            .unwrap_or(false)
    }

    async fn add(&self, pod: Pod, client: APIClient) -> anyhow::Result<()> {
        // To run an Add event, we load the WASM, update the pod status to Running,
        // and then execute the WASM, passing in the relevant data.
        // When the pod finishes, we update the status to Succeeded unless it
        // produces an error, in which case we mark it Failed.
        debug!(
            "Pod added {:?}",
            pod.metadata.as_ref().and_then(|m| m.name.as_ref())
        );
        let namespace = pod
            .metadata
            .as_ref()
            .and_then(|m| m.namespace.as_deref())
            .unwrap_or_else(|| "default");
        // This would lock us into one wascc actor per pod. I don't know if
        // that is a good thing. Other containers would then be limited
        // to acting as components... which largely follows the sidecar
        // pattern.
        //
        // Another possibility is to embed the key in the image reference
        // (image/foo.wasm@ed25519:PUBKEY). That might work best, but it is
        // not terribly useable.
        //
        // A really icky one would be to just require the pubkey in the env
        // vars and suck it out of there. But that violates the intention
        // of env vars, which is to communicate _into_ the runtime, not to
        // configure the runtime.
        let pubkey = pod
            .metadata
            .as_ref()
            .and_then(|s| s.annotations.as_ref())
            .unwrap()
            .get(ACTOR_PUBLIC_KEY)
            .map(|a| a.to_string())
            .unwrap_or_default();
        debug!("{:?}", pubkey);

        // TODO: Implement this for real.
        //
        // What it should do:
        // - for each volume
        //   - set up the volume map
        // - for each init container:
        //   - set up the runtime
        //   - mount any volumes (popen)
        //   - run it to completion
        //   - bail with an error if it fails
        // - for each container and ephemeral_container
        //   - set up the runtime
        //   - mount any volumes (popen)
        //   - run it to completion
        //   - bail if it errors
        let containers = pod.spec.as_ref().map(|s| &s.containers).unwrap();
        // Wrap this in a block so the write lock goes out of scope when we are done
        info!(
            "Starting containers for pod {:?}",
            pod.metadata.as_ref().and_then(|m| m.name.as_ref())
        );
        for container in containers {
            let env = self.env_vars(client.clone(), &container, &pod).await;

            debug!("Starting container {} on thread", container.name);
            let cloned_key = pubkey.clone();
            // TODO: Replace with actual image store lookup when it is merged
            let data = tokio::fs::read("./testdata/echo.wasm").await?;
            let http_result =
                tokio::task::spawn_blocking(move || wascc_run_http(data.clone(), env, &cloned_key))
                    .await?;
            match http_result {
                Ok(_) => {
                    pod_status(client.clone(), &pod, Phase::Running, namespace).await;
                }
                Err(e) => {
                    pod_status(client, &pod, Phase::Failed, namespace).await;
                    return Err(anyhow::anyhow!("Failed to run pod: {}", e));
                }
            }
        }
        info!(
            "All containers started for pod {:?}. Updating status",
            pod.metadata.as_ref().and_then(|m| m.name.as_ref())
        );
        Ok(())
    }

    async fn modify(&self, pod: Pod, _client: APIClient) -> anyhow::Result<()> {
        // Modify will be tricky. Not only do we need to handle legitimate modifications, but we
        // need to sift out modifications that simply alter the status. For the time being, we
        // just ignore them, which is the wrong thing to do... except that it demos better than
        // other wrong things.
        info!("Pod modified");
        info!("Modified pod spec: {:#?}", pod.status.unwrap());
        Ok(())
    }

    async fn delete(&self, pod: Pod, _client: APIClient) -> anyhow::Result<()> {
        let pubkey = pod
            .metadata
            .unwrap_or_default()
            .annotations
            .unwrap_or_default()
            .get(ACTOR_PUBLIC_KEY)
            .map(|a| a.to_string())
            .unwrap_or_else(|| "".into());
        wascc_stop(&pubkey).map_err(|e| anyhow::anyhow!("Failed to stop wascc actor: {}", e))
    }

    async fn status(&self, pod: Pod, _client: APIClient) -> anyhow::Result<Status> {
        match pod
            .metadata
            .unwrap_or_default()
            .annotations
            .unwrap_or_default()
            .get(ACTOR_PUBLIC_KEY)
        {
            None => Ok(Status {
                phase: Phase::Unknown,
                message: None,
                container_statuses: Vec::new(),
            }),
            Some(pk) => {
                let pk = pk.clone();
                let result = tokio::task::spawn_blocking(move || host::actor_claims(&pk)).await?;
                match result {
                    None => {
                        // FIXME: I don't know how to tell if an actor failed.
                        Ok(Status {
                            phase: Phase::Succeeded,
                            message: None,
                            container_statuses: Vec::new(),
                        })
                    }
                    Some(_) => Ok(Status {
                        phase: Phase::Running,
                        message: None,
                        container_statuses: Vec::new(),
                    }),
                }
            }
        }
    }
}

/// Run a WasCC module inside of the host, configuring it to handle HTTP requests.
///
/// This bootstraps an HTTP host, using the value of the env's `PORT` key to expose a port.
fn wascc_run_http(data: Vec<u8>, env: EnvVars, key: &str) -> anyhow::Result<()> {
    let mut httpenv: HashMap<String, String> = HashMap::new();
    httpenv.insert(
        "PORT".into(),
        env.get("PORT")
            .map(|a| a.to_string())
            .unwrap_or_else(|| "80".to_string()),
    );

    wascc_run(
        data,
        key,
        vec![Capability {
            name: HTTP_CAPABILITY,
            env,
        }],
    )
}

/// Stop a running waSCC actor.
fn wascc_stop(key: &str) -> anyhow::Result<(), wascc_host::errors::Error> {
    host::remove_actor(key)
}

/// Capability describes a waSCC capability.
///
/// Capabilities are made available to actors through a two-part processthread:
/// - They must be registered
/// - For each actor, the capability must be configured
struct Capability {
    name: &'static str,
    env: EnvVars,
}

/// Run the given WASM data as a waSCC actor with the given public key.
///
/// The provided capabilities will be configured for this actor, but the capabilities
/// must first be loaded into the host by some other process, such as register_native_capabilities().
fn wascc_run(data: Vec<u8>, key: &str, capabilities: Vec<Capability>) -> anyhow::Result<()> {
    info!("wascc run");
    let load = Actor::from_bytes(data).map_err(|e| anyhow::anyhow!("Error loading WASM: {}", e))?;
    host::add_actor(load).map_err(|e| anyhow::anyhow!("Error adding actor: {}", e))?;

    capabilities.iter().try_for_each(|cap| {
        info!("configuring capability {}", cap.name);
        host::configure(key, cap.name, cap.env.clone())
            .map_err(|e| anyhow::anyhow!("Error configuring capabilities for module: {}", e))
    })?;
    info!("Instance executing");
    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;
    use k8s_openapi::api::core::v1::PodSpec;

    #[cfg(target_os = "linux")]
    const ECHO_LIB: &str = "./testdata/libecho_provider.so";
    #[cfg(target_os = "macos")]
    const ECHO_LIB: &str = "./testdata/libecho_provider.dylib";

    #[tokio::test]
    async fn test_init() {
        let provider = WasccProvider {};
        provider
            .init()
            .await
            .expect("HTTP capability is registered");
    }

    #[test]
    fn test_wascc_run() {
        // Open file
        let data = std::fs::read("./testdata/echo.wasm").expect("read the wasm file");
        // Send into wascc_run
        wascc_run_http(
            data,
            EnvVars::new(),
            "MB4OLDIC3TCZ4Q4TGGOVAZC43VXFE2JQVRAXQMQFXUCREOOFEKOKZTY2",
        )
        .expect("successfully executed a WASM");

        // Give the webserver a chance to start up.
        std::thread::sleep(std::time::Duration::from_secs(3));
        wascc_stop("MB4OLDIC3TCZ4Q4TGGOVAZC43VXFE2JQVRAXQMQFXUCREOOFEKOKZTY2")
            .expect("Removed the actor");
    }

    #[test]
    fn test_wascc_echo() {
        let data = NativeCapability::from_file(ECHO_LIB).expect("loaded echo library");
        host::add_native_capability(data).expect("added echo capability");

        let key = "MDAYLDTOZEHQFPB3CL5PAFY5UTNCW32P54XGWYX3FOM2UBRYNCP3I3BF";

        let wasm = std::fs::read("./testdata/echo_actor_s.wasm").expect("load echo WASM");
        // TODO: use wascc_run to execute echo_actor
        wascc_run(
            wasm,
            key,
            vec![Capability {
                name: "wok:echoProvider",
                env: EnvVars::new(),
            }],
        )
        .expect("completed echo run")
    }

    #[test]
    fn test_can_schedule() {
        let wr = WasccProvider {};
        let mut mock = Default::default();
        assert!(!wr.can_schedule(&mock));

        let mut selector = std::collections::BTreeMap::new();
        selector.insert(
            "beta.kubernetes.io/arch".to_string(),
            "wasm32-wascc".to_string(),
        );
        mock.spec = Some(PodSpec {
            node_selector: Some(selector.clone()),
            ..Default::default()
        });
        assert!(wr.can_schedule(&mock));
        selector.insert("beta.kubernetes.io/arch".to_string(), "amd64".to_string());
        mock.spec = Some(PodSpec {
            node_selector: Some(selector),
            ..Default::default()
        });
        assert!(!wr.can_schedule(&mock));
    }
}
