//! Traits and types needed to create backend providers for a Kubelet
use std::collections::HashMap;

use async_trait::async_trait;
use k8s_openapi::api::core::v1::{ConfigMap, EnvVarSource, Secret};
use kube::api::Api;
use std::sync::Arc;
use thiserror::Error;
use tracing::{debug, error, info};

use crate::container::Container;
use crate::log::Sender;
use crate::node::Builder;
use crate::plugin_watcher::PluginRegistry;
use crate::pod::Pod;
use crate::pod::Status as PodStatus;
use crate::resources::DeviceManager;
use krator::{ObjectState, State};

/// A back-end for a Kubelet.
///
/// The primary responsibility of a Provider is to execute a workload (or schedule it on an external executor)
/// and then monitor it, exposing details back upwards into the Kubelet.
///
/// We pass in the client to facilitate cases where a provider may be middleware for another Kubernetes object,
/// or where a provider may require supplemental Kubernetes objects such as Secrets, ConfigMaps, or CRDs.
///
/// **Note**: this trait is defined using [async-trait](https://crates.io/crates/async-trait) which
/// allows for the use of async methods on traits. The documentation reflects the generated code. It is
/// recommended for methods that return `Pin<Box<dyn Future<Output = Result<T>> + Send + 'async_trait>>`
/// to be implemented as async functions using `#[async_trait]`.
///
/// # Example
/// ```rust
/// use async_trait::async_trait;
/// use kubelet::resources::DeviceManager;
/// use kubelet::plugin_watcher::PluginRegistry;
/// use kubelet::pod::{Pod, Status};
/// use kubelet::provider::{DevicePluginSupport, Provider, PluginSupport};
/// use kubelet::pod::state::Stub;
/// use kubelet::pod::state::prelude::*;
/// use std::sync::Arc;
/// use tokio::sync::RwLock;
///
/// struct MyProvider;
///
/// struct ProviderState;
/// struct PodState;
///
/// #[async_trait]
/// impl ObjectState for PodState {
///     type Manifest = Pod;
///     type Status = Status;
///     type SharedState = ProviderState;
///     async fn async_drop(self, _provider_state: &mut ProviderState) { }
/// }
///
/// #[async_trait]
/// impl Provider for MyProvider {
///     type ProviderState = ProviderState;
///     type InitialState = Stub;
///     type TerminatedState = Stub;
///     const ARCH: &'static str = "my-arch";
///
///     type PodState = PodState;
///
///     fn provider_state(&self) -> SharedState<ProviderState> {
///         Arc::new(RwLock::new(ProviderState {}))
///     }
///
///     async fn initialize_pod_state(&self, _pod: &Pod) -> anyhow::Result<Self::PodState> {
///         Ok(PodState)
///     }
///
///     async fn logs(&self, namespace: String, pod: String, container: String, sender: kubelet::log::Sender) -> anyhow::Result<()> { todo!() }
/// }
///
/// impl PluginSupport for ProviderState {
///     fn plugin_registry(&self) -> Option<Arc<PluginRegistry>> {
///         None
///     }
/// }
///
/// impl DevicePluginSupport for ProviderState {
///     fn device_plugin_manager(&self) -> Option<Arc<DeviceManager>> {
///         None
///     }
/// }
/// ```
#[async_trait]
pub trait Provider: Sized + Send + Sync + 'static {
    /// The state of the provider itself.
    type ProviderState: 'static + Send + Sync + PluginSupport + DevicePluginSupport;

    /// The state that is passed between Pod state handlers.
    type PodState: ObjectState<
        Manifest = Pod,
        Status = PodStatus,
        SharedState = Self::ProviderState,
    >;

    /// The initial state for Pod state machine.
    type InitialState: Default + State<Self::PodState>;

    /// The a state to handle early Pod termination.
    type TerminatedState: Default + State<Self::PodState>;

    /// Arch returns a string specifying what architecture this provider supports
    const ARCH: &'static str;

    /// Gets the provider state.
    fn provider_state(&self) -> krator::SharedState<Self::ProviderState>;

    /// Allows provider to populate node information.
    async fn node(&self, _builder: &mut Builder) -> anyhow::Result<()> {
        Ok(())
    }

    /// Hook to allow provider to introduced shared state into Pod state.
    // TODO: Is there a way to provide a default implementation of this if Self::PodState: Default?
    async fn initialize_pod_state(&self, pod: &Pod) -> anyhow::Result<Self::PodState>;

    /// Hook to allow the provider to react to the Kubelet being shut down
    ///
    /// It receives only the node name as a parameter in case the provider wants to set a condition
    /// on the object - for example to to signify that it did not crash but performed an orderly
    /// shutdown.
    ///
    /// There are currently no mechanisms in place to propagate the shutdown trigger to the state
    /// machine, providers will have to implement this themselves via a [`std::sync::mpsc::channel`]
    /// or the shared state or something similar.
    ///
    /// # Arguments
    ///
    /// * `node_name` - The name of the node object that was created by this Kubelet instance
    ///
    async fn shutdown(&self, node_name: &str) -> anyhow::Result<()> {
        info!(node_name, "Shutdown triggered for node, since no custom shutdown behavior was implemented Kubelet will simply shut down now");
        Ok(())
    }

    /// Given a Pod, get back the logs for the associated workload.
    async fn logs(
        &self,
        namespace: String,
        pod: String,
        container: String,
        sender: Sender,
    ) -> anyhow::Result<()>;

    /// Execute a given command on a workload and then return the result.
    ///
    /// The default implementation of this returns a message that this feature is
    /// not available. Override this only when there is an implementation.
    async fn exec(&self, _pod: Pod, _command: String) -> anyhow::Result<Vec<String>> {
        Err(NotImplementedError.into())
    }

    /// Resolve the environment variables for a container.
    ///
    /// This generally should not be overwritten unless you need to handle
    /// environment variable resolution in a special way, such as allowing
    /// custom Downward API fields.
    ///
    /// It is safe to call from within your own providers.
    async fn env_vars(
        container: &Container,
        pod: &Pod,
        client: &kube::Client,
    ) -> HashMap<String, String> {
        let mut env = HashMap::new();

        for env_var in container.env().clone().into_iter() {
            let key = env_var.name;
            let value = match env_var.value {
                Some(v) => v,
                None => {
                    on_missing_env_value(
                        env_var.value_from,
                        client,
                        pod.namespace(),
                        &field_map(pod),
                    )
                    .await
                }
            };
            env.insert(key, value);
        }
        env
    }
}

/// A trait for specifying where the volume path is located. Defaults to `None`
pub trait VolumeSupport {
    /// Gets the path at which to construct temporary directories for volumes.
    fn volume_path(&self) -> Option<&std::path::Path> {
        None
    }
}

/// A trait for specifying whether plugins are supported. Defaults to `None`
pub trait PluginSupport {
    /// Gets the plugin registry used to fetch volume plugins
    fn plugin_registry(&self) -> Option<Arc<PluginRegistry>> {
        None
    }
}

/// A trait for specifying whether device plugins are supported. Defaults to `None`
pub trait DevicePluginSupport {
    /// Fetch the device plugin manager to register and use device plugins
    fn device_plugin_manager(&self) -> Option<Arc<DeviceManager>> {
        None
    }
}

/// Resolve the environment variables for a container.
///
/// This generally should not be overwritten unless you need to handle
/// environment variable resolution in a special way, such as allowing
/// custom Downward API fields.
///
/// It is safe to call from within your own providers.
pub async fn env_vars(
    container: &Container,
    pod: &Pod,
    client: &kube::Client,
) -> HashMap<String, String> {
    let mut env = HashMap::new();

    for env_var in container.env().clone().into_iter() {
        let key = env_var.name;
        let value = match env_var.value {
            Some(v) => v,
            None => {
                on_missing_env_value(env_var.value_from, client, pod.namespace(), &field_map(pod))
                    .await
            }
        };
        env.insert(key, value);
    }
    env
}

/// Called when an env var does not have a value associated with.
///
/// This follows the env_var_source to get the value
#[doc(hidden)]
async fn on_missing_env_value(
    env_var_source: Option<EnvVarSource>,
    client: &kube::Client,
    ns: &str,
    fields: &HashMap<String, String>,
) -> String {
    let env_src = match env_var_source {
        Some(env_src) => env_src,
        None => return String::new(),
    };

    // ConfigMaps
    if let Some(cfkey) = env_src.config_map_key_ref.as_ref() {
        let name = cfkey.name.as_deref().unwrap_or_default();
        match Api::<ConfigMap>::namespaced(client.clone(), ns)
            .get(name)
            .await
        {
            Ok(cfgmap) => {
                // I am not totally clear on what the outcome should
                // be of a cfgmap key miss. So for now just return an
                // empty default.
                return cfgmap.data.get(&cfkey.key).cloned().unwrap_or_default();
            }
            Err(e) => {
                error!(error = %e, name, "Error fetching config map");
                return "".to_string();
            }
        }
    }
    // Secrets
    if let Some(seckey) = env_src.secret_key_ref.as_ref() {
        let name = seckey.name.as_deref().unwrap_or_default();
        match Api::<Secret>::namespaced(client.clone(), ns)
            .get(name)
            .await
        {
            Ok(mut secret) => {
                // I am not totally clear on what the outcome should
                // be of a secret key miss. So for now just return an
                // empty default.
                return secret
                    .data
                    .remove(&seckey.key)
                    .map(|s| String::from_utf8(s.0).unwrap_or_default())
                    .unwrap_or_default();
            }
            Err(e) => {
                error!(error = %e, name, "Error fetching secret");
                return String::new();
            }
        }
    }
    // Downward API (Field Refs)
    if let Some(cfkey) = env_src.field_ref.as_ref() {
        return fields.get(&cfkey.field_path).cloned().unwrap_or_default();
    }
    // Reource Fields (Not implementable just yet... need more of a model.)

    String::new()
}

/// Build the map of allowable field_ref values.
///
/// The Downward API only supports a small selection of fields. This
/// provides those fields.
fn field_map(pod: &Pod) -> HashMap<String, String> {
    let mut map: HashMap<String, String> = HashMap::new();
    map.insert("metadata.name".into(), pod.name().to_owned());
    map.insert("metadata.namespace".into(), pod.namespace().to_owned());
    map.insert(
        "spec.serviceAccountName".into(),
        pod.service_account_name().unwrap_or_default().to_owned(),
    );
    map.insert(
        "status.hostIP".into(),
        pod.host_ip().unwrap_or_default().to_owned(),
    );
    map.insert(
        "status.podIP".into(),
        pod.pod_ip().unwrap_or_default().to_owned(),
    );
    pod.labels().iter().for_each(|(k, v)| {
        debug!(item = %k, "adding to labels");
        map.insert(format!("metadata.labels.{}", k), v.clone());
    });
    pod.annotations().iter().for_each(|(k, v)| {
        map.insert(format!("metadata.annotations.{}", k), v.clone());
    });
    map
}

/// A Provider error
#[derive(Debug, Error)]
pub enum ProviderError {
    /// Pod was not found
    #[error("cannot find pod {}", pod_name)]
    PodNotFound {
        /// The pod's name
        pod_name: String,
    },
    /// Container was not found
    #[error("cannot find container {} in pod {}", container_name, pod_name)]
    ContainerNotFound {
        /// The container's pod's name
        pod_name: String,
        /// The container's name
        container_name: String,
    },
}

/// A specific operation is not implemented
#[derive(Error, Debug)]
#[error("Operation not supported")]
pub struct NotImplementedError;
