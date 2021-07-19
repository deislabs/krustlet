//! `pod` is a collection of utilities surrounding the Kubernetes pod API.
mod handle;
pub mod state;
mod status;

pub use handle::Handle;
pub(crate) use status::initialize_pod_container_statuses;
pub use status::{
    make_registered_status, make_status, make_status_with_containers, patch_status, Phase, Status,
};

use crate::container::{Container, ContainerKey};
use chrono::{DateTime, Utc};
use k8s_openapi::api::core::v1::{
    Container as KubeContainer, Pod as KubePod, Volume as KubeVolume,
};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use kube::api::{Resource, ResourceExt};
use serde::Deserialize;
use serde::Serialize;

/// A Kubernetes Pod
///
/// This is a new type around the k8s_openapi Pod definition
/// providing convenient accessor methods
#[derive(Default, Debug, Clone, Deserialize, Serialize)]
pub struct Pod {
    #[serde(flatten)]
    kube_pod: KubePod,
}

impl Pod {
    /// Get the name of the pod
    pub fn name(&self) -> &str {
        self.kube_pod
            .metadata
            .name
            .as_deref()
            .expect("Pod name should always be set but was not")
    }

    /// Get the pod's namespace
    ///
    /// Returns "default" if no namespace was explictily set
    pub fn namespace(&self) -> &str {
        self.kube_pod
            .metadata
            .namespace
            .as_deref()
            .unwrap_or("default")
    }

    /// Get the pod's node_selector map
    pub fn node_selector(&self) -> &std::collections::BTreeMap<String, String> {
        &self
            .kube_pod
            .spec
            .as_ref()
            .map(|s| &s.node_selector)
            .unwrap_or(&EMPTY_MAP)
    }

    /// Get the pod's service account name
    pub fn service_account_name(&self) -> Option<&str> {
        let spec = self.kube_pod.spec.as_ref()?;
        spec.service_account_name.as_deref()
    }

    /// Get the pod volumes
    pub fn volumes(&self) -> &Vec<KubeVolume> {
        self.kube_pod
            .spec
            .as_ref()
            .map(|s| &s.volumes)
            .unwrap_or(&EMPTY_VOLUMES)
    }

    /// Get the pod's host ip
    pub fn host_ip(&self) -> Option<&str> {
        let status = self.kube_pod.status.as_ref()?;
        status.host_ip.as_deref()
    }

    /// Get the pod's ip
    pub fn pod_ip(&self) -> Option<&str> {
        let status = self.kube_pod.status.as_ref()?;
        status.pod_ip.as_deref()
    }

    /// Get the pod's uid
    pub fn pod_uid(&self) -> &str {
        self.kube_pod
            .metadata
            .uid
            .as_deref()
            .expect("Pod uid should always be set but was not")
    }

    /// Get an iterator over the pod's labels
    pub fn labels(&self) -> &std::collections::BTreeMap<String, String> {
        &self.kube_pod.meta().labels
    }

    ///  Get the pod's annotations
    pub fn annotations(&self) -> &std::collections::BTreeMap<String, String> {
        &self.kube_pod.meta().annotations
    }

    /// Get the names of the pod's image pull secrets
    pub fn image_pull_secrets(&self) -> Vec<String> {
        match self.kube_pod.spec.as_ref() {
            None => vec![],
            Some(spec) => spec
                .image_pull_secrets
                .iter()
                .filter_map(|objref| objref.name.clone())
                .collect(),
        }
    }

    /// Indicate if this pod is a static pod.
    /// TODO: A missing owner_references field was an indication of static pod in my testing but I
    /// dont know how reliable this is.
    pub fn is_static(&self) -> bool {
        self.kube_pod.meta().owner_references.is_empty()
    }

    /// Indicate if this pod is part of a Daemonset
    pub fn is_daemonset(&self) -> bool {
        self.kube_pod
            .meta()
            .owner_references
            .iter()
            .any(|owner| owner.kind == "DaemonSet")
    }

    ///  Get a specific annotation from the pod
    pub fn get_annotation(&self, key: &str) -> Option<&str> {
        Some(self.annotations().get(key)?.as_str())
    }

    /// Get the deletionTimestamp if it exists
    pub fn deletion_timestamp(&self) -> Option<&DateTime<Utc>> {
        self.kube_pod
            .meta()
            .deletion_timestamp
            .as_ref()
            .map(|t| &t.0)
    }

    /// Find container by `ContainerKey` and return it.
    pub fn find_container(&self, key: &ContainerKey) -> Option<Container> {
        let containers: Vec<Container> = if key.is_init() {
            self.init_containers()
        } else {
            self.containers()
        };
        containers
            .into_iter()
            .find(|container| container.name() == key.name())
    }

    /// Finds the index of the container in the Pod's container statuses.
    pub fn container_status_index(&self, key: &ContainerKey) -> Option<usize> {
        match self.kube_pod.status.as_ref() {
            Some(status) => {
                let statuses = if key.is_init() {
                    &status.init_container_statuses
                } else {
                    &status.container_statuses
                };
                statuses
                    .iter()
                    .enumerate()
                    .find(|(_, status)| status.name == key.name())
                    .map(|(idx, _)| idx)
            }
            None => None,
        }
    }

    /// Get a pod's containers
    pub fn containers(&self) -> Vec<Container> {
        self.kube_pod
            .spec
            .as_ref()
            .map(|s| &s.containers)
            .unwrap_or_else(|| &EMPTY_VEC)
            .iter()
            .map(|c| Container::new(c))
            .collect()
    }

    /// Get a pod's init containers
    pub fn init_containers(&self) -> Vec<Container> {
        self.kube_pod
            .spec
            .as_ref()
            .map(|s| &s.init_containers)
            .unwrap_or(&EMPTY_VEC)
            .iter()
            .map(|c| Container::new(c))
            .collect()
    }

    /// Gets all of a pod's containers (init and application)
    pub fn all_containers(&self) -> Vec<Container> {
        let mut app_containers = self.containers();
        let init_containers = self.init_containers();
        app_containers.extend(init_containers);
        app_containers
    }

    /// Turn the Pod into the Kubernetes API version of a Pod
    pub fn into_kube_pod(self) -> KubePod {
        self.kube_pod
    }

    /// Turn a reference to a Pod into a reference to the Kubernetes API version of a Pod
    pub fn as_kube_pod(&self) -> &KubePod {
        &self.kube_pod
    }
}

impl k8s_openapi::Metadata for Pod {
    type Ty = ObjectMeta;

    fn metadata(&self) -> &ObjectMeta {
        self.kube_pod.metadata()
    }

    fn metadata_mut(&mut self) -> &mut ObjectMeta {
        self.kube_pod.metadata_mut()
    }
}

impl k8s_openapi::Resource for Pod {
    const API_VERSION: &'static str = KubePod::API_VERSION;
    const GROUP: &'static str = KubePod::GROUP;
    const KIND: &'static str = KubePod::KIND;
    const VERSION: &'static str = KubePod::VERSION;
    const URL_PATH_SEGMENT: &'static str = KubePod::URL_PATH_SEGMENT;
    type Scope = k8s_openapi::NamespaceResourceScope;
}

impl std::convert::From<KubePod> for Pod {
    fn from(api_pod: KubePod) -> Self {
        Self { kube_pod: api_pod }
    }
}

impl<'a> std::convert::From<&'a Pod> for &'a KubePod {
    fn from(pod: &'a Pod) -> Self {
        &pod.kube_pod
    }
}
impl std::convert::From<Pod> for KubePod {
    fn from(pod: Pod) -> Self {
        pod.kube_pod
    }
}

/// PodKey is a unique human readable key for storing a handle to a pod in a hash.
#[derive(Hash, Ord, Eq, PartialOrd, PartialEq, Debug, Clone, Default)]
pub struct PodKey {
    name: String,
    namespace: String,
}

impl PodKey {
    /// Creates a new pod key from arbitrary strings. In general, you'll likely want to use
    /// [`PodKey::from`] to convert from a Kubernetes Pod or our internal [`Pod`] representation
    pub fn new<N: AsRef<str>, T: AsRef<str>>(namespace: N, pod_name: T) -> Self {
        PodKey {
            name: pod_name.as_ref().to_owned(),
            namespace: namespace.as_ref().to_owned(),
        }
    }

    /// Returns the name of the pod in the pod key
    pub fn name(&self) -> String {
        self.name.clone()
    }

    /// Returns the namespace of the pod in the pod key
    pub fn namespace(&self) -> String {
        self.namespace.clone()
    }
}

impl From<Pod> for PodKey {
    fn from(p: Pod) -> Self {
        PodKey {
            name: p.name().to_owned(),
            namespace: p.namespace().to_owned(),
        }
    }
}

impl From<&Pod> for PodKey {
    fn from(p: &Pod) -> Self {
        PodKey {
            name: p.name().to_owned(),
            namespace: p.namespace().to_owned(),
        }
    }
}

impl From<KubePod> for PodKey {
    fn from(p: KubePod) -> Self {
        PodKey {
            name: p.name(),
            namespace: p.namespace().unwrap_or_else(|| "default".to_string()),
        }
    }
}

impl From<&KubePod> for PodKey {
    fn from(p: &KubePod) -> Self {
        PodKey {
            name: p.name(),
            namespace: p.namespace().unwrap_or_else(|| "default".to_string()),
        }
    }
}

lazy_static::lazy_static! {
    static ref EMPTY_MAP: std::collections::BTreeMap<String, String> = std::collections::BTreeMap::new();
    static ref EMPTY_VEC: Vec<KubeContainer> = Vec::new();
    static ref EMPTY_VOLUMES: Vec<KubeVolume> = Vec::new();
}
