//! A module for use in managing volumes in providers. Use of this module is not
//! mandatory to create a Provider, but it does provide common implementation
//! logic for supported volume providers.
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use k8s_openapi::api::core::v1::KeyToPath;
use k8s_openapi::api::core::v1::{PersistentVolumeClaim, Secret, Volume as KubeVolume};
use kube::api::Api;
use tracing::error;

use crate::plugin_watcher::PluginRegistry;
use crate::pod::Pod;

mod configmap;
mod downward;
mod hostpath;
mod persistentvolumeclaim;
mod projected;
mod secret;

pub use configmap::ConfigMapVolume;
pub use downward::DownwardApiVolume;
pub use hostpath::HostPathVolume;
pub use persistentvolumeclaim::PvcVolume;
pub use projected::ProjectedVolume;
pub use secret::SecretVolume;

/// type of volume
#[derive(Debug)]
pub enum VolumeType {
    /// configmap volume
    ConfigMap,
    /// secret volume
    Secret,
    /// PVC volume with an optional staging path if the volume supports stage/unstage
    PersistentVolumeClaim(Option<PathBuf>),
    /// hostpath volume
    HostPath,
    /// Downward API volume, populated using projected data from the Downward API
    DownwardApi,
    /// Projected volume, a new volume type used for all projected data types (ConfigMap, Secret,
    /// and Downward API)
    Projected,
}

/// A reference to a volume that can be mounted and unmounted. A `VolumeRef` should be stored
/// alongside a pod handle as a way to manage the lifecycle of a Pod's volume. Each embedded type
/// can be used separately as well
#[allow(clippy::large_enum_variant)]
pub enum VolumeRef {
    /// configmap volume
    ConfigMap(ConfigMapVolume),
    /// secret volume
    Secret(SecretVolume),
    /// PVC volume
    PersistentVolumeClaim(PvcVolume),
    /// hostpath volume
    HostPath(HostPathVolume),
    /// DownwardAPI volume
    DownwardApi(DownwardApiVolume),
    /// Projected volume, a new volume type used for all projected data types (ConfigMap, Secret,
    /// and Downward API)
    Projected(ProjectedVolume),
}

impl VolumeRef {
    /// Resolves the volumes for a pod.
    pub async fn volumes_from_pod(
        pod: &Pod,
        client: &kube::Client,
        plugin_registry: Option<Arc<PluginRegistry>>,
    ) -> anyhow::Result<HashMap<String, Self>> {
        let vols = pod
            .volumes()
            .iter()
            .map(|v| (v, plugin_registry.clone()))
            .map(|(vol, pr)| async move {
                Ok((vol.name.clone(), to_volume_ref(vol, pod, client, pr).await?))
            });
        futures::future::join_all(vols).await.into_iter().collect()
    }

    /// A convenience wrapper that calls the correct get_path method for the variant. Returns the
    /// path the volume is mounted at on the host, `None` if the volume hasn't been mounted
    pub fn get_path(&self) -> Option<&Path> {
        match self {
            VolumeRef::ConfigMap(cm) => cm.get_path(),
            VolumeRef::Secret(sec) => sec.get_path(),
            VolumeRef::PersistentVolumeClaim(pv) => pv.get_path(),
            VolumeRef::HostPath(host) => host.get_path(),
            VolumeRef::DownwardApi(d) => d.get_path(),
            VolumeRef::Projected(p) => p.get_path(),
        }
    }

    /// A convenience wrapper that calls the correct mount function for the variant
    pub async fn mount(&mut self, path: impl AsRef<Path>) -> anyhow::Result<()> {
        match self {
            VolumeRef::ConfigMap(cm) => cm.mount(path).await,
            VolumeRef::Secret(sec) => sec.mount(path).await,
            VolumeRef::PersistentVolumeClaim(pv) => pv.mount(path).await,
            VolumeRef::HostPath(host) => host.mount().await,
            VolumeRef::DownwardApi(d) => d.mount(path).await,
            // We need to clone the path here so we are sure that it is owned since this mount call
            // results in recursion
            VolumeRef::Projected(p) => p.mount(path.as_ref().to_owned()).await,
        }
    }

    /// A convenience wrapper that calls the correct unmount function for the variant
    pub async fn unmount(&mut self) -> anyhow::Result<()> {
        match self {
            VolumeRef::ConfigMap(cm) => cm.unmount().await,
            VolumeRef::Secret(sec) => sec.unmount().await,
            VolumeRef::PersistentVolumeClaim(pv) => pv.unmount().await,
            // Doesn't need any unmounting steps
            VolumeRef::HostPath(_) => Ok(()),
            VolumeRef::DownwardApi(d) => d.unmount().await,
            VolumeRef::Projected(p) => p.unmount().await,
        }
    }
}

fn mount_setting_for(key: &str, items_to_mount: &[KeyToPath]) -> ItemMount {
    if items_to_mount.is_empty() {
        ItemMount::MountAt(key.to_owned())
    } else {
        ItemMount::from(
            items_to_mount
                .iter()
                .find(|kp| kp.key == key)
                .map(|kp| kp.path.to_string()),
        )
    }
}

enum ItemMount {
    MountAt(String),
    DoNotMount,
}

impl From<Option<String>> for ItemMount {
    fn from(option: Option<String>) -> Self {
        match option {
            None => ItemMount::DoNotMount,
            Some(path) => ItemMount::MountAt(path),
        }
    }
}

async fn to_volume_ref(
    vol: &KubeVolume,
    pod: &Pod,
    client: &kube::Client,
    plugin_registry: Option<Arc<PluginRegistry>>,
) -> anyhow::Result<VolumeRef> {
    if vol.config_map.is_some() {
        Ok(VolumeRef::ConfigMap(ConfigMapVolume::new(
            vol,
            pod.namespace(),
            client.clone(),
        )?))
    } else if vol.secret.is_some() {
        Ok(VolumeRef::Secret(SecretVolume::new(
            vol,
            pod.namespace(),
            client.clone(),
        )?))
    } else if vol.persistent_volume_claim.is_some() {
        Ok(VolumeRef::PersistentVolumeClaim(
            PvcVolume::new(vol, pod.namespace(), client.clone(), plugin_registry).await?,
        ))
    } else if vol.host_path.is_some() {
        Ok(VolumeRef::HostPath(HostPathVolume::new(vol)?))
    } else if vol.downward_api.is_some() {
        Ok(VolumeRef::DownwardApi(DownwardApiVolume::new(
            vol,
            pod.to_owned(),
        )?))
    } else if vol.projected.is_some() {
        Ok(VolumeRef::Projected(ProjectedVolume::new(
            vol,
            pod.to_owned(),
            client.clone(),
        )?))
    } else {
        Err(anyhow::anyhow!(
            "Unsupported volume type. Currently supported types: ConfigMap, Secret, PersistentVolumeClaim, HostPath, and DownwardAPI"
        ))
    }
}
