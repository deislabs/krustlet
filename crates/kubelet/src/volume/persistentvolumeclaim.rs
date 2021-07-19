use std::collections::BTreeMap;
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;

use k8s_csi::v1_3_0::node_service_capability::{rpc, Rpc, Type as CapabilityType};
use k8s_csi::v1_3_0::volume_capability::access_mode::Mode as CSIMode;
use k8s_csi::v1_3_0::volume_capability::{
    AccessMode as CSIAccessMode, AccessType as CSIAccessType, MountVolume as CSIMountVolume,
};
use k8s_csi::v1_3_0::{node_client::NodeClient, volume_capability::BlockVolume};
use k8s_csi::v1_3_0::{
    NodeGetCapabilitiesRequest, NodePublishVolumeRequest, NodeStageVolumeRequest,
    NodeUnpublishVolumeRequest, VolumeCapability,
};

use k8s_openapi::api::core::v1::{
    CSIPersistentVolumeSource, PersistentVolume, PersistentVolumeClaimSpec,
    PersistentVolumeClaimVolumeSource, SecretReference, TypedLocalObjectReference,
    Volume as KubeVolume,
};
use k8s_openapi::api::storage::v1::StorageClass;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::LabelSelector;
use tempfile::Builder;
use thiserror::Error;
use tracing::log::{info, warn};

use crate::grpc_sock;
use crate::plugin_watcher::PluginRegistry;

use super::*;

/// VolumeError describes the possible error states when mounting persistent volume claims.
#[derive(Error, Debug)]
#[allow(clippy::enum_variant_names)]
enum VolumeError {
    #[error("bad volume mode")]
    BadVolumeMode,
    #[error("bad reclaim policy")]
    BadReclaimPolicy,
    #[error("bad access mode")]
    BadAccessMode,
}

// Kubernetes supports two volumeModes of PersistentVolumes: Filesystem and
// Block.
//
// volumeMode is an optional API parameter. Filesystem is the default mode used
// when volumeMode parameter is omitted.
//
// A volume with volumeMode: Filesystem is mounted into Pods into a directory.
// If the volume is backed by a block device and the device is empty,
// Kuberneretes creates a filesystem on the device before mounting it for the
// first time.
//
// You can set the value of volumeMode to Block to use a volume as a raw block
// device. Such volume is presented into a Pod as a block device, without any
// filesystem on it. This mode is useful to provide a Pod the fastest possible
// way to access a volume, without any filesystem layer between the Pod and the
// volume. On the other hand, the application running in the Pod must know how
// to handle a raw block device. See Raw Block Volume Support for an example on
// how to use a volume with volumeMode: Block in a Pod.
//
// https://kubernetes.io/docs/concepts/storage/persistent-volumes/#volume-mode
#[derive(Debug)]
enum VolumeMode {
    Block,
    Filesystem,
}

impl FromStr for VolumeMode {
    type Err = VolumeError;

    // defines what type of volume is required by the claim. The "filesystem"
    // mode is implied when not included in the claim spec.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "Block" => Ok(VolumeMode::Block),
            "Filesystem" => Ok(VolumeMode::Filesystem),
            "" => Ok(VolumeMode::Filesystem),
            _ => Err(VolumeError::BadVolumeMode),
        }
    }
}

#[derive(Debug)]
enum ReclaimPolicy {
    Delete,
    Recycle,
    Retain,
}

impl FromStr for ReclaimPolicy {
    type Err = VolumeError;

    // defines what type of reclaim policy is requested. The "delete" mode is
    // implied when not included in the storage class or persistent volume.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "Delete" => Ok(ReclaimPolicy::Delete),
            "Recycle" => Ok(ReclaimPolicy::Recycle),
            "Retain" => Ok(ReclaimPolicy::Retain),
            "" => Ok(ReclaimPolicy::Delete),
            _ => Err(VolumeError::BadReclaimPolicy),
        }
    }
}

#[derive(Debug)]
#[allow(clippy::enum_variant_names)]
enum AccessMode {
    ReadOnlyMany,
    ReadWriteMany,
    ReadWriteOnce,
}

impl FromStr for AccessMode {
    type Err = VolumeError;

    // defines what type of access mode is requested. An error is returned when
    // an invalid access mode is provided.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "ReadOnlyMany" => Ok(AccessMode::ReadOnlyMany),
            "ReadWriteMany" => Ok(AccessMode::ReadWriteMany),
            "ReadWriteOnce" => Ok(AccessMode::ReadWriteOnce),
            _ => Err(VolumeError::BadAccessMode),
        }
    }
}

/// A type that can manage a PVC volume with mounting and unmounting support. This type handles all
/// the underlying calls to the CSI driver
pub struct PvcVolume {
    name: String,
    client: kube::Client,
    // The whole PVC struct is very large, so I am boxing the larger data members to not make it
    // take up so much space on the stack (and it makes clippy happy)
    spec: Box<PersistentVolumeClaimSpec>,
    csi_client: NodeClient<tonic::transport::Channel>,
    csi_pv_source: Box<CSIPersistentVolumeSource>,
    mounted_path: Option<PathBuf>,
    // This allows us to keep a handle to the tempdir used if staging is enabled. When it is
    // dropped, cleanup of the directory will automatically happen
    staging_dir: Option<tempfile::TempDir>,
}

impl PvcVolume {
    /// Creates a new PVC volume from a Kubernetes volume object. Passing a non-PVC volume type will
    /// result in an error
    pub async fn new(
        vol: &KubeVolume,
        namespace: &str,
        client: kube::Client,
        plugin_registry: Option<Arc<PluginRegistry>>,
    ) -> anyhow::Result<Self> {
        let plugin_registry = match plugin_registry {
            Some(p) => p,
            None => {
                return Err(anyhow::anyhow!(
                    "cannot mount volume {}: CSI driver support not implemented",
                    vol.name
                ))
            }
        };

        let source = vol.persistent_volume_claim.as_ref().ok_or_else(|| {
            anyhow::anyhow!("Called a PVC volume constructor with a non-PVC volume")
        })?;

        let spec = get_pvc_spec(source, &client, namespace).await?;
        let csi_client = get_csi_client(&client, &spec, plugin_registry).await?;
        let csi_pv_source = get_csi(&client, source, &spec).await?;

        Ok(PvcVolume {
            name: vol.name.clone(),
            client,
            spec: Box::new(spec),
            csi_client,
            csi_pv_source: Box::new(csi_pv_source),
            mounted_path: None,
            staging_dir: None,
        })
    }

    /// Returns the path where the volume is mounted on the host. Will return `None` if the volume
    /// hasn't been mounted yet
    pub fn get_path(&self) -> Option<&Path> {
        self.mounted_path.as_deref()
    }

    /// Mounts the PVC volume in the given directory. The actual path will be
    /// $BASE_PATH/$VOLUME_NAME
    pub async fn mount(&mut self, base_path: impl AsRef<Path>) -> anyhow::Result<()> {
        let stage_unstage_volume = supports_stage_unstage(&mut self.csi_client).await?;

        let path = base_path.as_ref().join(&self.name);

        // we keep this around even if the driver does not support STAGE_UNSTAGE_VOLUME. unmount() still
        // needs it.
        tokio::fs::create_dir_all(&path).await?;
        // TODO(bacongobbler): implement node_unstage_volume(). We'll need to persist the staging_path
        // somewhere so we can recall that information during unpopulate()
        // ADDENDUM(thomastaylor312): Basically, it looks like most of the major providers don't support
        // stage/unstage, so for now we are going to defer implementing unstaging as passing that data
        // around is a little bit interesting with our current scheme

        // The call to .tempdir() includes blocking IO operations, so this is wrapped here
        // in order to spawn it on a separate thread pool so that we do not block this thread
        let staging_path_prefix = self.csi_pv_source.volume_handle.to_owned();
        let staging_path = tokio::task::spawn_blocking(move || {
            Builder::new().prefix(&staging_path_prefix).tempdir()
        })
        .await??;

        // We can unwrap safely here as `get_pvc_spec` validates all these fields
        let access_type = get_access_type(
            VolumeMode::from_str(&self.spec.volume_mode.clone().unwrap_or_default()).unwrap(),
            &self.csi_pv_source,
        );

        if stage_unstage_volume {
            let secrets = get_secrets_map(
                self.csi_pv_source.node_stage_secret_ref.clone(),
                &self.client,
            )
            .await?;
            stage_volume(
                &mut self.csi_client,
                &self.csi_pv_source,
                staging_path.path(),
                secrets,
                access_type.clone(),
            )
            .await?;
        }

        let secrets = get_secrets_map(
            self.csi_pv_source.node_publish_secret_ref.clone(),
            &self.client,
        )
        .await?;
        publish_volume(
            &mut self.csi_client,
            &self.csi_pv_source,
            staging_path.path(),
            stage_unstage_volume,
            &path,
            secrets,
            access_type,
        )
        .await?;

        self.mounted_path = Some(path);
        if stage_unstage_volume {
            self.staging_dir = Some(staging_path);
        }

        Ok(())
    }

    /// Unmounts the directory. Calling `unmount` on a directory that hasn't been mounted will log a
    /// warning, but otherwise not error
    pub async fn unmount(&mut self) -> anyhow::Result<()> {
        match self.mounted_path.take() {
            Some(p) => {
                // https://github.com/kubernetes/kubernetes/blob/6d5cb36d36f34cb4f5735b6adcd5ea8ebb4440ba/pkg/volume/csi/csi_mounter.go#L390
                unpublish_volume(&mut self.csi_client, &self.csi_pv_source, &p).await?;
                // Now remove the empty directory
                //although remove_dir_all crate could default to std::fs::remove_dir_all for unix family, we still prefer std::fs implemetation for unix
                #[cfg(target_family = "windows")]
                tokio::task::spawn_blocking(|| remove_dir_all::remove_dir_all(p)).await??;

                #[cfg(target_family = "unix")]
                tokio::fs::remove_dir_all(p).await?;
            }
            None => {
                warn!("Attempted to unmount PVC directory that wasn't mounted, this generally shouldn't happen");
            }
        }

        Ok(())
    }
}

// Validates a PersistentVolumeClaimSpec.
// https://github.com/kubernetes/kubernetes/blob/c970a46bc1bcc100bbbfabd5c12bd4c5d87f8aea/pkg/apis/core/validation/validation.go#L1965
pub(crate) fn validate(spec: &PersistentVolumeClaimSpec) -> anyhow::Result<()> {
    validate_access_modes(&spec.access_modes)?;

    validate_label_selector(spec.selector.as_ref())?;

    validate_storage_class(spec.storage_class_name.as_ref())?;

    validate_volume_mode(spec.volume_mode.as_ref())?;

    validate_data_source(spec.data_source.as_ref())?;

    Ok(())
}

fn validate_access_modes(modes: &[String]) -> anyhow::Result<()> {
    if modes.is_empty() {
        Err(anyhow::anyhow!("at least 1 access mode is required"))
    } else {
        for access_mode in modes {
            // validate access modes are correct
            AccessMode::from_str(access_mode)?;
        }
        Ok(())
    }
}

// TODO: remove this allow once failing validations are added.
#[allow(clippy::unnecessary_wraps)]
fn validate_label_selector(selector: Option<&LabelSelector>) -> anyhow::Result<()> {
    let _sel = match selector {
        None => return Ok(()),
        Some(s) => s,
    };

    // TODO: validate label selectors as done here:
    // https://github.com/kubernetes/kubernetes/blob/c970a46bc1bcc100bbbfabd5c12bd4c5d87f8aea/staging/src/k8s.io/apimachinery/pkg/apis/meta/v1/validation/validation.go.
    // This is a bunch of regex validation, so we may just want to ignore this step and let the API
    // call fail when we use the selectors instead. It just pushes the error down to the mounting
    // step
    Ok(())
}

fn validate_storage_class(class_name: Option<&String>) -> anyhow::Result<()> {
    match class_name {
        // According to the docs, a PVC can have no storage class set, it just means that it can
        // only bind to PVs with no class:
        // https://kubernetes.io/docs/concepts/storage/persistent-volumes/#class-1
        None => {
            info!("No storage class set. Kubelet may be unable to select a volume");
            Ok(())
        }
        Some(s) => {
            if s.is_empty() {
                return Err(anyhow::anyhow!(
                    "PersistentVolumeClaim must specify a storage class"
                ));
            }
            Ok(())
        }
    }
}

fn validate_volume_mode(mode: Option<&String>) -> anyhow::Result<()> {
    match mode {
        Some(a) => VolumeMode::from_str(a).map(|_| ()).map_err(|e| e.into()),
        None => Err(anyhow::anyhow!("at least 1 access mode is required")),
    }
}

// TODO: remove this allow once validations are added.
#[allow(clippy::unnecessary_wraps)]
fn validate_data_source(source: Option<&TypedLocalObjectReference>) -> anyhow::Result<()> {
    if source.is_some() {
        warn!("The `data_source` field is not currently supported in Krustlet. Support will be added once the feature hits beta")
    }
    Ok(())
}

// checks if the plugin supports the node_stage/unstage_volume API. Assume false if not specified.
async fn supports_stage_unstage(
    csi_client: &mut NodeClient<tonic::transport::Channel>,
) -> anyhow::Result<bool> {
    let mut stage_unstage_volume = false;
    let response = csi_client
        .node_get_capabilities(NodeGetCapabilitiesRequest {})
        .await?;
    for capability in &response.get_ref().capabilities {
        if let Some(typ) = &capability.r#type {
            let _typ_stage_unstage_volume = rpc::Type::StageUnstageVolume as i32;
            match typ {
                CapabilityType::Rpc(Rpc {
                    r#type: _typ_stage_unstage_volume,
                }) => {
                    stage_unstage_volume = true;
                }
            }
        }
    }
    Ok(stage_unstage_volume)
}

async fn stage_volume(
    csi_client: &mut NodeClient<tonic::transport::Channel>,
    csi: &CSIPersistentVolumeSource,
    staging_path: &Path,
    secrets: BTreeMap<String, String>,
    access_type: CSIAccessType,
) -> anyhow::Result<()> {
    // TODO: grab the publish_context and volume_context using the volume attachments API.
    // NOTE: The volume attachments API is referenced in Kubelet, but the information it provides
    // seems to be handled by info on a PVC and calls to the CSI plugin. So we have NO IDEA if this
    // is even doable or useful
    csi_client
        .node_stage_volume(NodeStageVolumeRequest {
            volume_id: csi.volume_handle.clone(),
            staging_target_path: staging_path.to_string_lossy().to_string(),
            volume_capability: Some(VolumeCapability {
                // TODO: determine the correct access mode and mount flags from the volume
                // https://github.com/kubernetes/kubernetes/blob/734889ed822d1a60c6dd61ccd8f1ed0e8ab31ea5/pkg/volume/csi/csi_attacher.go#L325-L333
                access_mode: Some(CSIAccessMode {
                    mode: CSIMode::SingleNodeWriter as i32,
                }),
                access_type: Some(access_type),
            }),
            secrets,
            publish_context: Default::default(),
            volume_context: Default::default(),
        })
        .await?;
    Ok(())
}

async fn publish_volume(
    csi_client: &mut NodeClient<tonic::transport::Channel>,
    csi: &CSIPersistentVolumeSource,
    staging_path: &Path,
    stage_unstage_volume: bool,
    path: &Path,
    secrets: BTreeMap<String, String>,
    access_type: CSIAccessType,
) -> anyhow::Result<()> {
    let mut req = NodePublishVolumeRequest {
        volume_id: csi.volume_handle.clone(),
        target_path: path.to_string_lossy().to_string(),
        staging_target_path: "".to_owned(),
        volume_capability: Some(VolumeCapability {
            // TODO: determine the correct access mode and mount flags from the volume
            // https://github.com/kubernetes/kubernetes/blob/734889ed822d1a60c6dd61ccd8f1ed0e8ab31ea5/pkg/volume/csi/csi_attacher.go#L325-L333
            access_mode: Some(CSIAccessMode {
                mode: CSIMode::SingleNodeWriter as i32,
            }),
            access_type: Some(access_type),
        }),
        readonly: csi.read_only.unwrap_or_default(),
        secrets,
        publish_context: Default::default(),
        volume_context: Default::default(),
    };
    if stage_unstage_volume {
        req.staging_target_path = staging_path.to_string_lossy().to_string();
    }
    csi_client.node_publish_volume(req).await?;
    Ok(())
}

async fn unpublish_volume(
    csi_client: &mut NodeClient<tonic::transport::Channel>,
    csi: &CSIPersistentVolumeSource,
    path: &Path,
) -> anyhow::Result<()> {
    let req = NodeUnpublishVolumeRequest {
        volume_id: csi.volume_handle.clone(),
        target_path: path.to_string_lossy().to_string(),
    };
    csi_client.node_unpublish_volume(req).await?;
    Ok(())
}

async fn get_csi_client(
    client: &kube::Client,
    spec: &PersistentVolumeClaimSpec,
    plugin_registry: Arc<PluginRegistry>,
) -> anyhow::Result<NodeClient<tonic::transport::Channel>> {
    let storage_class_client: Api<StorageClass> = Api::all(client.clone());
    // NOTE(thomastaylor312): Storage class names are not required (see comment in validate
    // function), so we can just try to find one with an empty string
    let def = String::default();
    let storage_class = storage_class_client
        .get(spec.storage_class_name.as_ref().unwrap_or(&def))
        .await?;
    let endpoint = plugin_registry
        .get_endpoint(&storage_class.provisioner)
        .await
        .ok_or_else(|| anyhow::anyhow!("could not get CSI plugin endpoint"))?;
    let chan = grpc_sock::client::socket_channel(endpoint).await?;
    Ok(NodeClient::new(chan))
}

async fn get_csi(
    client: &kube::Client,
    pvc_source: &PersistentVolumeClaimVolumeSource,
    spec: &PersistentVolumeClaimSpec,
) -> anyhow::Result<CSIPersistentVolumeSource> {
    let volume_name = spec.volume_name.as_ref().ok_or(anyhow::anyhow!(format!(
        "volume name for PVC {} must exist (is the volume bound?)",
        pvc_source.claim_name
    )))?;

    let pv_client: Api<PersistentVolume> = Api::all(client.clone());
    let pv = pv_client.get(&volume_name).await?;

    // https://github.com/kubernetes/kubernetes/blob/734889ed822d1a60c6dd61ccd8f1ed0e8ab31ea5/pkg/volume/csi/csi_attacher.go#L295-L298
    let csi = pv
        .spec
        .ok_or_else(|| anyhow::anyhow!("no PersistentVolume spec defined"))?
        .csi
        .ok_or_else(|| anyhow::anyhow!("no CSI spec defined"))?;
    Ok(csi)
}

async fn get_pvc_spec(
    pvc_source: &PersistentVolumeClaimVolumeSource,
    client: &kube::Client,
    namespace: &str,
) -> anyhow::Result<PersistentVolumeClaimSpec> {
    let pvc_client: Api<PersistentVolumeClaim> = Api::namespaced(client.clone(), namespace);

    let pvc = pvc_client.get(&pvc_source.claim_name).await?;
    let spec = match pvc.spec {
        Some(s) => s,
        None => {
            return Err(anyhow::anyhow!("PersistentVolumeClaim must specify a spec"));
        }
    };
    validate(&spec)?;
    Ok(spec)
}

async fn get_secrets_map(
    secret_ref: Option<SecretReference>,
    client: &kube::Client,
) -> anyhow::Result<BTreeMap<String, String>> {
    match secret_ref {
        None => Ok(BTreeMap::default()),
        Some(reference) => {
            let name = match reference.name {
                None => {
                    warn!("CSI source had a secret reference, but no secret name. Skipping secret fetch");
                    return Ok(BTreeMap::default());
                }
                Some(n) => n,
            };
            let namespace = match reference.namespace {
                None => {
                    warn!("CSI source had a secret reference, but no secret namespace. Skipping secret fetch");
                    return Ok(BTreeMap::default());
                }
                Some(n) => n,
            };
            let secret_client: Api<Secret> = Api::namespaced(client.clone(), &namespace);
            let secret = secret_client.get(&name).await?;
            secret
                .data
                .into_iter()
                .map(|(k, v)| {
                    // NOTE: So the CSI API wants the secret values as Strings. However, secrets can be
                    // arbitrary data. So we are blatantly converting this to UTF-8 in a safe way even
                    // if it isn't a String
                    let decoded = String::from_utf8_lossy(&base64::decode(v.0)?).into_owned();
                    Ok((k, decoded))
                })
                .collect::<anyhow::Result<BTreeMap<String, String>>>()
        }
    }
}

fn get_access_type(mode: VolumeMode, csi: &CSIPersistentVolumeSource) -> CSIAccessType {
    match mode {
        VolumeMode::Block => CSIAccessType::Block(BlockVolume {}),
        VolumeMode::Filesystem => CSIAccessType::Mount(CSIMountVolume {
            fs_type: csi.fs_type.clone().unwrap_or_default(),
            mount_flags: Default::default(),
        }),
    }
}
