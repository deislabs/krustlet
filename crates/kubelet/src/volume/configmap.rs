use std::path::Path;

use k8s_openapi::api::core::v1::{ConfigMap, KeyToPath, Volume as KubeVolume};
use k8s_openapi::ByteString;
use tracing::warn;

use super::*;
/// A type that can manage a ConfigMap volume with mounting and unmounting support
pub struct ConfigMapVolume {
    vol_name: String,
    cm_name: String,
    client: kube::Api<ConfigMap>,
    items: Vec<KeyToPath>,
    mounted_path: Option<PathBuf>,
}

impl ConfigMapVolume {
    /// Creates a new ConfigMap volume from a Kubernetes volume object. Passing a non-ConfigMap
    /// volume type will result in an error
    pub fn new(vol: &KubeVolume, namespace: &str, client: kube::Client) -> anyhow::Result<Self> {
        let cm_source = vol.config_map.as_ref().ok_or_else(|| {
            anyhow::anyhow!("Called a ConfigMap volume constructor with a non-ConfigMap volume")
        })?;
        Ok(ConfigMapVolume {
            vol_name: vol.name.clone(),
            cm_name: cm_source
                .name
                .clone()
                .ok_or_else(|| anyhow::anyhow!("no ConfigMap name was given"))?,
            client: Api::namespaced(client, namespace),
            items: cm_source.items.clone(),
            mounted_path: None,
        })
    }

    /// Returns the path where the volume is mounted on the host. Will return `None` if the volume
    /// hasn't been mounted yet
    pub fn get_path(&self) -> Option<&Path> {
        self.mounted_path.as_deref()
    }

    /// Mounts the ConfigMap volume in the given directory. The actual path will be
    /// $BASE_PATH/$VOLUME_NAME
    pub async fn mount(&mut self, base_path: impl AsRef<Path>) -> anyhow::Result<()> {
        let path = base_path.as_ref().join(&self.vol_name);
        tokio::fs::create_dir_all(&path).await?;

        self.mount_at(path.clone()).await?;

        // Set configmap directory to read-only.
        let mut perms = tokio::fs::metadata(&path).await?.permissions();
        perms.set_readonly(true);
        tokio::fs::set_permissions(path, perms).await?;

        Ok(())
    }

    /// A function for mounting the file(s) at the given path. It mainly exists to allow the
    /// projected volumes to mount everything at the same level. The given path must be a directory
    /// and already exist. This method will not set any permissions, so the caller is responsible
    /// for setting permissions on the directory
    pub(crate) async fn mount_at(&mut self, path: PathBuf) -> anyhow::Result<()> {
        let config_map = self.client.get(&self.cm_name).await?;
        let binary_data = config_map.binary_data;
        let binary_data = binary_data
            .into_iter()
            .filter_map(
                |(key, ByteString(data))| match mount_setting_for(&key, &self.items) {
                    ItemMount::MountAt(mount_path) => Some((path.join(mount_path), data)),
                    ItemMount::DoNotMount => None,
                },
            )
            .map(|(file_path, data)| async move { tokio::fs::write(file_path, &data).await });
        let binary_data = futures::future::join_all(binary_data);

        let data = config_map.data;
        let data = data
            .into_iter()
            .filter_map(|(key, data)| match mount_setting_for(&key, &self.items) {
                ItemMount::MountAt(mount_path) => Some((path.join(mount_path), data)),
                ItemMount::DoNotMount => None,
            })
            .map(|(file_path, data)| async move { tokio::fs::write(file_path, &data).await });
        let data = futures::future::join_all(data);

        let (binary_data, data) = futures::future::join(binary_data, data).await;
        binary_data
            .into_iter()
            .chain(data)
            .collect::<tokio::io::Result<_>>()?;

        // Update the mounted directory
        self.mounted_path = Some(path);

        Ok(())
    }

    /// Unmounts the directory, which removes all files. Calling `unmount` on a directory that
    /// hasn't been mounted will log a warning, but otherwise not error
    pub async fn unmount(&mut self) -> anyhow::Result<()> {
        match self.mounted_path.take() {
            Some(p) => {
                // Because things are set to read only, we need to remove the read only flag so it
                // can be deleted
                let mut perms = tokio::fs::metadata(&p).await?.permissions();
                perms.set_readonly(false);
                tokio::fs::set_permissions(&p, perms).await?;

                //although remove_dir_all crate could default to std::fs::remove_dir_all for unix family, we still prefer std::fs implemetation for unix
                #[cfg(target_family = "windows")]
                tokio::task::spawn_blocking(|| remove_dir_all::remove_dir_all(p)).await??;

                #[cfg(target_family = "unix")]
                tokio::fs::remove_dir_all(p).await?;
            }
            None => {
                warn!("Attempted to unmount ConfigMap directory that wasn't mounted, this generally shouldn't happen");
            }
        }
        Ok(())
    }
}
