use std::path::Path;

use either::Either;
use k8s_openapi::api::authentication::v1::{BoundObjectReference, TokenRequest, TokenRequestSpec};
use k8s_openapi::api::core::v1::{
    ConfigMapVolumeSource, DownwardAPIVolumeSource, Pod as KubePod, SecretVolumeSource,
    Volume as KubeVolume, VolumeProjection,
};
use k8s_openapi::Resource;
use tracing::warn;

use super::*;

/// A type that can manage a Secret volume with mounting and unmounting support
pub struct ProjectedVolume {
    vol_name: String,
    volumes: Vec<super::VolumeRef>,
    service_accounts: Vec<ServiceAccountSource>,
    mounted_path: Option<PathBuf>,
}

struct ServiceAccountSource {
    file_name: String,
    service_account_name: String,
    namespace: String,
    client: kube::Client,
    audience: String,
    expiration_time: i64,
    pod_name: String,
    pod_uid: String,
}

impl ServiceAccountSource {
    async fn mount_at(&mut self, path: impl AsRef<Path>) -> anyhow::Result<()> {
        // As far as I can tell, this is the only way to access the token subresource on service accounts
        let (req, _) = TokenRequest::create_namespaced_service_account_token(
            &self.service_account_name,
            &self.namespace,
            &TokenRequest {
                spec: TokenRequestSpec {
                    audiences: vec![self.audience.clone()],
                    expiration_seconds: Some(self.expiration_time),
                    bound_object_ref: Some(BoundObjectReference {
                        api_version: Some(KubePod::API_VERSION.to_owned()),
                        kind: Some(KubePod::KIND.to_owned()),
                        name: Some(self.pod_name.clone()),
                        uid: Some(self.pod_uid.clone()),
                    }),
                },
                ..Default::default()
            },
            Default::default(),
        )?;
        // Get the token from the API
        let token_resp: TokenRequest = self.client.request(req).await?;
        let mount_path = path.as_ref().join(&self.file_name);

        let token = token_resp
            .status
            .ok_or_else(|| anyhow::anyhow!("Service account token was not issued"))?
            .token;
        tokio::fs::write(&mount_path, token).await?;

        // TODO(thomastaylor312): Right now we don't automatically rotate the token. We should
        // probably spawn a task as part of this VolumeRef to auto-rotate the token that drops along
        // with the rest of the ProjectedVolume type

        Ok(())
    }
}

impl ProjectedVolume {
    /// Creates a new Projected volume from a Kubernetes volume object. Passing a non-Projected
    /// volume type will result in an error.
    pub fn new(vol: &KubeVolume, pod: Pod, client: kube::Client) -> anyhow::Result<Self> {
        let source = vol.projected.as_ref().ok_or_else(|| {
            anyhow::anyhow!("Called a Projected volume constructor with a non-projected volume")
        })?;
        let mut volumes = Vec::new();
        let mut service_accounts = Vec::new();
        for s in source
            .sources
            .iter()
            .map(|proj| (client.clone(), proj))
            .map(|(c, proj)| to_volume_ref(c, &pod, proj))
            .collect::<anyhow::Result<Vec<Either<_, _>>>>()?
            .into_iter()
        {
            match s {
                Either::Left(v) => volumes.push(v),
                Either::Right(sa) => service_accounts.push(sa),
            }
        }
        Ok(ProjectedVolume {
            vol_name: vol.name.clone(),
            volumes,
            service_accounts,
            mounted_path: None,
        })
    }

    /// Returns the path where the volume is mounted on the host. Will return `None` if the volume
    /// hasn't been mounted yet
    pub fn get_path(&self) -> Option<&Path> {
        self.mounted_path.as_deref()
    }

    /// Mounts the Secret volume in the given directory. The actual path will be
    /// $BASE_PATH/$VOLUME_NAME
    #[async_recursion::async_recursion]
    pub async fn mount<P: AsRef<Path> + Send + 'static>(
        &mut self,
        base_path: P,
    ) -> anyhow::Result<()> {
        let path = base_path.as_ref().join(&self.vol_name);
        tokio::fs::create_dir_all(&path).await?;

        let vol_futures =
            self.volumes
                .iter_mut()
                .map(|v| (path.clone(), v))
                .map(|(p, v)| async move {
                    match v {
                        VolumeRef::ConfigMap(c) => c.mount_at(p).await,
                        VolumeRef::DownwardApi(d) => d.mount_at(p).await,
                        VolumeRef::Secret(s) => s.mount_at(p).await,
                        // This is iterating over something completely internal to the type. There is
                        // never a case where we should get another volume and if we do, that is
                        // programmer error
                        _ => panic!("Got unrecognized volume type, this is a programmer error"),
                    }
                });

        let sa_futures = self
            .service_accounts
            .iter_mut()
            .map(|s| (path.clone(), s))
            .map(|(p, s)| async move { s.mount_at(p).await });

        // Join together all of the futures and then collect any errors. We can't just chain
        // together the future iterators because they technically have different types
        let (res1, res2) = futures::future::join(
            futures::future::join_all(vol_futures),
            futures::future::join_all(sa_futures),
        )
        .await;
        res1.into_iter()
            .chain(res2.into_iter())
            .collect::<anyhow::Result<()>>()?;

        self.mounted_path = Some(path);

        Ok(())
    }

    /// Unmounts the directory, which removes all files. Calling `unmount` on a directory that
    /// hasn't been mounted will log a warning, but otherwise not error
    pub async fn unmount(&mut self) -> anyhow::Result<()> {
        match self.mounted_path.take() {
            Some(p) => {
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

// Default audience should be the apiserver as specified here:
// https://kubernetes.io/docs/reference/kubernetes-api/config-and-storage-resources/volume/#projections
const DEFAULT_AUDIENCE: &str = "api";
// Default expiration time for a token is 1 hour as specified here:
// https://kubernetes.io/docs/reference/kubernetes-api/config-and-storage-resources/volume/#projections
const DEFAULT_EXPIRATION_SECONDS: i64 = 3600;

fn to_volume_ref(
    client: kube::Client,
    pod: &Pod, // take a borrowed reference to the pod so we only clone when needed
    proj: &VolumeProjection,
) -> anyhow::Result<Either<super::VolumeRef, ServiceAccountSource>> {
    // Assemble a volume type to use in constructing each of our VolumeRefs
    if let Some(s) = proj.secret.as_ref() {
        let vol = KubeVolume {
            // this doesn't matter as we are using `mount_at`, but is a required field
            name: "secret-projection".into(),
            secret: Some(SecretVolumeSource {
                items: s.items.to_owned(),
                secret_name: s.name.to_owned(),
                optional: s.optional.to_owned(),
                ..Default::default()
            }),
            ..Default::default()
        };
        Ok(Either::Left(VolumeRef::Secret(SecretVolume::new(
            &vol,
            pod.namespace(),
            client,
        )?)))
    } else if let Some(cm) = proj.config_map.as_ref() {
        let vol = KubeVolume {
            // this doesn't matter as we are using `mount_at`, but is a required field
            name: "configmap-projection".into(),
            config_map: Some(ConfigMapVolumeSource {
                items: cm.items.to_owned(),
                name: cm.name.to_owned(),
                optional: cm.optional.to_owned(),
                ..Default::default()
            }),
            ..Default::default()
        };
        Ok(Either::Left(VolumeRef::ConfigMap(ConfigMapVolume::new(
            &vol,
            pod.namespace(),
            client,
        )?)))
    } else if let Some(d) = proj.downward_api.as_ref() {
        let vol = KubeVolume {
            // this doesn't matter as we are using `mount_at`, but is a required field
            name: "downwardapi-projection".into(),
            downward_api: Some(DownwardAPIVolumeSource {
                items: d.items.to_owned(),
                ..Default::default()
            }),
            ..Default::default()
        };
        Ok(Either::Left(VolumeRef::DownwardApi(
            DownwardApiVolume::new(&vol, pod.to_owned())?,
        )))
    } else if let Some(sa) = proj.service_account_token.as_ref() {
        Ok(Either::Right(ServiceAccountSource{
            file_name: sa.path.to_owned(),
            service_account_name: pod.service_account_name().ok_or_else(|| anyhow::anyhow!("Unable to create a service account token projection. The pod is missing a service account"))?.to_owned(),
            namespace: pod.namespace().to_owned(),
            client,
            audience: sa.audience.to_owned().unwrap_or_else(|| String::from(DEFAULT_AUDIENCE)),
            expiration_time: sa.expiration_seconds.unwrap_or(DEFAULT_EXPIRATION_SECONDS),
            pod_name: pod.name().to_owned(),
            pod_uid: pod.pod_uid().to_owned(),
        }))
    } else {
        Err(anyhow::anyhow!("No source specified in projected source"))
    }
}
