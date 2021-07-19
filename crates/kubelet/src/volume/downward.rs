use std::collections::BTreeMap;
use std::path::Path;

use k8s_openapi::{
    api::core::v1::{
        DownwardAPIVolumeFile, ObjectFieldSelector, ResourceFieldSelector, Volume as KubeVolume,
    },
    apimachinery::pkg::api::resource::Quantity as KubeQuantity,
};
use tracing::warn;

use crate::container::Container;
use crate::resources::quantity::{self, Quantity, QuantityType, Suffix};

use super::*;
/// A type that can manage a Downward API volume with mounting and unmounting support
pub struct DownwardApiVolume {
    vol_name: String,
    pod: Pod,
    items: Vec<DownwardAPIVolumeFile>,
    mounted_path: Option<PathBuf>,
}

impl DownwardApiVolume {
    /// Creates a new Downward API volume from a Kubernetes volume object. Passing a non-Downward
    /// API volume type will result in an error
    pub fn new(vol: &KubeVolume, pod: Pod) -> anyhow::Result<Self> {
        let da_source = vol.downward_api.as_ref().ok_or_else(|| {
            anyhow::anyhow!(
                "Called a Downward API volume constructor with a non-Downward API volume"
            )
        })?;
        Ok(DownwardApiVolume {
            vol_name: vol.name.clone(),
            pod,
            items: da_source.items.clone(),
            mounted_path: None,
        })
    }

    /// Returns the path where the volume is mounted on the host. Will return `None` if the volume
    /// hasn't been mounted yet
    pub fn get_path(&self) -> Option<&Path> {
        self.mounted_path.as_deref()
    }

    /// Mounts the Downward API volume in the given directory. The actual path will be
    /// $BASE_PATH/$VOLUME_NAME
    pub async fn mount(&mut self, base_path: impl AsRef<Path>) -> anyhow::Result<()> {
        let path = base_path.as_ref().join(&self.vol_name);
        tokio::fs::create_dir_all(&path).await?;
        self.mount_at(path.clone()).await?;

        // Set directory to read-only.
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
        // Mount field refs
        let field_refs = self
            .items
            .iter()
            .filter_map(|d| {
                d.field_ref
                    .as_ref()
                    .map(|f| (path.join(&d.path), data_from_field_ref(f, &self.pod)))
            })
            .map(|(p, res)| async move {
                let data = res?;
                tokio::fs::write(p, &data).await.map_err(|e| e.into())
            });
        let field_refs = futures::future::join_all(field_refs);

        let containers = self.pod.containers();
        let resource_refs = self
            .items
            .iter()
            .filter_map(|d| {
                d.resource_field_ref
                    .as_ref()
                    .map(|f| (path.join(&d.path), data_from_resource_ref(f, &containers)))
            })
            .map(|(p, res)| async move {
                let data = res?;
                tokio::fs::write(p, &data).await.map_err(|e| e.into())
            });
        let resource_refs = futures::future::join_all(resource_refs);

        let (field_refs, resource_refs) = futures::future::join(field_refs, resource_refs).await;
        field_refs
            .into_iter()
            .chain(resource_refs)
            .collect::<anyhow::Result<_>>()?;

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
                warn!("Attempted to unmount Downward API directory that wasn't mounted, this generally shouldn't happen");
            }
        }
        Ok(())
    }
}

/// Generates the data for the given field ref. This handling is a little bit different from the
/// environment variables version of the downward API because there are other fields allowed in the
/// environment variables and you can also get all of the labels in a single file using the volume.
/// See:
/// https://kubernetes.io/docs/tasks/inject-data-application/downward-api-volume-expose-pod-information/#capabilities-of-the-downward-api
fn data_from_field_ref(field_ref: &ObjectFieldSelector, pod: &Pod) -> anyhow::Result<Vec<u8>> {
    let (first, path) = field_ref.field_path.split_once('.').ok_or_else(|| {
        anyhow::anyhow!(
            "Path {} should be of the format metadata.<KEY_NAME>. No '.' separator was found",
            field_ref.field_path
        )
    })?;
    if first != "metadata" {
        anyhow::bail!(
            "Downward API access for volumes is only allowed with 'metadata'. Found {}",
            first
        );
    }

    // Label/annotation keys do not allow braces, so if they exist, splitting at the opening brace
    // should be valid or at least result in an invalid key if used maliciously
    let has_start_brace = path.contains('[');
    let has_end_brace = path.ends_with(']');
    let is_label_or_annotation = path.contains("labels") || path.contains("annotations");

    // Brief overview of what is being checked here as a precaution (since I think these fields are checked for validity by the API, but I am not sure):
    // 1. If the path contains any braces and isn't for labels or annotations, error
    // 2. If the path is missing a start or end brace (but has at least one) and is an annotation or label, error
    // 3. If all the braces are valid and it is a label or annotation, parse the key
    // 4. Otherwise, treat as a normal path
    let (path, key) = if (has_start_brace || has_end_brace) && !is_label_or_annotation {
        anyhow::bail!("Invalid path syntax, only 'labels' or 'annotations' can have key lookup (e.g. metadata.labels['mylabel'])")
    } else if (!has_start_brace ^ !has_end_brace) && is_label_or_annotation {
        anyhow::bail!(
            "Invalid path syntax, missing starting or ending brace for label/annotation key lookup"
        );
    } else if has_start_brace && has_end_brace && is_label_or_annotation {
        // We can unwrap here because we already validated there is a start brace
        let (path, rest) = path.split_once('[').unwrap();
        // Trim off the remaining expected characters
        // TODO: Do we want to validate open and close single quotes here?
        let trim_pat: &[_] = &['\'', ']'];
        let key = rest.trim_matches(trim_pat);
        (path, Some(key))
    } else {
        (path, None)
    };

    // Now grab the data
    Ok(match (path, key) {
        ("name", None) => pod.name().as_bytes().to_vec(),
        ("namespace", None) => pod.namespace().as_bytes().to_vec(),
        ("uid", None) => pod.pod_uid().as_bytes().to_vec(),
        ("labels", None) => btree_to_data(pod.labels()),
        ("labels", Some(key)) => pod
            .labels()
            .get(key)
            .ok_or_else(|| anyhow::anyhow!("Key {} was not found in labels", key))?
            .as_bytes()
            .to_vec(),
        ("annotations", None) => btree_to_data(pod.annotations()),
        ("annotations", Some(key)) => pod
            .annotations()
            .get(key)
            .ok_or_else(|| anyhow::anyhow!("Key {} was not found in annotations", key))?
            .as_bytes()
            .to_vec(),
        _ => anyhow::bail!("Field ref {} does not exist", field_ref.field_path),
    })
}

/// Converts an annotation or labels to a per-line printing with the label and value
fn btree_to_data(data: &BTreeMap<String, String>) -> Vec<u8> {
    data.iter()
        .map(|(k, v)| format!("{}=\"{}\"", k, v))
        .collect::<Vec<String>>()
        .join("\n")
        .into_bytes()
}

fn data_from_resource_ref(
    resource_ref: &ResourceFieldSelector,
    containers: &[Container],
) -> anyhow::Result<Vec<u8>> {
    let (kind, path) = resource_ref.resource.split_once('.').ok_or_else(|| {
        anyhow::anyhow!(
            "Path {} should be of the format requests.<KEY_NAME>. No '.' separator was found",
            resource_ref.resource
        )
    })?;

    if kind != "requests" && kind != "limits" {
        anyhow::bail!(
            "Downward API resource ref only supports 'requests' and 'limits'. Found {}",
            kind
        );
    }

    let empty_string = String::new();
    let container_name = resource_ref
        .container_name
        .as_ref()
        .unwrap_or(&empty_string);
    let container = containers
        .iter()
        .find(|c| c.name() == container_name)
        .ok_or_else(|| {
            anyhow::anyhow!(
                "Container with name of {} does not exist in the pod",
                container_name
            )
        })?;
    let resources = container
        .resources()
        .ok_or_else(|| anyhow::anyhow!("No resources were found on the container"))?;
    let empty_quantity = KubeQuantity::default();
    match (kind, path) {
        ("requests", "cpu") => calculate_value(
            QuantityType::Cpu(
                resources
                    .requests
                    .get("cpu")
                    .ok_or_else(|| anyhow::anyhow!("A CPU request value was not found"))?,
            ),
            resource_ref.divisor.as_ref(),
        ),
        ("requests", "memory") => calculate_value(
            QuantityType::Memory(
                resources
                    .requests
                    .get("memory")
                    .ok_or_else(|| anyhow::anyhow!("A memory request value was not found"))?,
            ),
            resource_ref.divisor.as_ref(),
        ),
        // TODO(thomastaylor312): According to the docs, if a limit is not specified, we should
        // default to the node allocatable value for CPU and memory. We have no easy way to access
        // that here, so for now we are just defaulting
        ("limits", "cpu") => calculate_value(
            QuantityType::Cpu(resources.limits.get("cpu").unwrap_or(&empty_quantity)),
            resource_ref.divisor.as_ref(),
        ),
        ("limits", "memory") => calculate_value(
            QuantityType::Memory(resources.limits.get("memory").unwrap_or(&empty_quantity)),
            resource_ref.divisor.as_ref(),
        ),
        _ => anyhow::bail!("Resource ref {} does not exist", resource_ref.resource),
    }
}

fn calculate_value(
    data: QuantityType<'_>,
    divisor: Option<&KubeQuantity>,
) -> anyhow::Result<Vec<u8>> {
    // NOTE: This assumes that the API has validated the fields and so we shouldn't get a divisor
    // that doesn't match the type. We may have to revisit that assumption
    let suffix = match divisor {
        Some(q) => quantity::get_suffix(q),
        None if matches!(data, QuantityType::Cpu(_)) => Suffix::Millicpu,
        None if matches!(data, QuantityType::Memory(_)) => Suffix::Mebibyte,
        None => Suffix::None,
    };

    let mut quantity_str = match Quantity::from_kube_quantity(data)? {
        Quantity::Cpu(v) => (v / suffix.get_value()).to_string(),
        Quantity::Memory(v) => ((v as f64 / suffix.get_value()) as u128).to_string(),
    };
    // Push the suffix on the end
    quantity_str.push_str(suffix.as_ref());
    Ok(quantity_str.into_bytes())
}

#[cfg(test)]
mod test {
    use super::*;

    #[tokio::test]
    async fn test_valid_mount() {
        let pod_namespace = "test";
        let component_label = "kube-thingz";
        let tier_label = "control-plane";
        let managed_label = "helm";
        let config_hash = "foobar";
        let custom_annotation = "kustomannotation";
        let pod_uid = "a-uid";
        let fake_pod = serde_json::json!({
            "kind": "Pod",
            "apiVersion": "v1",
            "metadata": {
                "name": "test-pod",
                "namespace": pod_namespace,
                "labels": {
                    "component": component_label,
                    "tier": tier_label,
                    "app.kubernetes.io/managed-by": managed_label
                },
                "annotations": {
                    "kubernetes.io/config.hash": config_hash,
                    "mycustomannotation": custom_annotation,
                },
                "uid": pod_uid
            },
            "spec": {
                "containers": [
                    {
                        "name": "test-container",
                        "resources": {
                            "requests": {
                                "memory": "1024",
                                "cpu": "500m",
                            },
                            "limits": {
                                "memory": ".5G",
                                "cpu": "1.25",
                            }
                        }
                    }
                ],
                "volumes": [
                    {
                        "name": "podinfo",
                        "downwardAPI": {
                            "items": [
                                {
                                    "path": "cpu_limit",
                                    "resourceFieldRef": {
                                        "containerName": "test-container",
                                        "resource": "limits.cpu",
                                        "divisor": "1m",
                                    }
                                },
                                {
                                    "path": "cpu_request",
                                    "resourceFieldRef": {
                                        "containerName": "test-container",
                                        "resource": "requests.cpu",
                                        "divisor": "1",
                                    }
                                },
                                {
                                    "path": "memory_limit",
                                    "resourceFieldRef": {
                                        "containerName": "test-container",
                                        "resource": "limits.memory",
                                        "divisor": "1M",
                                    }
                                },
                                {
                                    "path": "memory_request",
                                    "resourceFieldRef": {
                                        "containerName": "test-container",
                                        "resource": "requests.memory",
                                        "divisor": "1Ki",
                                    }
                                },
                                {
                                    "path": "pod_name",
                                    "fieldRef": {
                                        "fieldPath": "metadata.name"
                                    }
                                },
                                {
                                    "path": "pod_namespace",
                                    "fieldRef": {
                                        "fieldPath": "metadata.namespace"
                                    }
                                },
                                {
                                    "path": "pod_uid",
                                    "fieldRef": {
                                        "fieldPath": "metadata.uid"
                                    }
                                },
                                {
                                    "path": "all_labels",
                                    "fieldRef": {
                                        "fieldPath": "metadata.labels"
                                    }
                                },
                                {
                                    "path": "all_annotations",
                                    "fieldRef": {
                                        "fieldPath": "metadata.annotations"
                                    }
                                },
                                {
                                    "path": "normal_label",
                                    "fieldRef": {
                                        "fieldPath": "metadata.labels['component']"
                                    }
                                },
                                {
                                    "path": "pathed_label",
                                    "fieldRef": {
                                        "fieldPath": "metadata.labels['app.kubernetes.io/managed-by']"
                                    }
                                },
                                {
                                    "path": "normal_annotation",
                                    "fieldRef": {
                                        "fieldPath": "metadata.annotations['mycustomannotation']"
                                    }
                                },
                                {
                                    "path": "pathed_annotation",
                                    "fieldRef": {
                                        "fieldPath": "metadata.annotations['kubernetes.io/config.hash']"
                                    }
                                },
                            ]
                        }
                    }
                ]
            }
        });
        let fake_pod: Pod = serde_json::from_value(fake_pod).unwrap();
        let vol = fake_pod.volumes()[0].clone();
        let mut downward = DownwardApiVolume::new(&vol, fake_pod)
            .expect("Should be able to create a new DownwardApiVolume");
        // Setup a tempdir where we can mount things at
        let tempdir = tempfile::tempdir().expect("Unable to create tempdir");
        downward
            .mount(tempdir.path())
            .await
            .expect("Mounting should work properly");

        // Test that all the data is there and valid
        let vol_dir = downward
            .get_path()
            .expect("A mounted volume should have a mounted path");

        // Check for unit formatting for resources
        assert_content(
            vol_dir.join("cpu_limit"),
            "1250m",
            "CPU limit should exist and be written with the correct units",
        )
        .await;
        assert_content(
            vol_dir.join("cpu_request"),
            "0.5",
            "CPU request should exist and be written with the correct units",
        )
        .await;
        assert_content(
            vol_dir.join("memory_limit"),
            "500M",
            "Memory limit should exist and be written with the correct units",
        )
        .await;
        assert_content(
            vol_dir.join("memory_request"),
            "1Ki",
            "Memory request should exist and be written with the correct units",
        )
        .await;

        // Now move on to individual metadata items
        assert_content(
            vol_dir.join("pod_name"),
            "test-pod",
            "Pod name should be correct",
        )
        .await;
        assert_content(
            vol_dir.join("pod_namespace"),
            pod_namespace,
            "Pod namespace should be correct",
        )
        .await;
        assert_content(
            vol_dir.join("pod_uid"),
            pod_uid,
            "Pod UID should be correct",
        )
        .await;
        assert_content(
            vol_dir.join("normal_label"),
            component_label,
            "Normal label should be correct",
        )
        .await;
        assert_content(
            vol_dir.join("pathed_label"),
            managed_label,
            "Pathed label should be correct",
        )
        .await;
        assert_content(
            vol_dir.join("normal_annotation"),
            custom_annotation,
            "Normal annotation should be correct",
        )
        .await;
        assert_content(
            vol_dir.join("pathed_annotation"),
            config_hash,
            "Pathed annotation should be correct",
        )
        .await;

        // Now test all labels and all annotations. Due to it being stored in a Btreemap, the keys
        // will be written out in sorted order
        let expected = format!(
            r#"app.kubernetes.io/managed-by="{}"
component="{}"
tier="{}""#,
            managed_label, component_label, tier_label
        );

        assert_content(
            vol_dir.join("all_labels"),
            &expected,
            "All labels should be correct",
        )
        .await;

        let expected = format!(
            r#"kubernetes.io/config.hash="{}"
mycustomannotation="{}""#,
            config_hash, custom_annotation
        );

        assert_content(
            vol_dir.join("all_annotations"),
            &expected,
            "All annotations should be correct",
        )
        .await;

        downward
            .unmount()
            .await
            .expect("Should unmount successfully")
    }

    #[tokio::test]
    async fn test_invalid() {
        let fake_pod = serde_json::json!({
            "kind": "Pod",
            "apiVersion": "v1",
            "metadata": {
                "name": "test-pod",
            },
            "spec": {
                "containers": [
                    {
                        "name": "test-container",
                    }
                ],
                "volumes": [
                    {
                        "name": "podinfo",
                        "downwardAPI": {
                            "items": [
                                {
                                    "path": "test",
                                    "resourceFieldRef": {
                                        "containerName": "test-container",
                                        "resource": "requests.cpu",
                                        "divisor": "1",
                                    }
                                },
                            ]
                        }
                    }
                ]
            }
        });

        let fake_pod: Pod = serde_json::from_value(fake_pod).unwrap();
        let mut vol = fake_pod.volumes()[0].clone();
        let mut downward = DownwardApiVolume::new(&vol, fake_pod.clone())
            .expect("Should be able to create a new DownwardApiVolume");
        // Setup a tempdir where we can mount things at
        let tempdir = tempfile::tempdir().expect("Unable to create tempdir");
        downward
            .mount(tempdir.path())
            .await
            .expect_err("Missing CPU request should fail");

        // This is just changing the path to test memory as well. Yeah, it is 10 lines, but better
        // than 30 lines of copied JSON from above
        vol.downward_api
            .as_mut()
            .unwrap()
            .items
            .get_mut(0)
            .unwrap()
            .resource_field_ref
            .as_mut()
            .unwrap()
            .resource = "requests.memory".to_string();

        let mut downward = DownwardApiVolume::new(&vol, fake_pod.clone())
            .expect("Should be able to create a new DownwardApiVolume");
        downward
            .mount(tempdir.path())
            .await
            .expect_err("Missing memory request should fail");

        // Remove the resource field and request a non-existent key
        {
            // Wrapped in a block to drop the mutable borrow
            let source = vol.downward_api.as_mut().unwrap().items.get_mut(0).unwrap();
            source.resource_field_ref = None;
            source.field_ref = Some(ObjectFieldSelector {
                field_path: "metadata.nonexistent".to_string(),
                ..Default::default()
            });
        }

        let mut downward = DownwardApiVolume::new(&vol, fake_pod.clone())
            .expect("Should be able to create a new DownwardApiVolume");
        downward
            .mount(tempdir.path())
            .await
            .expect_err("Non-existent metadata field should fail");

        // Now try and request something that isn't in metadata
        vol.downward_api
            .as_mut()
            .unwrap()
            .items
            .get_mut(0)
            .unwrap()
            .field_ref
            .as_mut()
            .unwrap()
            .field_path = "spec.status".to_string();

        let mut downward = DownwardApiVolume::new(&vol, fake_pod)
            .expect("Should be able to create a new DownwardApiVolume");
        downward
            .mount(tempdir.path())
            .await
            .expect_err("A valid path outside of metadata should fail");
    }

    async fn assert_content(path: PathBuf, expected: &str, message: &str) {
        let content = tokio::fs::read_to_string(path)
            .await
            .expect("Unable to read file");
        assert_eq!(content, expected, "{}", message);
    }
}
