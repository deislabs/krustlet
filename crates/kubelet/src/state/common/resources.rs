//! Resources can be successfully allocated to the Pod.
use crate::pod::state::prelude::*;
use crate::provider::DevicePluginSupport;
use crate::resources::device_plugin_manager::PodResourceRequests;
use crate::resources::util;
use crate::volume::{HostPathVolume, VolumeRef};
use k8s_openapi::api::core::v1::HostPathVolumeSource;
use k8s_openapi::api::core::v1::Volume as KubeVolume;
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use std::collections::HashMap;
use tracing::{debug, error, info};

use super::error::Error;
use super::image_pull::ImagePull;
use super::{GenericPodState, GenericProvider};

/// Resources can be successfully allocated to the Pod
pub struct Resources<P: GenericProvider> {
    phantom: std::marker::PhantomData<P>,
}

impl<P: GenericProvider> std::fmt::Debug for Resources<P> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        "Resources".fmt(formatter)
    }
}

impl<P: GenericProvider> Default for Resources<P> {
    fn default() -> Self {
        Self {
            phantom: std::marker::PhantomData,
        }
    }
}

#[async_trait::async_trait]
impl<P: GenericProvider> State<P::PodState> for Resources<P> {
    async fn next(
        self: Box<Self>,
        provider_state: SharedState<P::ProviderState>,
        pod_state: &mut P::PodState,
        pod: Manifest<Pod>,
    ) -> Transition<P::PodState> {
        let pod = pod.latest();
        debug!(pod = %pod.name(), "Preparing to allocate resources for this pod");
        let device_plugin_manager = provider_state.read().await.device_plugin_manager();

        // Only check for allocatable resources if a device plugin manager was provided.
        if let Some(device_plugin_manager) = device_plugin_manager {
            // Create a map of devices requested by this Pod's containers, keyed by container name
            let mut container_devices: PodResourceRequests = HashMap::new();
            for container in pod.all_containers() {
                if let Some(resources) = container.resources() {
                    let extended_resources: HashMap<String, Quantity> = resources
                        .requests
                        .iter()
                        .filter(|(resource_name, _)| util::is_extended_resource_name(resource_name))
                        .map(|(k, v)| (k.clone(), v.clone()))
                        .collect();
                    container_devices.insert(container.name().to_string(), extended_resources);
                }
            }
            // Do allocate for this Pod
            if let Err(e) = device_plugin_manager
                .do_allocate(&pod.pod_uid(), container_devices)
                .await
            {
                error!(error = %e);
                let next = Error::<P>::new(e.to_string());
                return Transition::next(self, next);
            }

            // In Pod, set env vars and set HostPath volumes for each `ContainerAllocateResponse`.
            // TODO: add support for setting container path mounts, env vars, annotations, and
            // device specs (with permissions) specified in the device plugins'
            // `ContainerAllocateResponse`s.
            if let Some(container_allocate_responses) =
                device_plugin_manager.get_pod_allocate_responses(pod.pod_uid())
            {
                let mut host_paths: Vec<String> = Vec::new();
                let mut env_vars: HashMap<String, HashMap<String, String>> = HashMap::new();
                // Get host paths, env vars, and annotations from allocate responses.
                container_allocate_responses.iter().for_each(|(c, rs)| {
                    rs.iter().for_each(|r| {
                        env_vars.insert(c.clone(), r.envs.clone());
                        r.mounts
                            .iter()
                            .for_each(|m| host_paths.push(m.host_path.clone()))
                    });
                });
                let volumes: HashMap<String, VolumeRef> = host_paths
                    .into_iter()
                    .map(|p| HostPathVolumeSource {
                        path: p,
                        ..Default::default()
                    })
                    .map(|h| KubeVolume {
                        name: h.path.clone(),
                        host_path: Some(h),
                        ..Default::default()
                    })
                    .map(|k| {
                        (
                            k.name.clone(),
                            VolumeRef::HostPath(HostPathVolume::new(&k).unwrap()),
                        )
                    })
                    .collect();
                pod_state.set_volumes(volumes).await;
                pod_state.set_env_vars(env_vars).await;
            }

            info!("Resources allocated to Pod: {}", pod.name());
        }

        let next = ImagePull::<P>::default();
        Transition::next(self, next)
    }

    async fn status(&self, _pod_state: &mut P::PodState, _pod: &Pod) -> anyhow::Result<PodStatus> {
        Ok(make_status(Phase::Pending, "Resources"))
    }
}

impl<P: GenericProvider> TransitionTo<Error<P>> for Resources<P> {}
impl<P: GenericProvider> TransitionTo<ImagePull<P>> for Resources<P> {}
