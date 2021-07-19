//! The kubelet device plugin manager. Consists of a `DeviceRegistry` that hosts a registration
//! service for device plugins, a `DeviceManager` that maintains a device plugin client for each
//! registered device plugin, a `NodePatcher` that patches the Node status with the extended
//! resources advertised by device plugins, and a `PodDevices` that maintains a list of Pods that
//! are actively using allocated resources.
pub mod manager;
pub(crate) mod node_patcher;
pub(crate) mod plugin_connection;
pub(crate) mod pod_devices;
use crate::device_plugin_api::v1beta1::{
    registration_server::{Registration, RegistrationServer},
    Device, Empty, RegisterRequest,
};
use crate::grpc_sock;
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
pub use manager::DeviceManager;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::task;
#[cfg(target_family = "windows")]
use tokio_compat_02::FutureExt;
use tonic::transport::Server;
use tracing::debug;

pub(crate) const PLUGIN_MANGER_SOCKET_NAME: &str = "kubelet.sock";

/// `DeviceIdMap` contains the device Ids of all the devices advertised by device plugins. Key is
/// resource name.
type DeviceIdMap = HashMap<String, PluginDeviceIds>;

/// `PluginDeviceIds` contains the IDs of all the devices advertised by a single device plugin
type PluginDeviceIds = HashSet<String>;

/// `DeviceMap` contains all the devices advertised by all device plugins. Key is resource name.
type DeviceMap = HashMap<String, PluginDevicesMap>;

/// `PluginDevicesMap` contains all of the devices advertised by a single device plugin. Key is
/// device ID.
type PluginDevicesMap = HashMap<String, Device>;

/// Map of resources requested by a Container. Key is resource name and value is requested quantity
/// of the resource
type ContainerResourceRequests = HashMap<String, Quantity>;

/// Map of resources requested by the Containers of a Pod. Key is container name and value is the
/// Container's resource requests
pub type PodResourceRequests = HashMap<String, ContainerResourceRequests>;

/// Healthy means the device is allocatable (whether already allocated or not)
const HEALTHY: &str = "Healthy";

/// Hosts the device plugin `Registration` service (defined in the device plugin API) for a
/// `DeviceManager`. Upon device plugin registration, reaches out to its `DeviceManager` to validate
/// the device plugin and establish a connection with it.
#[derive(Clone)]
pub struct DeviceRegistry {
    device_manager: Arc<DeviceManager>,
}

impl DeviceRegistry {
    /// Returns a new `DeviceRegistry` with a reference to a `DeviceManager` to handle each
    /// registered device plugin.
    pub fn new(device_manager: Arc<DeviceManager>) -> Self {
        DeviceRegistry { device_manager }
    }
}
#[async_trait::async_trait]

impl Registration for DeviceRegistry {
    async fn register(
        &self,
        request: tonic::Request<RegisterRequest>,
    ) -> Result<tonic::Response<Empty>, tonic::Status> {
        let register_request = request.into_inner();
        debug!(resource = %register_request.resource_name, "Register called by device plugin");
        // Validate
        self.device_manager
            .validate(&register_request)
            .await
            .map_err(|e| tonic::Status::new(tonic::Code::InvalidArgument, format!("{}", e)))?;
        // Create a list and watch connection with the device plugin
        self.device_manager
            .create_plugin_connection(register_request)
            .await
            .map_err(|e| tonic::Status::new(tonic::Code::NotFound, format!("{}", e)))?;
        Ok(tonic::Response::new(Empty {}))
    }
}

/// Starts the `DeviceManager` by running its `NodePatcher` and serving the `DeviceRegistry` which
/// hosts the device plugin manager's `Registration` service on the socket specified in the
/// `DeviceManager`. Returns an error if either the `NodePatcher` or `DeviceRegistry` error.
pub async fn serve_device_registry(device_manager: Arc<DeviceManager>) -> anyhow::Result<()> {
    // Create plugin manager if it doesn't exist
    tokio::fs::create_dir_all(&device_manager.plugin_dir).await?;
    let manager_socket = device_manager.plugin_dir.join(PLUGIN_MANGER_SOCKET_NAME);
    debug!(
        "Serving device plugin manager on socket {:?}",
        manager_socket
    );
    // Delete any existing manager socket
    match tokio::fs::remove_file(&manager_socket).await {
        Ok(_) => (),
        Err(e) if matches!(e.kind(), std::io::ErrorKind::NotFound) => (),
        Err(e) => return Err(e.into()),
    }
    let socket = grpc_sock::server::Socket::new(&manager_socket)?;
    let node_status_patcher = device_manager.node_status_patcher.clone();
    let (tx, rx) = tokio::sync::oneshot::channel();
    let node_patcher_task = task::spawn(async move {
        node_status_patcher.listen_and_patch(tx).await.unwrap();
    });
    // Have NodePatcher notify when it has created a receiver to avoid race case of the
    // DeviceManager trying to send device info to the NodeStatusPatcher before the
    // NodeStatusPatcher has created a receiver. Sender would error due to no active receivers.
    rx.await?;
    let device_registry = DeviceRegistry::new(device_manager);
    let device_manager_task = task::spawn(async {
        let serv = Server::builder()
            .add_service(RegistrationServer::new(device_registry))
            .serve_with_incoming(socket);
        #[cfg(target_family = "windows")]
        let serv = serv.compat();
        serv.await.expect("Unable to serve device plugin manager");
    });
    tokio::try_join!(node_patcher_task, device_manager_task).map_err(anyhow::Error::from)?;
    Ok(())
}

#[cfg(test)]
pub mod test_utils {
    use super::*;
    use futures::pin_mut;
    use http::{Request as HttpRequest, Response as HttpResponse};
    use hyper::Body;
    use kube::Client;
    use std::convert::TryFrom;
    use tokio::sync::RwLock;
    use tower_test::mock;

    /// Unhealthy means the device is not allocatable
    pub const UNHEALTHY: &str = "Unhealthy";

    pub fn mock_client() -> kube::Client {
        kube::Client::try_from(kube::Config::new("http://127.0.0.1:8080".parse().unwrap())).unwrap()
    }

    /// Creates a mock kubernetes API service that the NodePatcher calls to when device plugin
    /// resources need to be updated in the Node status. It verifies the request and always returns
    /// a fake Node. Returns a client that will reference this mock service and the task the service
    /// is running on.
    pub async fn create_mock_kube_service(
        node_name: &str,
    ) -> (Client, tokio::task::JoinHandle<()>) {
        // Mock client as inspired by this thread on kube-rs crate:
        // https://github.com/clux/kube-rs/issues/429
        let (mock_service, handle) = mock::pair::<HttpRequest<Body>, HttpResponse<Body>>();
        let node_name = node_name.to_string();
        let spawned = tokio::spawn(async move {
            pin_mut!(handle);
            let (request, send) = handle.next_request().await.expect("service not called");
            assert_eq!(request.method(), http::Method::PATCH);
            assert_eq!(
                request.uri().to_string(),
                format!("/api/v1/nodes/{}/status?", node_name)
            );
            let node: k8s_openapi::api::core::v1::Node =
                serde_json::from_value(serde_json::json!({
                    "apiVersion": "v1",
                    "kind": "Node",
                    "metadata": {
                        "name": "test",
                    }
                }))
                .unwrap();
            send.send_response(
                HttpResponse::builder()
                    .body(Body::from(serde_json::to_vec(&node).unwrap()))
                    .unwrap(),
            );
        });
        let client = Client::new(mock_service, "default");
        (client, spawned)
    }

    pub fn create_mock_healthy_devices(r1_name: &str, r2_name: &str) -> Arc<RwLock<DeviceMap>> {
        let r1_devices: PluginDevicesMap = (0..3)
            .map(|x| Device {
                id: format!("{}-id{}", r1_name, x),
                health: HEALTHY.to_string(),
                topology: None,
            })
            .map(|d| (d.id.clone(), d))
            .collect();

        let r2_devices: PluginDevicesMap = (0..2)
            .map(|x| Device {
                id: format!("{}-id{}", r2_name, x),
                health: HEALTHY.to_string(),
                topology: None,
            })
            .map(|d| (d.id.clone(), d))
            .collect();

        let device_map: DeviceMap = [
            (r1_name.to_string(), r1_devices),
            (r2_name.to_string(), r2_devices),
        ]
        .iter()
        .cloned()
        .collect();

        Arc::new(RwLock::new(device_map))
    }
}
