//! A crate for building custom Kubernetes [kubelets](https://kubernetes.io/docs/reference/command-line-tools-reference/kubelet/).
//!
//! The crate provides the [`Provider`] trait for declaring a Kubelet backend
//! as well as a the [`Kubelet`] type which takes a [`Provider`] and runs
//! a Kubelet server.
//!
//! # Example
//! ```rust,no_run
//! use kubelet::Kubelet;
//! use kubelet::config::Config;
//! use kubelet::pod::Pod;
//! use kubelet::provider::Provider;
//! use kubelet::state::{SharedState, AsyncDrop};
//! use kubelet::pod::state::prelude::*;
//! use kubelet::pod::state::Stub;
//!
//! // Create some type that will act as your provider
//! struct MyProvider;
//!
//! // Track shared provider-level state across pods.
//! struct ProviderState;
//! // Track pod state amongst pod state handlers.
//! struct PodState;
//!
//! impl ResourceState for PodState {
//!     type Manifest = Pod;
//!     type Status = PodStatus;
//! }
//!
//! #[async_trait::async_trait]
//! impl AsyncDrop for PodState {
//!     type ProviderState = ProviderState;
//!     async fn async_drop(self, _provider_state: &mut ProviderState) {}
//! }
//!
//! // Implement the `Provider` trait for that type
//! #[async_trait::async_trait]
//! impl Provider for MyProvider {
//!     const ARCH: &'static str = "my-arch";
//!     type InitialState = Stub;
//!     type TerminatedState = Stub;
//!     type ProviderState = ProviderState;
//!     type PodState = PodState;
//!
//!     fn provider_state(&self) -> SharedState<ProviderState> {
//!         SharedState::new(ProviderState {})
//!     }
//!    
//!     async fn initialize_pod_state(&self, _pod: &Pod) -> anyhow::Result<Self::PodState> {
//!         Ok(PodState)
//!     }
//!
//!     async fn logs(&self, namespace: String, pod: String, container: String, sender: kubelet::log::Sender) -> anyhow::Result<()> { todo!() }
//! }
//!
//! async {
//!     // Instantiate your provider type
//!     let provider = MyProvider;
//!
//!     // Load a kubernetes configuration
//!     let kubeconfig = kube::Config::infer().await.unwrap();
//!     // Get a configuration for the Kubelet
//!     let kubelet_config = Config::default();
//!
//!     // Instantiate the Kubelet
//!     let kubelet = Kubelet::new(provider, kubeconfig, kubelet_config).await.unwrap();
//!     // Start the Kubelet and block on it
//!     kubelet.start().await.unwrap();
//! };
//! ```

#![deny(missing_docs)]
#![cfg_attr(feature = "docs", feature(doc_cfg))]

mod bootstrapping;
mod config_interpreter;
mod kubelet;

pub(crate) mod kubeconfig;
pub(crate) mod webserver;
pub(crate) mod plugin_registration_api {
    pub(crate) mod v1 {
        pub const API_VERSION: &str = "v1";

        tonic::include_proto!("pluginregistration.v1");
    }
}
pub(crate) mod fs_watch;
pub(crate) mod grpc_sock;
#[cfg(target_family = "windows")]
#[allow(dead_code)]
pub(crate) mod mio_uds_windows;
pub(crate) mod plugin_watcher;

pub mod backoff;
pub mod config;
pub mod container;
pub mod handle;
pub mod log;
pub mod node;
pub mod pod;
pub mod provider;
pub mod secret;
pub mod state;
pub mod store;
pub mod volume;

pub use self::kubelet::Kubelet;
pub use bootstrapping::bootstrap;

#[cfg(feature = "derive")]
#[allow(unused_imports)]
#[macro_use]
// Note that this crate is re-exported within `state` for now as it only has to do with that
extern crate kubelet_derive;
