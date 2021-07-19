//! `resources` contains utilities and managers for container resources.

pub(crate) mod device_plugin_manager;
pub(crate) mod quantity;

pub use device_plugin_manager::manager::DeviceManager;
pub mod util;
