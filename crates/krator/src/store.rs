//! Defines Store type for caching Kubernetes objects locally.

use crate::object::ObjectKey;
use kube::api::GroupVersionKind;
use std::any::Any;
use std::collections::HashMap;
use tokio::sync::RwLock;

type ResourceMap = HashMap<GroupVersionKind, HashMap<ObjectKey, Box<dyn Any + Send + Sync>>>;

/// Stores or caches arbitrary Kubernetes objects.
pub struct Store {
    objects: RwLock<ResourceMap>,
}

impl Default for Store {
    fn default() -> Self {
        Store::new()
    }
}

impl Store {
    /// Initialize empty store.
    pub fn new() -> Self {
        Store {
            objects: RwLock::new(HashMap::new()),
        }
    }

    /// Insert a new object.
    pub async fn insert<R: 'static + k8s_openapi::Resource + Sync + Send>(
        &self,
        namespace: Option<String>,
        name: String,
        object: R,
    ) {
        let mut objects = self.objects.write().await;
        let key = GroupVersionKind::gvk(R::GROUP, R::VERSION, R::KIND).unwrap();
        let resource_objects = (*objects).entry(key).or_insert_with(HashMap::new);
        let object_key = ObjectKey::new(namespace, name);
        resource_objects.insert(object_key, Box::new(object));
    }

    /// Clear cache for specified object kind.
    pub async fn reset(&self, gvk: &GroupVersionKind) {
        let mut objects = self.objects.write().await;
        let key = gvk.clone();
        let resource_objects = (*objects).entry(key).or_insert_with(HashMap::new);
        resource_objects.clear();
    }

    /// Delete a cached object.
    pub async fn delete_any(
        &self,
        namespace: Option<String>,
        name: String,
        gvk: &GroupVersionKind,
    ) {
        let mut objects = self.objects.write().await;
        let key = gvk.clone();
        let resource_objects = (*objects).entry(key).or_insert_with(HashMap::new);
        let object_key = ObjectKey::new(namespace, name);
        resource_objects.remove(&object_key);
    }

    /// Insert an object that has already been type erased.
    pub async fn insert_any(
        &self,
        namespace: Option<String>,
        name: String,
        gvk: &GroupVersionKind,
        object: Box<dyn Any + Send + Sync>,
    ) {
        let mut objects = self.objects.write().await;
        let key = gvk.clone();
        let resource_objects = (*objects).entry(key).or_insert_with(HashMap::new);
        let object_key = ObjectKey::new(namespace, name);
        resource_objects.insert(object_key, object);
    }

    /// Fetch an object.
    pub async fn get<R: 'static + k8s_openapi::Resource + Clone>(
        &self,
        namespace: Option<String>,
        name: String,
    ) -> anyhow::Result<Option<R>> {
        let objects = self.objects.read().await;
        let key = GroupVersionKind::gvk(R::GROUP, R::VERSION, R::KIND).unwrap();
        let object_key = ObjectKey::new(namespace, name);
        match (*objects).get(&key) {
            Some(resource_objects) => match resource_objects.get(&object_key) {
                Some(any_object) => match any_object.downcast_ref::<R>() {
                    Some(object) => Ok(Some(object.clone())),
                    None => {
                        anyhow::bail!(
                            "Could not interpret interred object as type {}/{} {}",
                            R::GROUP,
                            R::VERSION,
                            R::KIND
                        );
                    }
                },
                None => Ok(None),
            },
            // TODO: Should this be an error since we probably arent tracking that resource type?
            None => Ok(None),
        }
    }
}
