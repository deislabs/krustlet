//! Resolves image pull secrets

use k8s_openapi::api::core::v1::Secret;
use kube::api::Api;
use oci_distribution::secrets::RegistryAuth;

/// Resolves registry authentication from image pull secrets
pub struct RegistryAuthResolver {
    kube_client: kube::Client,
    pod_namespace: String,
    image_pull_secret_names: Vec<String>,
}

impl RegistryAuthResolver {
    /// Creates a resolver for the given pod
    pub fn new(client: kube::Client, pod: &crate::pod::Pod) -> Self {
        // TODO: is it safe to capture this stuff or might we need to re-resolve e.g.
        // the list of secret names after a pod modify?
        RegistryAuthResolver {
            kube_client: client,
            pod_namespace: pod.namespace().to_owned(),
            image_pull_secret_names: pod.image_pull_secrets(),
        }
    }

    /// Get the registry authentication method appropriate to the given image reference
    pub async fn resolve_registry_auth(
        &self,
        reference: &oci_distribution::Reference,
    ) -> anyhow::Result<RegistryAuth> {
        let secrets_api: Api<Secret> =
            Api::namespaced(self.kube_client.clone(), &self.pod_namespace);

        let secret_futures: Vec<_> = self
            .image_pull_secret_names
            .iter()
            .map(|name| secrets_api.get(name))
            .collect();
        let secret_results = futures::future::join_all(secret_futures).await;

        for secret_result in secret_results {
            match secret_result {
                Err(e) => return Err(e.into()),
                Ok(secret) => {
                    if let Some(auth) = parse_auth(&secret, reference.registry()) {
                        return Ok(auth);
                    }
                }
            }
        }

        Ok(RegistryAuth::Anonymous)
    }
}

fn parse_auth(secret: &Secret, registry_name: &str) -> Option<RegistryAuth> {
    if secret.data.is_empty() {
        None
    } else {
        parse_auth_from_secret_data(&secret.data, registry_name)
    }
}

fn parse_auth_from_secret_data(
    secret_data: &std::collections::BTreeMap<String, k8s_openapi::ByteString>,
    registry_name: &str,
) -> Option<RegistryAuth> {
    secret_data
        .values()
        .find_map(|v| parse_auth_from_secret_value(v, registry_name))
}

fn parse_auth_from_secret_value(
    secret_value: &k8s_openapi::ByteString,
    registry_name: &str,
) -> Option<RegistryAuth> {
    // We are intereted in secret_value if it is of the form
    // {
    //   "auths": {
    //     "reg1": { ... },
    //     "reg2": { ... }
    //   }
    // }
    parse_byte_string_json(secret_value)
        .and_then(|value| parse_auth_from_json_value(&value, registry_name))
}

fn parse_byte_string_json(byte_string: &k8s_openapi::ByteString) -> Option<serde_json::Value> {
    serde_json::from_slice(&byte_string.0).ok()
}

fn parse_auth_from_json_value(
    json_value: &serde_json::Value,
    registry_name: &str,
) -> Option<RegistryAuth> {
    json_value
        .get("auths")
        .and_then(|auths| auths.get(registry_name))
        .and_then(|creds| parse_auth_from_json_creds(creds))
}

fn parse_auth_from_json_creds(json_creds: &serde_json::Value) -> Option<RegistryAuth> {
    let username = json_creds.get("username");
    let password = json_creds.get("password");
    // TODO: my test creds also included an entry "auth" - should we return this? (e.g. bearer auth?)
    match (username, password) {
        (Some(serde_json::Value::String(u)), Some(serde_json::Value::String(p))) => {
            Some(RegistryAuth::Basic(u.to_owned(), p.to_owned()))
        }
        _ => None,
    }
}
