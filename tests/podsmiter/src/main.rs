use std::fmt::Display;

use k8s_openapi::api::{
    core::v1::{Namespace, Pod},
    storage::v1::StorageClass,
};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use k8s_openapi::Metadata;
use kube::api::{Api, DeleteParams, ListParams};

const E2E_NS_PREFIXES: &[&str] = &["wasi-e2e"];

#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    let result = smite_all_integration_test_resources().await;

    match &result {
        Ok(message) => println!("{}", message),
        Err(e) => println!("{}", e),
    };

    result.map(|_| ())
}

async fn smite_all_integration_test_resources() -> anyhow::Result<&'static str> {
    let client = match kube::Client::try_default().await {
        Ok(c) => c,
        Err(e) => {
            return Err(anyhow::anyhow!(
                "Failed to acquire Kubernetes client: {}",
                e
            ))
        }
    };

    let namespaces = list_e2e_namespaces(client.clone()).await?;
    let storageclasses = list_storageclasses(client.clone()).await?;

    if namespaces.is_empty() && storageclasses.is_empty() {
        return Ok("No e2e namespaces or StorageClasses found");
    }
    if !confirm_smite(&namespaces, &storageclasses) {
        return Ok("Operation cancelled");
    }

    let pod_smite_operations = namespaces
        .iter()
        .map(|ns| smite_namespace_pods(client.clone(), ns));
    let pod_smite_results = futures::future::join_all(pod_smite_operations).await;
    let (_, pod_smite_errors) = pod_smite_results.partition_success();

    if !pod_smite_errors.is_empty() {
        return Err(smite_failure_error(&pod_smite_errors));
    }

    println!("Requested force-delete of all pods; requesting delete of namespaces...");

    Smiter::new(namespaces, None, DeleteParams::default())
        .smite::<Namespace>(client.clone())
        .await?;

    println!("Requesting delete of storage classes...");
    Smiter::new(storageclasses, None, DeleteParams::default())
        .smite::<StorageClass>(client)
        .await?;

    Ok("All e2e resources force-deleted; namespace cleanup may take a couple of minutes")
}

async fn list_e2e_namespaces(client: kube::Client) -> anyhow::Result<Vec<String>> {
    println!("Finding e2e namespaces...");

    let nsapi: Api<Namespace> = Api::all(client.clone());
    let nslist = nsapi.list(&ListParams::default()).await?;

    Ok(nslist
        .iter()
        .map(name_of)
        .filter(|s| is_e2e_resource(s.as_str()))
        .collect())
}

fn name_of(ns: &impl Metadata<Ty = ObjectMeta>) -> String {
    ns.metadata().name.as_ref().unwrap().to_owned()
}

fn is_e2e_resource(item: &str) -> bool {
    E2E_NS_PREFIXES
        .iter()
        .any(|prefix| item.starts_with(prefix))
}

async fn smite_namespace_pods(client: kube::Client, namespace: &str) -> anyhow::Result<()> {
    println!("Finding pods in namespace {}...", namespace);

    let podapi: Api<Pod> = Api::namespaced(client.clone(), namespace);
    let pods = podapi.list(&ListParams::default()).await?;

    let names_to_delete = pods
        .into_iter()
        .map(|p| p.metadata.name.unwrap_or_default())
        .collect();

    println!("Deleting pods in namespace {}...", namespace);

    let smiter = Smiter::new(
        names_to_delete,
        Some(namespace.to_owned()),
        DeleteParams {
            grace_period_seconds: Some(0),
            ..DeleteParams::default()
        },
    );

    smiter.smite::<Pod>(client).await
}

fn smite_failure_error<T: Display>(errors: &[T]) -> anyhow::Error {
    let message_list = errors
        .iter()
        .map(|e| format!("{}", e))
        .collect::<Vec<_>>()
        .join("\n");
    anyhow::anyhow!(
        "Some integration test resources were not cleaned up:\n{}",
        message_list
    )
}

fn confirm_smite(namespaces: &[String], storageclasses: &[String]) -> bool {
    println!(
        "Smite these resources and namespaces (all resources within it)?:\nNamespaces: {}\nStorageClasses: {} (y/n) ",
        namespaces.join(", "),
        storageclasses.join(", "),
    );
    let mut response = String::new();
    match std::io::stdin().read_line(&mut response) {
        Err(e) => {
            eprintln!("Error reading response: {}", e);
            confirm_smite(namespaces, storageclasses)
        }
        Ok(_) => response.starts_with('y') || response.starts_with('Y'),
    }
}

async fn list_storageclasses(client: kube::Client) -> anyhow::Result<Vec<String>> {
    let sc: Api<StorageClass> = Api::all(client);
    let all = sc.list(&ListParams::default()).await?;
    Ok(all
        .items
        .into_iter()
        .map(|class| class.metadata.name.unwrap_or_default())
        .filter(|s| is_e2e_resource(s.as_str()))
        .collect())
}

struct Smiter {
    names_to_smite: Vec<String>,
    namespace: Option<String>,
    params: DeleteParams,
}

impl Smiter {
    fn new(names_to_smite: Vec<String>, namespace: Option<String>, params: DeleteParams) -> Self {
        Smiter {
            names_to_smite,
            namespace,
            params,
        }
    }

    async fn smite<T>(self, client: kube::Client) -> anyhow::Result<()>
    where
        T: kube::Resource + Clone + serde::de::DeserializeOwned + std::fmt::Debug,
        <T as kube::Resource>::DynamicType: Default,
    {
        let api: Api<T> = match self.namespace.as_ref() {
            Some(ns) => Api::namespaced(client, &ns),
            None => Api::all(client),
        };
        let smite_operations = self
            .names_to_smite
            .iter()
            .map(|name| (name, api.clone(), self.params.clone()))
            .map(|(name, api, params)| async move {
                api.delete(&name, &params).await?;
                Ok::<_, kube::Error>(())
            });
        let smite_results = futures::future::join_all(smite_operations).await;
        let (_, smite_errors) = smite_results.partition_success();

        if !smite_errors.is_empty() {
            return Err(smite_failure_error(&smite_errors));
        }
        Ok(())
    }
}

// TODO: deduplicate with oneclick

trait ResultSequence {
    type SuccessItem;
    type FailureItem;
    fn partition_success(self) -> (Vec<Self::SuccessItem>, Vec<Self::FailureItem>);
}

impl<T, E: std::fmt::Debug> ResultSequence for Vec<Result<T, E>> {
    type SuccessItem = T;
    type FailureItem = E;
    fn partition_success(self) -> (Vec<Self::SuccessItem>, Vec<Self::FailureItem>) {
        let (success_results, error_results): (Vec<_>, Vec<_>) =
            self.into_iter().partition(|r| r.is_ok());
        let success_values = success_results.into_iter().map(|r| r.unwrap()).collect();
        let error_values = error_results
            .into_iter()
            .map(|r| r.err().unwrap())
            .collect();
        (success_values, error_values)
    }
}
