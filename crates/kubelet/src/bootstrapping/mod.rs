use std::{convert::TryFrom, env, path::Path, str};

use futures::{StreamExt, TryStreamExt};
use k8s_openapi::api::certificates::v1beta1::CertificateSigningRequest;
use kube::api::{Api, ListParams, PostParams};
use kube::config::Kubeconfig;
use kube::Config;
use kube_runtime::watcher::{watcher, Event};
use rcgen::{
    Certificate, CertificateParams, DistinguishedName, DnType, KeyPair, SanType,
    PKCS_ECDSA_P256_SHA256,
};
use tokio::fs::{read, write};
use tracing::{debug, info, instrument, trace};

use crate::config::Config as KubeletConfig;
use crate::kubeconfig::exists as kubeconfig_exists;
use crate::kubeconfig::KUBECONFIG;

const APPROVED_TYPE: &str = "Approved";

/// Bootstrap the cluster with TLS certificates but only if no existing kubeconfig can be found.
pub async fn bootstrap<K: AsRef<Path>>(
    config: &KubeletConfig,
    bootstrap_file: K,
    notify: impl Fn(String),
) -> anyhow::Result<Config> {
    debug!(%config.node_name, "Starting bootstrap");
    let kubeconfig = bootstrap_auth(config, bootstrap_file).await?;
    bootstrap_tls(config, kubeconfig.clone(), notify).await?;
    Ok(kubeconfig)
}

#[instrument(level = "info", skip(config, bootstrap_file))]
async fn bootstrap_auth<K: AsRef<Path>>(
    config: &KubeletConfig,
    bootstrap_file: K,
) -> anyhow::Result<Config> {
    if kubeconfig_exists() {
        debug!("Found existing kubeconfig, loading...");
        Config::infer()
            .await
            .map_err(|e| anyhow::anyhow!("Unable to load config from host: {}", e))
    } else {
        // TODO: if configured, kubelet automatically requests renewal of the certificate when it is close to expiry
        let original_kubeconfig = std::path::PathBuf::from(env::var(KUBECONFIG)?);
        debug!(
            bootstrap_file = %bootstrap_file.as_ref().display(),
            "No existing kubeconfig found, loading bootstrap config"
        );
        env::set_var(KUBECONFIG, bootstrap_file.as_ref().as_os_str());
        let conf = kube::Config::infer().await?;
        let client = kube::Client::try_from(conf)?;

        trace!("Generating auth certificate");
        let cert_bundle = gen_auth_cert(config)?;
        trace!("Getting cluster information from bootstrap config");
        let bootstrap_config = read_from(&bootstrap_file).await?;
        let named_cluster = bootstrap_config
            .clusters
            .into_iter()
            .next()
            .ok_or_else(|| {
                anyhow::anyhow!("Unable to find cluster information in bootstrap config")
            })?;
        let server = named_cluster.cluster.server;
        trace!(%server, "Identified server information from bootstrap config");
        let ca_data = named_cluster
            .cluster
            .certificate_authority_data
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Unable to find certificate authority information in bootstrap config"
                )
            })?;
        trace!(csr_name = %config.node_name, "Generating and sending CSR to Kubernetes API");
        let csrs: Api<CertificateSigningRequest> = Api::all(client);
        let csr_json = serde_json::json!({
          "apiVersion": "certificates.k8s.io/v1beta1",
          "kind": "CertificateSigningRequest",
          "metadata": {
            "name": config.node_name,
          },
          "spec": {
            "request": base64::encode(cert_bundle.serialize_request_pem()?.as_bytes()),
            "signerName": "kubernetes.io/kube-apiserver-client-kubelet",
            "usages": [
              "digital signature",
              "key encipherment",
              "client auth"
            ]
          }
        });

        let post_data = serde_json::from_value(csr_json)
            .expect("Invalid CSR JSON, this is a programming error");

        csrs.create(&PostParams::default(), &post_data).await?;

        trace!("CSR creation successful, waiting for certificate approval");

        // Wait for CSR signing
        let inf = watcher(
            csrs,
            ListParams::default().fields(&format!("metadata.name={}", config.node_name)),
        );

        let mut watcher = inf.boxed();
        let mut generated_kubeconfig = Vec::new();
        let mut got_cert = false;
        let start = std::time::Instant::now();
        while let Some(event) = watcher.try_next().await? {
            trace!(?event, "Got event from watcher");
            let status = match event {
                Event::Applied(m) => m.status.unwrap(),
                Event::Restarted(mut certs) => {
                    // We should only ever get one cert for this node, so error in any circumstance we don't
                    if certs.len() > 1 {
                        return Err(anyhow::anyhow!("On watch restart, got more than 1 authentication CSR. This means something is in an incorrect state"));
                    }
                    certs.remove(0).status.unwrap()
                }
                Event::Deleted(_) => {
                    return Err(anyhow::anyhow!(
                        "Authentication CSR was deleted before it was approved"
                    ))
                }
            };

            if let Some(cert) = status.certificate {
                if status
                    .conditions
                    .into_iter()
                    .any(|c| c.type_.as_str() == APPROVED_TYPE)
                {
                    debug!("Certificate has been approved, generating kubeconfig");
                    generated_kubeconfig = gen_kubeconfig(
                        ca_data,
                        server,
                        cert,
                        cert_bundle.serialize_private_key_pem(),
                    )?;
                    got_cert = true;
                    break;
                }
            }

            info!(elapsed = ?start.elapsed(), "Got modified event, but CSR for authentication certs is not currently approved");
        }

        if !got_cert {
            return Err(anyhow::anyhow!(
                "Authentication certificates were never approved"
            ));
        }

        // Make sure the directory where the certs should live exists
        trace!("Ensuring desired kubeconfig directory exists");
        if let Some(p) = original_kubeconfig.parent() {
            tokio::fs::create_dir_all(p).await?;
        }

        debug!(path = %original_kubeconfig.display(), "Writing generated kubeconfig to file");
        write(&original_kubeconfig, &generated_kubeconfig).await?;
        // Set environment variable back to original value
        // so that infer will now pick up the file we generated
        env::set_var(KUBECONFIG, original_kubeconfig.as_os_str());

        Config::infer()
            .await
            .map_err(|e| anyhow::anyhow!("Unable to load generated config: {}", e))
    }
}

#[instrument(level = "info", skip(config, kubeconfig, notify))]
async fn bootstrap_tls(
    config: &KubeletConfig,
    kubeconfig: Config,
    notify: impl Fn(String),
) -> anyhow::Result<()> {
    debug!("Starting bootstrap of TLS serving certs");
    if config.server_config.cert_file.exists() {
        return Ok(());
    }

    trace!("Generating TLS certificate");
    let cert_bundle = gen_tls_cert(config)?;

    let csr_name = format!("{}-tls", config.hostname);
    trace!(%csr_name, "Generating and sending CSR to Kubernetes API");
    let client = kube::Client::try_from(kubeconfig)?;
    let csrs: Api<CertificateSigningRequest> = Api::all(client);
    let csr_json = serde_json::json!({
        "apiVersion": "certificates.k8s.io/v1beta1",
        "kind": "CertificateSigningRequest",
        "metadata": {
            "name": csr_name,
        },
        "spec": {
        "request": base64::encode(cert_bundle.serialize_request_pem()?.as_bytes()),
        "signerName": "kubernetes.io/kubelet-serving",
        "usages": [
            "digital signature",
            "key encipherment",
            "server auth"
        ]
        }
    });

    let post_data =
        serde_json::from_value(csr_json).expect("Invalid CSR JSON, this is a programming error");

    csrs.create(&PostParams::default(), &post_data).await?;

    trace!("CSR creation successful, sending notification and waiting for certificate approval");

    notify(awaiting_user_csr_approval("TLS", &csr_name));

    // Wait for CSR signing
    let inf = watcher(
        csrs,
        ListParams::default().fields(&format!("metadata.name={}", csr_name)),
    );

    let mut watcher = inf.boxed();
    let mut certificate = String::new();
    let mut got_cert = false;
    let start = std::time::Instant::now();
    while let Some(event) = watcher.try_next().await? {
        trace!(?event, "Got event from watcher");
        let status = match event {
            Event::Applied(m) => m.status.unwrap(),
            Event::Restarted(mut certs) => {
                // We should only ever get one cert for this node, so error in any circumstance we don't
                if certs.len() > 1 {
                    return Err(anyhow::anyhow!("On watch restart, got more than 1 serving CSR. This means something is in an incorrect state"));
                }
                certs.remove(0).status.unwrap()
            }
            Event::Deleted(_) => {
                return Err(anyhow::anyhow!(
                    "Serving CSR was deleted before it was approved"
                ))
            }
        };

        if let Some(cert) = status.certificate {
            if status
                .conditions
                .into_iter()
                .any(|c| c.type_.as_str() == APPROVED_TYPE)
            {
                debug!("Certificate has been approved, extracting cert from response");
                certificate = std::str::from_utf8(&cert.0)?.to_owned();
                got_cert = true;
                break;
            }
        }
        info!(remaining = ?start.elapsed(), "Got modified event, but CSR for serving certs is not currently approved");
    }

    if !got_cert {
        return Err(anyhow::anyhow!(
            "Authentication certificates were never approved"
        ));
    }

    let private_key = cert_bundle.serialize_private_key_pem();
    debug!(
        cert_file = %config.server_config.cert_file.display(),
        private_key_file = %config.server_config.private_key_file.display(),
        "Got certificate from API, writing cert and private key to disk"
    );
    // Make sure the directory where the certs should live exists
    if let Some(p) = config.server_config.cert_file.parent() {
        tokio::fs::create_dir_all(p).await?;
    }
    write(&config.server_config.cert_file, &certificate).await?;
    write(&config.server_config.private_key_file, &private_key).await?;

    notify(completed_csr_approval("TLS"));

    Ok(())
}

fn awaiting_user_csr_approval(cert_description: &str, csr_name: &str) -> String {
    format!(
        "{} certificate requires manual approval. Run kubectl certificate approve {}",
        cert_description, csr_name
    )
}

fn completed_csr_approval(cert_description: &str) -> String {
    format!(
        "received {} certificate approval: continuing",
        cert_description
    )
}

// Known false positive for non_exhaustive struct `CertificateParams`
// https://github.com/rust-lang/rust-clippy/issues/6559
#[allow(clippy::field_reassign_with_default)]
fn gen_auth_cert(config: &KubeletConfig) -> anyhow::Result<Certificate> {
    let mut params = CertificateParams::default();
    params.not_before = chrono::Utc::now();
    params.not_after = chrono::Utc::now() + chrono::Duration::weeks(52);
    let mut distinguished_name = DistinguishedName::new();
    distinguished_name.push(DnType::OrganizationName, "system:nodes");
    distinguished_name.push(
        DnType::CommonName,
        &format!("system:node:{}", config.node_name),
    );
    params.distinguished_name = distinguished_name;
    params
        .key_pair
        .replace(KeyPair::generate(&PKCS_ECDSA_P256_SHA256)?);

    params.alg = &PKCS_ECDSA_P256_SHA256;

    Ok(Certificate::from_params(params)?)
}

// Known false positive for non_exhaustive struct `CertificateParams`
// https://github.com/rust-lang/rust-clippy/issues/6559
#[allow(clippy::field_reassign_with_default)]
fn gen_tls_cert(config: &KubeletConfig) -> anyhow::Result<Certificate> {
    let mut params = CertificateParams::default();
    params.not_before = chrono::Utc::now();
    params.not_after = chrono::Utc::now() + chrono::Duration::weeks(52);
    let mut distinguished_name = DistinguishedName::new();
    distinguished_name.push(DnType::OrganizationName, "system:nodes");
    distinguished_name.push(
        DnType::CommonName,
        &format!("system:node:{}", config.hostname),
    );
    params.distinguished_name = distinguished_name;
    params
        .key_pair
        .replace(KeyPair::generate(&PKCS_ECDSA_P256_SHA256)?);

    params.alg = &PKCS_ECDSA_P256_SHA256;

    params.subject_alt_names = vec![
        SanType::DnsName(config.hostname.clone()),
        SanType::IpAddress(config.node_ip),
    ];

    Ok(Certificate::from_params(params)?)
}

fn gen_kubeconfig(
    ca_data: String,
    server: String,
    client_cert_data: k8s_openapi::ByteString,
    client_key: String,
) -> anyhow::Result<Vec<u8>> {
    let json = serde_json::json!({
        "kind": "Config",
        "apiVersion": "v1",
        "preferences": {},
        "clusters": [{
            "name": "krustlet",
            "cluster": {
                "certificate-authority-data": ca_data,
                "server": server,
            }
        }],
        "users":[{
            "name": "krustlet",
            "user": {
                "client-certificate-data": client_cert_data,
                "client-key-data": base64::encode(client_key.as_bytes())
            }
        }],
        "contexts": [{
            "name": "krustlet",
            "context": {
                "cluster": "krustlet",
                "user": "krustlet",
            }
        }],
        "current-context": "krustlet"
    });

    serde_json::to_vec(&json)
        .map_err(|e| anyhow::anyhow!("Unable to serialize generated kubeconfig: {}", e))
}

async fn read_from<P: AsRef<Path>>(path: P) -> anyhow::Result<Kubeconfig> {
    // Serde yaml doesn't have async support so we have to read the whole file in
    let raw = read(path)
        .await
        .map_err(|e| anyhow::anyhow!(format!("Error loading bootstrap file: {}", e)))?;
    let config = serde_yaml::from_slice(&raw)
        .map_err(|e| anyhow::anyhow!(format!("Error parsing bootstrap file: {}", e)))?;

    Ok(config)
}
