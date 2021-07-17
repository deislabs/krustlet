//! Errors related to interacting with an OCI compliant remote store

/// The OCI specification defines a specific error format.
///
/// This struct represents that error format, which is formally described here:
/// https://github.com/opencontainers/distribution-spec/blob/master/spec.md#errors-2
#[derive(serde::Deserialize, Debug)]
pub struct OciError {
    /// The error code
    pub code: OciErrorCode,
    /// An optional message associated with the error
    #[serde(default)]
    pub message: String,
    /// Unstructured optional data associated with the error
    #[serde(default)]
    pub detail: serde_json::Value,
}

impl std::error::Error for OciError {
    fn description(&self) -> &str {
        self.message.as_str()
    }
}
impl std::fmt::Display for OciError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "OCI API error: {}", self.message.as_str())
    }
}

#[derive(serde::Deserialize)]
pub(crate) struct OciEnvelope {
    pub(crate) errors: Vec<OciError>,
}

/// OCI error codes
///
/// Outlined here: https://github.com/opencontainers/distribution-spec/blob/master/spec.md#errors-2
#[derive(serde::Deserialize, Debug, PartialEq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum OciErrorCode {
    /// Blob unknown to registry
    ///
    /// This error MAY be returned when a blob is unknown to the registry in a specified
    /// repository. This can be returned with a standard get or if a manifest
    /// references an unknown layer during upload.
    BlobUnknown,
    /// Blob upload is invalid
    ///
    /// The blob upload encountered an error and can no longer proceed.
    BlobUploadInvalid,
    /// Blob upload is unknown to registry
    BlobUploadUnknown,
    /// Provided digest did not match uploaded content.
    DigestInvalid,
    /// Blob is unknown to registry
    ManifestBlobUnknown,
    /// Manifest is invalid
    ///
    /// During upload, manifests undergo several checks ensuring validity. If
    /// those checks fail, this error MAY be returned, unless a more specific
    /// error is included. The detail will contain information the failed
    /// validation.
    ManifestInvalid,
    /// Manifest unknown
    ///
    /// This error is returned when the manifest, identified by name and tag is unknown to the repository.
    ManifestUnknown,
    /// Invalid repository name
    NameInvalid,
    /// Repository name is not known
    NameUnknown,
    /// Provided length did not match content length
    SizeInvalid,
    /// Authentication required.
    Unauthorized,
    /// Requested access to the resource is denied
    Denied,
    /// This operation is unsupported
    Unsupported,
    /// Too many requests from client
    Toomanyrequests,
}

#[cfg(test)]
mod test {
    use super::*;

    const EXAMPLE_ERROR: &str = r#"
      {"errors":[{"code":"UNAUTHORIZED","message":"authentication required","detail":[{"Type":"repository","Name":"hello-wasm","Action":"pull"}]}]}
      "#;
    #[test]
    fn test_deserialize() {
        let envelope: OciEnvelope =
            serde_json::from_str(EXAMPLE_ERROR).expect("parse example error");
        let e = &envelope.errors[0];
        assert_eq!(OciErrorCode::Unauthorized, e.code);
        assert_eq!("authentication required", e.message);
        assert_ne!(serde_json::value::Value::Null, e.detail);
    }

    const EXAMPLE_ERROR_MISSING_MESSAGE: &str = r#"
      {"errors":[{"code":"UNAUTHORIZED","detail":[{"Type":"repository","Name":"hello-wasm","Action":"pull"}]}]}
      "#;
    #[test]
    fn test_deserialize_without_message_field() {
        let envelope: OciEnvelope =
            serde_json::from_str(EXAMPLE_ERROR_MISSING_MESSAGE).expect("parse example error");
        let e = &envelope.errors[0];
        assert_eq!(OciErrorCode::Unauthorized, e.code);
        assert_eq!(String::default(), e.message);
        assert_ne!(serde_json::value::Value::Null, e.detail);
    }

    const EXAMPLE_ERROR_MISSING_DETAIL: &str = r#"
      {"errors":[{"code":"UNAUTHORIZED","message":"authentication required"}]}
      "#;
    #[test]
    fn test_deserialize_without_detail_field() {
        let envelope: OciEnvelope =
            serde_json::from_str(EXAMPLE_ERROR_MISSING_DETAIL).expect("parse example error");
        let e = &envelope.errors[0];
        assert_eq!(OciErrorCode::Unauthorized, e.code);
        assert_eq!("authentication required", e.message);
        assert_eq!(serde_json::value::Value::Null, e.detail);
    }
}
