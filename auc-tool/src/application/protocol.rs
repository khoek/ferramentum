use serde::{Deserialize, Serialize};

pub const PROTOCOL_MAJOR: u16 = 1;
pub(crate) const MAX_FRAME_BYTES: usize = 64 * 1024;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RequestId([u8; 16]);

impl RequestId {
    pub fn random() -> Self {
        use rand::RngExt as _;

        Self(rand::rng().random())
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case", tag = "method")]
pub enum ApplicationRequest {
    Status,
    Touch,
    ListCredentials,
    DeleteCredential {
        credential_id: String,
    },
    #[serde(other)]
    Unknown,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case", tag = "result")]
pub enum ApplicationResponse {
    Status(Status),
    Touch(TouchReceipt),
    Credentials { credentials: Vec<CredentialSummary> },
    Deleted { credential_id: String },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Status {
    pub product: String,
    pub package: String,
    pub version: String,
    pub protocol_major: u16,
    pub device_present: bool,
    pub pending_touch: bool,
    pub credential_count: usize,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TouchReceipt {
    pub operation: String,
    pub rp_id: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CredentialSummary {
    pub credential_id: String,
    pub rp_id: String,
    pub user_name: Option<String>,
    pub discoverable: bool,
    pub backup_eligible: bool,
    pub backed_up: bool,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum ErrorCode {
    BadRequest,
    Unauthorized,
    UnsupportedProtocol,
    NotFound,
    Conflict,
    Unavailable,
    Internal,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ProtocolError {
    pub code: ErrorCode,
    pub message: String,
}

impl ProtocolError {
    pub fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ApplicationError {
    #[error("auc application frame exceeds the {MAX_FRAME_BYTES}-byte limit")]
    FrameTooLarge,
    #[error("auc agent closed the connection before sending a complete frame")]
    EarlyEof,
    #[error("failed to encode auc application CBOR: {0}")]
    Encode(String),
    #[error("failed to decode auc application CBOR: {0}")]
    Decode(String),
    #[error("auc response request ID did not match the request")]
    MismatchedRequestId,
    #[error("auc application protocol v{0} is unsupported")]
    UnsupportedProtocol(u16),
    #[error("auc request failed ({code:?}): {message}")]
    Remote { code: ErrorCode, message: String },
    #[error("auc application I/O failed: {0}")]
    Io(#[from] std::io::Error),
}

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct RequestEnvelope {
    pub request_id: RequestId,
    pub minimum_protocol_major: u16,
    pub maximum_protocol_major: u16,
    pub request: ApplicationRequest,
}

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct ResponseEnvelope {
    pub request_id: RequestId,
    pub protocol_major: u16,
    pub body: ResponseBody,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case", tag = "status", content = "body")]
pub(crate) enum ResponseBody {
    Ok(ApplicationResponse),
    Error(ProtocolError),
}

pub(crate) fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>, ApplicationError> {
    let mut bytes = Vec::new();
    ciborium::into_writer(value, &mut bytes)
        .map_err(|error| ApplicationError::Encode(error.to_string()))?;
    if bytes.len() > MAX_FRAME_BYTES {
        return Err(ApplicationError::FrameTooLarge);
    }
    Ok(bytes)
}

pub(crate) fn decode<T>(bytes: &[u8]) -> Result<T, ApplicationError>
where
    T: for<'de> Deserialize<'de>,
{
    if bytes.len() > MAX_FRAME_BYTES {
        return Err(ApplicationError::FrameTooLarge);
    }
    ciborium::from_reader(bytes).map_err(|error| ApplicationError::Decode(error.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn requests_round_trip_and_frames_are_bounded() {
        let envelope = RequestEnvelope {
            request_id: RequestId([0xDE; 16]),
            minimum_protocol_major: 1,
            maximum_protocol_major: 1,
            request: ApplicationRequest::DeleteCredential {
                credential_id: "deadbeef".to_string(),
            },
        };
        let decoded: RequestEnvelope = decode(&encode(&envelope).unwrap()).unwrap();
        assert_eq!(decoded.request_id, envelope.request_id);
        assert_eq!(decoded.request, envelope.request);
        assert!(matches!(
            decode::<RequestEnvelope>(&vec![0; MAX_FRAME_BYTES + 1]),
            Err(ApplicationError::FrameTooLarge)
        ));
    }

    #[test]
    fn protocol_v1_ignores_additive_fields() {
        let envelope = RequestEnvelope {
            request_id: RequestId([0xDE; 16]),
            minimum_protocol_major: PROTOCOL_MAJOR,
            maximum_protocol_major: PROTOCOL_MAJOR,
            request: ApplicationRequest::DeleteCredential {
                credential_id: "deadbeef".to_string(),
            },
        };
        let mut value: ciborium::Value =
            ciborium::from_reader(encode(&envelope).unwrap().as_slice()).unwrap();
        let ciborium::Value::Map(fields) = &mut value else {
            panic!("application envelope must encode as a map");
        };
        fields.push((
            ciborium::Value::Text("future-envelope-field".to_string()),
            ciborium::Value::Bool(true),
        ));
        let request = fields
            .iter_mut()
            .find_map(|(key, value)| {
                (key == &ciborium::Value::Text("request".to_string())).then_some(value)
            })
            .unwrap();
        let ciborium::Value::Map(request_fields) = request else {
            panic!("application method must encode as a map");
        };
        request_fields.push((
            ciborium::Value::Text("future-method-field".to_string()),
            ciborium::Value::Integer(0xDEADBEEF_u64.into()),
        ));
        let mut encoded = Vec::new();
        ciborium::into_writer(&value, &mut encoded).unwrap();

        let decoded: RequestEnvelope = decode(&encoded).unwrap();
        assert_eq!(decoded.request, envelope.request);
    }
}
