use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use capulus::managed::PeerCredentials;
use rustix::net::sockopt::socket_peercred;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::Semaphore;

use super::protocol::{
    ApplicationError, ApplicationRequest, ApplicationResponse, ErrorCode, PROTOCOL_MAJOR,
    ProtocolError, RequestEnvelope, ResponseBody, ResponseEnvelope, decode, encode,
};

pub trait ApplicationHandler: Send + Sync + 'static {
    fn handle(
        &self,
        peer: PeerCredentials,
        request: ApplicationRequest,
    ) -> impl Future<Output = Result<ApplicationResponse, ProtocolError>> + Send;
}

pub struct ApplicationServer<H> {
    listener: UnixListener,
    handler: Arc<H>,
}

impl<H: ApplicationHandler> ApplicationServer<H> {
    pub fn new(listener: UnixListener, handler: Arc<H>) -> Self {
        Self { listener, handler }
    }

    pub async fn run(self) -> Result<(), ApplicationError> {
        let permits = Arc::new(Semaphore::new(32));
        loop {
            let permit = Arc::clone(&permits)
                .acquire_owned()
                .await
                .expect("application connection semaphore remains open");
            let (stream, _) = self.listener.accept().await?;
            let handler = Arc::clone(&self.handler);
            tokio::spawn(async move {
                let _permit = permit;
                if let Err(error) =
                    tokio::time::timeout(Duration::from_secs(30), serve_connection(stream, handler))
                        .await
                        .map_err(|_| {
                            ApplicationError::Io(std::io::Error::from(std::io::ErrorKind::TimedOut))
                        })
                        .and_then(|result| result)
                {
                    eprintln!("auc application connection failed: {error}");
                }
            });
        }
    }
}

async fn serve_connection<H: ApplicationHandler>(
    mut stream: UnixStream,
    handler: Arc<H>,
) -> Result<(), ApplicationError> {
    let credentials = socket_peercred(&stream).map_err(|error| {
        ApplicationError::Io(std::io::Error::from_raw_os_error(error.raw_os_error()))
    })?;
    let peer = PeerCredentials {
        pid: credentials.pid.as_raw_nonzero().get() as u32,
        uid: credentials.uid.as_raw(),
        gid: credentials.gid.as_raw(),
    };
    let request: RequestEnvelope = decode(&read_frame(&mut stream).await?)?;
    let body = if request.minimum_protocol_major <= PROTOCOL_MAJOR
        && request.maximum_protocol_major >= PROTOCOL_MAJOR
    {
        match handler.handle(peer, request.request).await {
            Ok(response) => ResponseBody::Ok(response),
            Err(error) => ResponseBody::Error(error),
        }
    } else {
        ResponseBody::Error(ProtocolError::new(
            ErrorCode::UnsupportedProtocol,
            format!("auc-agent supports application protocol v{PROTOCOL_MAJOR}"),
        ))
    };
    let payload = encode(&ResponseEnvelope {
        request_id: request.request_id,
        protocol_major: PROTOCOL_MAJOR,
        body,
    })?;
    stream.write_u32(payload.len() as u32).await?;
    stream.write_all(&payload).await?;
    stream.flush().await?;
    Ok(())
}

async fn read_frame(stream: &mut UnixStream) -> Result<Vec<u8>, ApplicationError> {
    let length = match stream.read_u32().await {
        Ok(length) => length as usize,
        Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => {
            return Err(ApplicationError::EarlyEof);
        }
        Err(error) => return Err(error.into()),
    };
    if length > super::protocol::MAX_FRAME_BYTES {
        return Err(ApplicationError::FrameTooLarge);
    }
    let mut payload = vec![0_u8; length];
    stream.read_exact(&mut payload).await.map_err(|error| {
        if error.kind() == std::io::ErrorKind::UnexpectedEof {
            ApplicationError::EarlyEof
        } else {
            error.into()
        }
    })?;
    Ok(payload)
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use tokio::sync::oneshot;

    use super::*;
    use crate::application::{ApplicationClient, Status};

    struct RecordingHandler {
        credentials: Mutex<Option<oneshot::Sender<PeerCredentials>>>,
    }

    impl ApplicationHandler for RecordingHandler {
        async fn handle(
            &self,
            peer: PeerCredentials,
            _request: ApplicationRequest,
        ) -> Result<ApplicationResponse, ProtocolError> {
            if let Some(sender) = self.credentials.lock().expect("lock").take() {
                let _ = sender.send(peer);
            }
            Ok(ApplicationResponse::Status(Status {
                product: "auc".to_string(),
                package: "auc-tool".to_string(),
                version: env!("CARGO_PKG_VERSION").to_string(),
                protocol_major: PROTOCOL_MAJOR,
                device_present: false,
                pending_touch: false,
                credential_count: 0,
            }))
        }
    }

    #[tokio::test]
    async fn server_uses_kernel_peer_credentials() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("agent.sock");
        let listener = UnixListener::bind(&path).unwrap();
        let (credentials_tx, credentials_rx) = oneshot::channel();
        let server = tokio::spawn(
            ApplicationServer::new(
                listener,
                Arc::new(RecordingHandler {
                    credentials: Mutex::new(Some(credentials_tx)),
                }),
            )
            .run(),
        );
        let response = tokio::task::spawn_blocking(move || {
            ApplicationClient::new(path).request(ApplicationRequest::Status)
        })
        .await
        .unwrap()
        .unwrap();
        let credentials = tokio::time::timeout(Duration::from_secs(1), credentials_rx)
            .await
            .unwrap()
            .unwrap();
        server.abort();

        assert!(matches!(response, ApplicationResponse::Status(_)));
        assert_eq!(credentials.pid, std::process::id());
        assert_eq!(credentials.uid, rustix::process::geteuid().as_raw());
        assert_eq!(credentials.gid, rustix::process::getegid().as_raw());
    }
}
