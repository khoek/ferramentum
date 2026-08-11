use std::io::{Read, Write};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::time::Duration;

use super::protocol::{
    ApplicationError, ApplicationRequest, ApplicationResponse, PROTOCOL_MAJOR, RequestEnvelope,
    RequestId, ResponseBody, ResponseEnvelope, decode, encode,
};

#[derive(Clone, Debug)]
pub struct ApplicationClient {
    socket_path: PathBuf,
    timeout: Duration,
}

impl ApplicationClient {
    pub fn new(socket_path: impl Into<PathBuf>) -> Self {
        Self {
            socket_path: socket_path.into(),
            timeout: Duration::from_secs(30),
        }
    }

    pub fn socket_path(&self) -> &Path {
        &self.socket_path
    }

    pub fn request(
        &self,
        request: ApplicationRequest,
    ) -> Result<ApplicationResponse, ApplicationError> {
        let request_id = RequestId::random();
        let payload = encode(&RequestEnvelope {
            request_id,
            minimum_protocol_major: PROTOCOL_MAJOR,
            maximum_protocol_major: PROTOCOL_MAJOR,
            request,
        })?;
        let mut stream = UnixStream::connect(&self.socket_path)?;
        stream.set_read_timeout(Some(self.timeout))?;
        stream.set_write_timeout(Some(self.timeout))?;
        stream.write_all(&(payload.len() as u32).to_be_bytes())?;
        stream.write_all(&payload)?;
        stream.flush()?;
        let response: ResponseEnvelope = decode(&read_frame(&mut stream)?)?;
        if response.request_id != request_id {
            return Err(ApplicationError::MismatchedRequestId);
        }
        if response.protocol_major != PROTOCOL_MAJOR {
            return Err(ApplicationError::UnsupportedProtocol(
                response.protocol_major,
            ));
        }
        match response.body {
            ResponseBody::Ok(response) => Ok(response),
            ResponseBody::Error(error) => Err(ApplicationError::Remote {
                code: error.code,
                message: error.message,
            }),
        }
    }
}

fn read_frame(stream: &mut UnixStream) -> Result<Vec<u8>, ApplicationError> {
    let mut header = [0_u8; 4];
    read_exact(stream, &mut header)?;
    let length = u32::from_be_bytes(header) as usize;
    if length > super::protocol::MAX_FRAME_BYTES {
        return Err(ApplicationError::FrameTooLarge);
    }
    let mut payload = vec![0_u8; length];
    read_exact(stream, &mut payload)?;
    Ok(payload)
}

fn read_exact(stream: &mut UnixStream, buffer: &mut [u8]) -> Result<(), ApplicationError> {
    match stream.read_exact(buffer) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => {
            Err(ApplicationError::EarlyEof)
        }
        Err(error) => Err(error.into()),
    }
}
