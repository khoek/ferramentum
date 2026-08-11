use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, SyncSender, TryRecvError};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};
use passless_uhid::{DeviceIdentity, RawUhidDevice, UhidEvent};
use rand::RngExt as _;
use soft_fido2_transport::{ChannelManager, Cmd, Message, Packet};

use crate::authenticator::{AuthenticatorEngine, PresenceGate};
use crate::vault::Vault;

const CTAPHID_BROADCAST_CID: u32 = 0xffff_ffff;
const CTAPHID_MAX_MESSAGE_SIZE: usize = 7609;
const INITIAL_PAYLOAD_SIZE: usize = 57;
const CONTINUATION_PAYLOAD_SIZE: usize = 59;
const MAX_ALLOCATED_CHANNELS: usize = 64;
const KEEPALIVE_INTERVAL: Duration = Duration::from_millis(100);
const POLL_INTERVAL: Duration = Duration::from_millis(2);
const CAPABILITY_WINK: u8 = 0x01;
const CAPABILITY_CBOR: u8 = 0x04;
const CAPABILITY_NMSG: u8 = 0x08;
const KEEPALIVE_PROCESSING: u8 = 0x01;
const KEEPALIVE_UP_NEEDED: u8 = 0x02;

pub fn run_uhid(
    vault: Vault,
    presence: PresenceGate,
    device_present: Arc<AtomicBool>,
) -> Result<()> {
    let identity = DeviceIdentity::new(
        "auc software authenticator",
        "auc/uhid",
        vault.device_unique_name()?,
        0x1209,
        0xa0c0,
        0x0001,
    );
    let device = RawUhidDevice::create(identity).context("failed to create auc UHID device")?;
    device
        .set_nonblocking(true)
        .context("failed to make auc UHID descriptor nonblocking")?;
    let _presence = DevicePresence::new(Arc::clone(&device_present));
    let engine = AuthenticatorEngine::new(vault, presence.clone())?;
    let (work_tx, work_rx) = mpsc::sync_channel(1);
    let (result_tx, result_rx) = mpsc::sync_channel(1);
    thread::Builder::new()
        .name("auc-ctap".to_string())
        .spawn(move || command_worker(engine, work_rx, result_tx))
        .context("failed to start auc CTAP worker")?;
    TransportLoop::new(UhidEndpoint::new(device), presence, work_tx, result_rx).run()
}

struct DevicePresence {
    present: Arc<AtomicBool>,
}

impl DevicePresence {
    fn new(present: Arc<AtomicBool>) -> Self {
        present.store(true, Ordering::Release);
        Self { present }
    }
}

impl Drop for DevicePresence {
    fn drop(&mut self) {
        self.present.store(false, Ordering::Release);
    }
}

struct CtapWork {
    channel: u32,
    request: Vec<u8>,
}

struct CtapResult {
    channel: u32,
    response: Result<Vec<u8>>,
}

fn command_worker(
    mut engine: AuthenticatorEngine,
    work: Receiver<CtapWork>,
    results: SyncSender<CtapResult>,
) {
    while let Ok(work) = work.recv() {
        let result = CtapResult {
            channel: work.channel,
            response: engine.handle(&work.request),
        };
        if results.send(result).is_err() {
            break;
        }
    }
}

trait HidEndpoint {
    fn read_event(&mut self) -> Result<Option<EndpointEvent>>;
    fn write_packet(&mut self, packet: &[u8; 64]) -> Result<()>;
}

enum EndpointEvent {
    Packet([u8; 64]),
    Disconnected,
}

struct UhidEndpoint {
    device: RawUhidDevice,
}

impl UhidEndpoint {
    fn new(device: RawUhidDevice) -> Self {
        Self { device }
    }
}

impl HidEndpoint for UhidEndpoint {
    fn read_event(&mut self) -> Result<Option<EndpointEvent>> {
        loop {
            match self.device.read_event()? {
                Some(UhidEvent::Output { data, .. }) if data.len() == 65 && data[0] == 0 => {
                    return Ok(Some(EndpointEvent::Packet(
                        data[1..]
                            .try_into()
                            .expect("validated FIDO report has 64 bytes"),
                    )));
                }
                Some(UhidEvent::Output { data, .. }) if data.len() == 64 => {
                    return Ok(Some(EndpointEvent::Packet(
                        data.as_slice()
                            .try_into()
                            .expect("validated FIDO report has 64 bytes"),
                    )));
                }
                Some(UhidEvent::Close) => return Ok(Some(EndpointEvent::Disconnected)),
                Some(UhidEvent::GetReport { id, .. }) => {
                    self.device
                        .send_get_report_reply(id, libc::EIO as u16, &[])?;
                }
                Some(UhidEvent::SetReport { id, .. }) => {
                    self.device.send_set_report_reply(id, libc::EIO as u16)?;
                }
                Some(_) => {}
                None => return Ok(None),
            }
        }
    }

    fn write_packet(&mut self, packet: &[u8; 64]) -> Result<()> {
        self.device.write_packet(packet).map_err(Into::into)
    }
}

struct ActiveCtap {
    channel: u32,
    cancelled: bool,
    last_keepalive: Instant,
}

struct ChannelLock {
    channel: u32,
    expires: Instant,
}

struct TransportLoop<E> {
    endpoint: E,
    assembly: ChannelManager,
    allocated: HashMap<u32, Instant>,
    lock: Option<ChannelLock>,
    active: Option<ActiveCtap>,
    presence: PresenceGate,
    work: SyncSender<CtapWork>,
    results: Receiver<CtapResult>,
}

impl<E: HidEndpoint> TransportLoop<E> {
    fn new(
        endpoint: E,
        presence: PresenceGate,
        work: SyncSender<CtapWork>,
        results: Receiver<CtapResult>,
    ) -> Self {
        Self {
            endpoint,
            assembly: ChannelManager::new(),
            allocated: HashMap::new(),
            lock: None,
            active: None,
            presence,
            work,
            results,
        }
    }

    fn run(mut self) -> Result<()> {
        loop {
            match self.endpoint.read_event()? {
                Some(EndpointEvent::Packet(packet)) => {
                    self.process_packet(Packet::from_bytes(packet))?;
                }
                Some(EndpointEvent::Disconnected) => self.cancel_active(),
                None => {}
            }
            self.process_result()?;
            self.send_keepalive()?;
            thread::sleep(POLL_INTERVAL);
        }
    }

    fn process_packet(&mut self, packet: Packet) -> Result<()> {
        if !self.packet_channel_is_valid(&packet) {
            return self.write_error(packet.cid(), TransportError::InvalidChannel);
        }
        let channel = packet.cid();
        if packet.is_init()
            && packet
                .payload_len()
                .is_some_and(|length| usize::from(length) > CTAPHID_MAX_MESSAGE_SIZE)
        {
            return self.write_error(channel, TransportError::InvalidLength);
        }
        if let Some(last_used) = self.allocated.get_mut(&channel) {
            *last_used = Instant::now();
        }
        match self.assembly.process_packet(packet) {
            Ok(Some(message)) => self.process_message(message),
            Ok(None) => Ok(()),
            Err(error) => self.write_error(channel, TransportError::from_soft(error)),
        }
    }

    fn packet_channel_is_valid(&self, packet: &Packet) -> bool {
        if packet.cid() == CTAPHID_BROADCAST_CID {
            return packet.is_init() && packet.cmd() == Some(Cmd::Init);
        }
        self.allocated.contains_key(&packet.cid())
    }

    fn process_message(&mut self, message: Message) -> Result<()> {
        self.expire_lock();
        if self
            .lock
            .as_ref()
            .is_some_and(|lock| lock.channel != message.cid && message.cmd != Cmd::Init)
        {
            return self.write_error(message.cid, TransportError::ChannelBusy);
        }
        match message.cmd {
            Cmd::Init => self.handle_init(message),
            Cmd::Ping => self.write_message(Message::new(
                message.cid,
                Cmd::Ping,
                message.data,
                Some(CTAPHID_MAX_MESSAGE_SIZE),
            )),
            Cmd::Wink if message.data.is_empty() => self.write_message(Message::new(
                message.cid,
                Cmd::Wink,
                Vec::new(),
                Some(CTAPHID_MAX_MESSAGE_SIZE),
            )),
            Cmd::Wink => self.write_error(message.cid, TransportError::InvalidLength),
            Cmd::Lock => self.handle_lock(message),
            Cmd::Cancel => {
                if !message.data.is_empty() {
                    return self.write_error(message.cid, TransportError::InvalidLength);
                }
                self.cancel(message.cid);
                Ok(())
            }
            Cmd::Cbor => self.start_cbor(message),
            Cmd::Msg | Cmd::Keepalive | Cmd::Error => {
                self.write_error(message.cid, TransportError::InvalidCommand)
            }
            _ => self.write_error(message.cid, TransportError::InvalidCommand),
        }
    }

    fn handle_init(&mut self, message: Message) -> Result<()> {
        if message.data.len() != 8 {
            return self.write_error(message.cid, TransportError::InvalidLength);
        }
        if message.cid != CTAPHID_BROADCAST_CID {
            self.cancel(message.cid);
        }
        let channel = if message.cid == CTAPHID_BROADCAST_CID {
            let Some(channel) = self.allocate_channel() else {
                return self.write_error(message.cid, TransportError::ChannelBusy);
            };
            channel
        } else {
            message.cid
        };
        let mut data = Vec::with_capacity(17);
        data.extend_from_slice(&message.data);
        data.extend_from_slice(&channel.to_be_bytes());
        data.push(2);
        data.push(
            env!("CARGO_PKG_VERSION_MAJOR")
                .parse()
                .context("auc major version does not fit the CTAPHID version field")?,
        );
        data.push(
            env!("CARGO_PKG_VERSION_MINOR")
                .parse()
                .context("auc minor version does not fit the CTAPHID version field")?,
        );
        data.push(
            env!("CARGO_PKG_VERSION_PATCH")
                .parse()
                .context("auc patch version does not fit the CTAPHID version field")?,
        );
        data.push(CAPABILITY_WINK | CAPABILITY_CBOR | CAPABILITY_NMSG);
        self.write_message(Message::new(
            message.cid,
            Cmd::Init,
            data,
            Some(CTAPHID_MAX_MESSAGE_SIZE),
        ))
    }

    fn allocate_channel(&mut self) -> Option<u32> {
        if self.allocated.len() >= MAX_ALLOCATED_CHANNELS {
            let protected_active = self.active.as_ref().map(|active| active.channel);
            let protected_lock = self.lock.as_ref().map(|lock| lock.channel);
            let (&oldest, _) = self
                .allocated
                .iter()
                .filter(|(channel, _)| {
                    Some(**channel) != protected_active && Some(**channel) != protected_lock
                })
                .min_by_key(|(_, last_used)| *last_used)?;
            self.allocated.remove(&oldest);
            self.assembly.cancel_channel(oldest);
        }
        loop {
            let channel: u32 = rand::rng().random();
            if channel != 0
                && channel != CTAPHID_BROADCAST_CID
                && !self.allocated.contains_key(&channel)
            {
                self.allocated.insert(channel, Instant::now());
                return Some(channel);
            }
        }
    }

    fn handle_lock(&mut self, message: Message) -> Result<()> {
        let Some(&seconds) = message.data.first().filter(|_| message.data.len() == 1) else {
            return self.write_error(message.cid, TransportError::InvalidLength);
        };
        if seconds > 10 {
            return self.write_error(message.cid, TransportError::InvalidParameter);
        }
        if seconds == 0 {
            if self
                .lock
                .as_ref()
                .is_some_and(|lock| lock.channel == message.cid)
            {
                self.lock = None;
            }
        } else {
            self.lock = Some(ChannelLock {
                channel: message.cid,
                expires: Instant::now() + Duration::from_secs(seconds.into()),
            });
        }
        self.write_message(Message::new(
            message.cid,
            Cmd::Lock,
            Vec::new(),
            Some(CTAPHID_MAX_MESSAGE_SIZE),
        ))
    }

    fn start_cbor(&mut self, message: Message) -> Result<()> {
        if message.data.len() > CTAPHID_MAX_MESSAGE_SIZE {
            return self.write_error(message.cid, TransportError::InvalidLength);
        }
        if self.active.is_some() {
            return self.write_error(message.cid, TransportError::ChannelBusy);
        }
        self.presence.begin_command(message.cid)?;
        if self
            .work
            .send(CtapWork {
                channel: message.cid,
                request: message.data,
            })
            .is_err()
        {
            self.presence.finish_command(message.cid);
            bail!("auc CTAP worker exited unexpectedly");
        }
        self.active = Some(ActiveCtap {
            channel: message.cid,
            cancelled: false,
            last_keepalive: Instant::now(),
        });
        Ok(())
    }

    fn cancel(&mut self, channel: u32) {
        self.assembly.cancel_channel(channel);
        if let Some(active) = self
            .active
            .as_mut()
            .filter(|active| active.channel == channel)
        {
            active.cancelled = true;
            self.presence.cancel(channel);
        }
    }

    fn cancel_active(&mut self) {
        if let Some(active) = self.active.as_mut() {
            active.cancelled = true;
            self.presence.cancel(active.channel);
        }
        self.assembly.clear();
    }

    fn process_result(&mut self) -> Result<()> {
        match self.results.try_recv() {
            Ok(result) => {
                let active = self
                    .active
                    .take()
                    .ok_or_else(|| anyhow!("auc CTAP worker returned without an active command"))?;
                if result.channel != active.channel {
                    bail!("auc CTAP worker returned a mismatched channel");
                }
                let cancelled = active.cancelled || self.presence.is_cancelled(active.channel);
                self.presence.finish_command(active.channel);
                if cancelled {
                    return Ok(());
                }
                match result.response {
                    Ok(response) => self.write_message(Message::new(
                        active.channel,
                        Cmd::Cbor,
                        response,
                        Some(CTAPHID_MAX_MESSAGE_SIZE),
                    )),
                    Err(error) => {
                        eprintln!("auc CTAP command failed: {error:#}");
                        self.write_error(active.channel, TransportError::Other)
                    }
                }
            }
            Err(TryRecvError::Empty) => Ok(()),
            Err(TryRecvError::Disconnected) => bail!("auc CTAP worker disconnected"),
        }
    }

    fn send_keepalive(&mut self) -> Result<()> {
        let Some(active) = self.active.as_mut() else {
            return Ok(());
        };
        if active.cancelled || active.last_keepalive.elapsed() < KEEPALIVE_INTERVAL {
            return Ok(());
        }
        let channel = active.channel;
        active.last_keepalive = Instant::now();
        let status = if self.presence.is_waiting(channel) {
            KEEPALIVE_UP_NEEDED
        } else {
            KEEPALIVE_PROCESSING
        };
        self.write_message(Message::new(
            channel,
            Cmd::Keepalive,
            vec![status],
            Some(CTAPHID_MAX_MESSAGE_SIZE),
        ))
    }

    fn expire_lock(&mut self) {
        if self
            .lock
            .as_ref()
            .is_some_and(|lock| Instant::now() >= lock.expires)
        {
            self.lock = None;
        }
    }

    fn write_message(&mut self, message: Message) -> Result<()> {
        for packet in fragment_message(&message)? {
            self.endpoint.write_packet(&packet)?;
        }
        Ok(())
    }

    fn write_error(&mut self, channel: u32, error: TransportError) -> Result<()> {
        self.endpoint
            .write_packet(Packet::new_error(channel, error.soft()).as_bytes())
    }
}

fn fragment_message(message: &Message) -> Result<Vec<[u8; 64]>> {
    let limit = message.max_msg_size.unwrap_or(CTAPHID_MAX_MESSAGE_SIZE);
    if message.data.len() > limit || message.data.len() > CTAPHID_MAX_MESSAGE_SIZE {
        bail!("auc CTAPHID response exceeds the protocol limit");
    }
    let mut packets = Vec::with_capacity(
        1 + message
            .data
            .len()
            .saturating_sub(INITIAL_PAYLOAD_SIZE)
            .div_ceil(CONTINUATION_PAYLOAD_SIZE),
    );
    let mut initial = [0_u8; 64];
    initial[..4].copy_from_slice(&message.cid.to_be_bytes());
    initial[4] = message.cmd.to_u8_init();
    initial[5..7].copy_from_slice(&(message.data.len() as u16).to_be_bytes());
    let initial_length = message.data.len().min(INITIAL_PAYLOAD_SIZE);
    initial[7..7 + initial_length].copy_from_slice(&message.data[..initial_length]);
    packets.push(initial);
    for (sequence, chunk) in message.data[initial_length..]
        .chunks(CONTINUATION_PAYLOAD_SIZE)
        .enumerate()
    {
        let sequence = u8::try_from(sequence)
            .ok()
            .filter(|sequence| *sequence <= 127)
            .ok_or_else(|| anyhow!("auc CTAPHID response requires too many packets"))?;
        let mut continuation = [0_u8; 64];
        continuation[..4].copy_from_slice(&message.cid.to_be_bytes());
        continuation[4] = sequence;
        continuation[5..5 + chunk.len()].copy_from_slice(chunk);
        packets.push(continuation);
    }
    Ok(packets)
}

#[derive(Clone, Copy)]
enum TransportError {
    InvalidCommand,
    InvalidParameter,
    InvalidLength,
    InvalidSequence,
    MessageTimeout,
    ChannelBusy,
    InvalidChannel,
    Other,
}

impl TransportError {
    fn from_soft(error: soft_fido2_transport::Error) -> Self {
        match error {
            soft_fido2_transport::Error::InvalidSequence => Self::InvalidSequence,
            soft_fido2_transport::Error::InvalidChannel => Self::InvalidChannel,
            soft_fido2_transport::Error::InvalidCommand => Self::InvalidCommand,
            soft_fido2_transport::Error::InvalidPacket
            | soft_fido2_transport::Error::FragmentationError => Self::InvalidSequence,
            soft_fido2_transport::Error::MessageTooLarge => Self::InvalidLength,
            soft_fido2_transport::Error::Timeout => Self::MessageTimeout,
            soft_fido2_transport::Error::ChannelBusy => Self::ChannelBusy,
            _ => Self::Other,
        }
    }

    fn soft(self) -> soft_fido2_transport::ctaphid::ErrorCode {
        use soft_fido2_transport::ctaphid::ErrorCode;

        match self {
            Self::InvalidCommand => ErrorCode::InvalidCmd,
            Self::InvalidParameter => ErrorCode::InvalidPar,
            Self::InvalidLength => ErrorCode::InvalidLen,
            Self::InvalidSequence => ErrorCode::InvalidSeq,
            Self::MessageTimeout => ErrorCode::MsgTimeout,
            Self::ChannelBusy => ErrorCode::ChannelBusy,
            Self::InvalidChannel => ErrorCode::InvalidChannel,
            Self::Other => ErrorCode::Other,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use super::*;

    #[derive(Default)]
    struct MockEndpoint {
        incoming: VecDeque<EndpointEvent>,
        outgoing: Vec<[u8; 64]>,
    }

    impl HidEndpoint for MockEndpoint {
        fn read_event(&mut self) -> Result<Option<EndpointEvent>> {
            Ok(self.incoming.pop_front())
        }

        fn write_packet(&mut self, packet: &[u8; 64]) -> Result<()> {
            self.outgoing.push(*packet);
            Ok(())
        }
    }

    fn transport() -> (
        TransportLoop<MockEndpoint>,
        Receiver<CtapWork>,
        SyncSender<CtapResult>,
    ) {
        let (work_tx, work_rx) = mpsc::sync_channel(1);
        let (result_tx, result_rx) = mpsc::sync_channel(1);
        (
            TransportLoop::new(
                MockEndpoint::default(),
                PresenceGate::new(),
                work_tx,
                result_rx,
            ),
            work_rx,
            result_tx,
        )
    }

    fn process_message(transport: &mut TransportLoop<MockEndpoint>, message: &Message) {
        for packet in fragment_message(message).unwrap() {
            transport
                .process_packet(Packet::from_bytes(packet))
                .unwrap();
        }
    }

    #[test]
    fn ctaphid_fragmentation_round_trips_at_the_protocol_limit() {
        let message = Message::new(
            0xdead_beef,
            Cmd::Cbor,
            vec![0xa5; CTAPHID_MAX_MESSAGE_SIZE],
            Some(CTAPHID_MAX_MESSAGE_SIZE),
        );
        let packets = fragment_message(&message)
            .unwrap()
            .into_iter()
            .map(Packet::from_bytes)
            .collect::<Vec<_>>();
        assert_eq!(
            Message::from_packets(&packets, Some(CTAPHID_MAX_MESSAGE_SIZE)).unwrap(),
            message
        );
    }

    #[test]
    fn transport_errors_map_to_specific_ctaphid_codes() {
        use soft_fido2_transport::ctaphid::ErrorCode;

        assert_eq!(TransportError::InvalidLength.soft(), ErrorCode::InvalidLen);
        assert_eq!(TransportError::ChannelBusy.soft(), ErrorCode::ChannelBusy);
        assert_eq!(
            TransportError::InvalidChannel.soft(),
            ErrorCode::InvalidChannel
        );
    }

    #[test]
    fn broadcast_init_allocates_a_channel_and_echoes_the_nonce() {
        let (mut transport, _, _) = transport();
        let nonce = vec![0x5a; 8];
        process_message(
            &mut transport,
            &Message::new(
                CTAPHID_BROADCAST_CID,
                Cmd::Init,
                nonce.clone(),
                Some(CTAPHID_MAX_MESSAGE_SIZE),
            ),
        );
        let response = Message::from_packets(
            &transport
                .endpoint
                .outgoing
                .iter()
                .copied()
                .map(Packet::from_bytes)
                .collect::<Vec<_>>(),
            Some(CTAPHID_MAX_MESSAGE_SIZE),
        )
        .unwrap();

        assert_eq!(response.cmd, Cmd::Init);
        assert_eq!(&response.data[..8], nonce);
        let channel = u32::from_be_bytes(response.data[8..12].try_into().unwrap());
        assert!(transport.allocated.contains_key(&channel));
        assert_eq!(
            response.data[16],
            CAPABILITY_WINK | CAPABILITY_CBOR | CAPABILITY_NMSG
        );
    }

    #[test]
    fn cancel_discards_the_eventual_cbor_worker_result() {
        let (mut transport, work, results) = transport();
        let channel = 0xdead_beef;
        transport.allocated.insert(channel, Instant::now());
        process_message(
            &mut transport,
            &Message::new(
                channel,
                Cmd::Cbor,
                vec![0x04],
                Some(CTAPHID_MAX_MESSAGE_SIZE),
            ),
        );
        assert_eq!(work.recv().unwrap().channel, channel);
        process_message(
            &mut transport,
            &Message::new(
                channel,
                Cmd::Cancel,
                Vec::new(),
                Some(CTAPHID_MAX_MESSAGE_SIZE),
            ),
        );
        results
            .send(CtapResult {
                channel,
                response: Ok(vec![0x00]),
            })
            .unwrap();
        transport.process_result().unwrap();

        assert!(transport.endpoint.outgoing.is_empty());
        assert!(transport.active.is_none());
        assert!(!transport.presence.has_pending_touch());
    }

    #[test]
    fn oversized_initial_frame_is_rejected_before_assembly() {
        let (mut transport, _, _) = transport();
        let channel = 0xdead_beef;
        transport.allocated.insert(channel, Instant::now());
        let mut bytes = [0_u8; 64];
        bytes[..4].copy_from_slice(&channel.to_be_bytes());
        bytes[4] = Cmd::Cbor.to_u8_init();
        bytes[5..7].copy_from_slice(&((CTAPHID_MAX_MESSAGE_SIZE + 1) as u16).to_be_bytes());

        transport.process_packet(Packet::from_bytes(bytes)).unwrap();
        let response = Message::from_packets(
            &[Packet::from_bytes(transport.endpoint.outgoing[0])],
            Some(CTAPHID_MAX_MESSAGE_SIZE),
        )
        .unwrap();
        assert_eq!(response.cmd, Cmd::Error);
        assert_eq!(
            response.data,
            vec![soft_fido2_transport::ctaphid::ErrorCode::InvalidLen as u8]
        );
    }

    #[test]
    fn channel_eviction_preserves_the_active_and_locked_channels() {
        let (mut transport, _, _) = transport();
        let now = Instant::now();
        for channel in 1..=MAX_ALLOCATED_CHANNELS as u32 {
            transport
                .allocated
                .insert(channel, now + Duration::from_millis(channel.into()));
        }
        transport.active = Some(ActiveCtap {
            channel: 1,
            cancelled: false,
            last_keepalive: now,
        });
        transport.lock = Some(ChannelLock {
            channel: 2,
            expires: now + Duration::from_secs(10),
        });

        let allocated = transport.allocate_channel().unwrap();
        assert!(transport.allocated.contains_key(&1));
        assert!(transport.allocated.contains_key(&2));
        assert!(!transport.allocated.contains_key(&3));
        assert!(transport.allocated.contains_key(&allocated));
        assert_eq!(transport.allocated.len(), MAX_ALLOCATED_CHANNELS);
    }
}
