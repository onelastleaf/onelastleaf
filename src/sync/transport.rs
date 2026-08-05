use std::{fmt, io, time::Duration};

use prost::Message;
use snow::{Builder, HandshakeState, TransportState, params::NoiseParams};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    time::{Instant, timeout_at},
};

use crate::protocol::oll::SyncEnvelope;

use super::security::NoisePsk;

pub(crate) const HANDSHAKE_DEADLINE: Duration = Duration::from_secs(10);
pub(crate) const MAX_CHUNK_BYTES: u32 = 61_440;
const PREFACE: &[u8; 8] = b"OLLSYNC\x01";
const NOISE_PARAMS: &str = "Noise_NNpsk0_25519_ChaChaPoly_BLAKE2s";
const MAX_HANDSHAKE_MESSAGE: usize = 1_024;
const MAX_CIPHERTEXT: usize = u16::MAX as usize;
const NOISE_TAG_BYTES: usize = 16;
pub(super) const MAX_PLAINTEXT: usize = MAX_CIPHERTEXT - NOISE_TAG_BYTES;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TransportError {
    Io(io::ErrorKind),
    DeadlineExceeded,
    InvalidPreface,
    InvalidFrameLength,
    NoiseHandshake,
    NoiseTransport,
    EnvelopeTooLarge,
    InvalidEnvelope,
}

impl fmt::Display for TransportError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(_) => formatter.write_str("sync transport I/O failed"),
            Self::DeadlineExceeded => formatter.write_str("sync transport deadline exceeded"),
            Self::InvalidPreface => formatter.write_str("sync transport preface is invalid"),
            Self::InvalidFrameLength => {
                formatter.write_str("sync transport frame length is invalid")
            }
            Self::NoiseHandshake => formatter.write_str("Noise handshake failed"),
            Self::NoiseTransport => formatter.write_str("Noise transport authentication failed"),
            Self::EnvelopeTooLarge => {
                formatter.write_str("sync envelope exceeds the transport frame limit")
            }
            Self::InvalidEnvelope => formatter.write_str("sync envelope protobuf is invalid"),
        }
    }
}

impl std::error::Error for TransportError {}

pub(crate) struct NoiseTransport<S> {
    stream: S,
    noise: TransportState,
    handshake_hash: [u8; 32],
    read_buffer: Vec<u8>,
}

impl<S> NoiseTransport<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    pub(crate) async fn connect(
        mut stream: S,
        psk: &NoisePsk,
        deadline: Instant,
    ) -> Result<Self, TransportError> {
        let mut handshake = handshake_state(psk, true)?;
        let mut message = [0_u8; MAX_HANDSHAKE_MESSAGE];
        let written = handshake
            .write_message(&[], &mut message)
            .map_err(|_| TransportError::NoiseHandshake)?;
        let mut initial = Vec::with_capacity(PREFACE.len() + 2 + written);
        initial.extend_from_slice(PREFACE);
        initial.extend_from_slice(
            &u16::try_from(written)
                .expect("Noise handshake message fits its documented limit")
                .to_be_bytes(),
        );
        initial.extend_from_slice(&message[..written]);
        write_all_until(&mut stream, &initial, Some(deadline)).await?;

        let response = read_frame(&mut stream, MAX_HANDSHAKE_MESSAGE, Some(deadline)).await?;
        handshake
            .read_message(&response, &mut [])
            .map_err(|_| TransportError::NoiseHandshake)?;
        finish_handshake(stream, handshake)
    }

    pub(crate) async fn accept(
        mut stream: S,
        psk: &NoisePsk,
        deadline: Instant,
    ) -> Result<Self, TransportError> {
        let mut preface = [0_u8; PREFACE.len()];
        read_exact_until(&mut stream, &mut preface, Some(deadline)).await?;
        if &preface != PREFACE {
            return Err(TransportError::InvalidPreface);
        }

        let mut handshake = handshake_state(psk, false)?;
        let request = read_frame(&mut stream, MAX_HANDSHAKE_MESSAGE, Some(deadline)).await?;
        handshake
            .read_message(&request, &mut [])
            .map_err(|_| TransportError::NoiseHandshake)?;
        let mut response = [0_u8; MAX_HANDSHAKE_MESSAGE];
        let written = handshake
            .write_message(&[], &mut response)
            .map_err(|_| TransportError::NoiseHandshake)?;
        write_frame(
            &mut stream,
            &response[..written],
            MAX_HANDSHAKE_MESSAGE,
            Some(deadline),
        )
        .await?;
        finish_handshake(stream, handshake)
    }

    pub(crate) fn handshake_hash(&self) -> &[u8; 32] {
        &self.handshake_hash
    }

    pub(crate) async fn write_envelope(
        &mut self,
        envelope: &SyncEnvelope,
        deadline: Option<Instant>,
    ) -> Result<(), TransportError> {
        if envelope.encoded_len() > MAX_PLAINTEXT {
            return Err(TransportError::EnvelopeTooLarge);
        }
        let plaintext = envelope.encode_to_vec();
        let mut ciphertext = vec![0_u8; plaintext.len() + NOISE_TAG_BYTES];
        let written = self
            .noise
            .write_message(&plaintext, &mut ciphertext)
            .map_err(|_| TransportError::NoiseTransport)?;
        ciphertext.truncate(written);
        write_frame(&mut self.stream, &ciphertext, MAX_CIPHERTEXT, deadline).await
    }

    pub(crate) async fn read_envelope(
        &mut self,
        deadline: Option<Instant>,
    ) -> Result<SyncEnvelope, TransportError> {
        let ciphertext = read_buffered_frame(
            &mut self.stream,
            &mut self.read_buffer,
            MAX_CIPHERTEXT,
            deadline,
        )
        .await?;
        let mut plaintext = vec![0_u8; ciphertext.len()];
        let read = self
            .noise
            .read_message(&ciphertext, &mut plaintext)
            .map_err(|_| TransportError::NoiseTransport)?;
        plaintext.truncate(read);
        SyncEnvelope::decode(plaintext.as_slice()).map_err(|_| TransportError::InvalidEnvelope)
    }

    pub(crate) async fn shutdown(&mut self) -> Result<(), TransportError> {
        self.stream
            .shutdown()
            .await
            .map_err(|error| TransportError::Io(error.kind()))
    }
}

fn handshake_state(psk: &NoisePsk, initiator: bool) -> Result<HandshakeState, TransportError> {
    let params = NOISE_PARAMS
        .parse::<NoiseParams>()
        .map_err(|_| TransportError::NoiseHandshake)?;
    let builder = Builder::new(params)
        .prologue(PREFACE)
        .map_err(|_| TransportError::NoiseHandshake)?
        .psk(0, psk.expose())
        .map_err(|_| TransportError::NoiseHandshake)?;
    if initiator {
        builder.build_initiator()
    } else {
        builder.build_responder()
    }
    .map_err(|_| TransportError::NoiseHandshake)
}

fn finish_handshake<S>(
    stream: S,
    handshake: HandshakeState,
) -> Result<NoiseTransport<S>, TransportError> {
    if !handshake.is_handshake_finished() {
        return Err(TransportError::NoiseHandshake);
    }
    let handshake_hash = handshake
        .get_handshake_hash()
        .try_into()
        .map_err(|_| TransportError::NoiseHandshake)?;
    let noise = handshake
        .into_transport_mode()
        .map_err(|_| TransportError::NoiseHandshake)?;
    Ok(NoiseTransport {
        stream,
        noise,
        handshake_hash,
        read_buffer: Vec::with_capacity(2),
    })
}

async fn read_buffered_frame<S>(
    stream: &mut S,
    buffered: &mut Vec<u8>,
    maximum: usize,
    deadline: Option<Instant>,
) -> Result<Vec<u8>, TransportError>
where
    S: AsyncRead + Unpin,
{
    read_at_least_until(stream, buffered, 2, deadline).await?;
    let length = usize::from(u16::from_be_bytes([buffered[0], buffered[1]]));
    if length == 0 || length > maximum {
        return Err(TransportError::InvalidFrameLength);
    }
    let frame_end = 2 + length;
    buffered.reserve(frame_end.saturating_sub(buffered.len()));
    read_at_least_until(stream, buffered, frame_end, deadline).await?;
    let frame = buffered[2..frame_end].to_vec();
    buffered.drain(..frame_end);
    Ok(frame)
}

async fn read_at_least_until<S>(
    stream: &mut S,
    buffered: &mut Vec<u8>,
    required: usize,
    deadline: Option<Instant>,
) -> Result<(), TransportError>
where
    S: AsyncRead + Unpin,
{
    let mut chunk = [0_u8; 8 * 1024];
    while buffered.len() < required {
        let remaining = required - buffered.len();
        let read_capacity = remaining.min(chunk.len());
        let output = &mut chunk[..read_capacity];
        let read = match deadline {
            Some(deadline) => timeout_at(deadline, stream.read(output))
                .await
                .map_err(|_| TransportError::DeadlineExceeded)?,
            None => stream.read(output).await,
        }
        .map_err(|error| TransportError::Io(error.kind()))?;
        if read == 0 {
            return Err(TransportError::Io(io::ErrorKind::UnexpectedEof));
        }
        buffered.extend_from_slice(&output[..read]);
    }
    Ok(())
}

async fn read_frame<S>(
    stream: &mut S,
    maximum: usize,
    deadline: Option<Instant>,
) -> Result<Vec<u8>, TransportError>
where
    S: AsyncRead + Unpin,
{
    let mut encoded_length = [0_u8; 2];
    read_exact_until(stream, &mut encoded_length, deadline).await?;
    let length = usize::from(u16::from_be_bytes(encoded_length));
    if length == 0 || length > maximum {
        return Err(TransportError::InvalidFrameLength);
    }
    let mut frame = vec![0_u8; length];
    read_exact_until(stream, &mut frame, deadline).await?;
    Ok(frame)
}

async fn write_frame<S>(
    stream: &mut S,
    frame: &[u8],
    maximum: usize,
    deadline: Option<Instant>,
) -> Result<(), TransportError>
where
    S: AsyncWrite + Unpin,
{
    if frame.is_empty() || frame.len() > maximum {
        return Err(TransportError::InvalidFrameLength);
    }
    let length = u16::try_from(frame.len()).map_err(|_| TransportError::InvalidFrameLength)?;
    let mut encoded = Vec::with_capacity(2 + frame.len());
    encoded.extend_from_slice(&length.to_be_bytes());
    encoded.extend_from_slice(frame);
    write_all_until(stream, &encoded, deadline).await
}

async fn read_exact_until<S>(
    stream: &mut S,
    output: &mut [u8],
    deadline: Option<Instant>,
) -> Result<(), TransportError>
where
    S: AsyncRead + Unpin,
{
    match deadline {
        Some(deadline) => timeout_at(deadline, stream.read_exact(output))
            .await
            .map_err(|_| TransportError::DeadlineExceeded)?
            .map(|_| ())
            .map_err(|error| TransportError::Io(error.kind())),
        None => stream
            .read_exact(output)
            .await
            .map(|_| ())
            .map_err(|error| TransportError::Io(error.kind())),
    }
}

async fn write_all_until<S>(
    stream: &mut S,
    bytes: &[u8],
    deadline: Option<Instant>,
) -> Result<(), TransportError>
where
    S: AsyncWrite + Unpin,
{
    match deadline {
        Some(deadline) => timeout_at(deadline, stream.write_all(bytes))
            .await
            .map_err(|_| TransportError::DeadlineExceeded)?
            .map_err(|error| TransportError::Io(error.kind())),
        None => stream
            .write_all(bytes)
            .await
            .map_err(|error| TransportError::Io(error.kind())),
    }
}

#[cfg(test)]
mod tests {
    use std::{
        pin::Pin,
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
        task::{Context, Poll},
    };

    use tokio::{
        io::{AsyncRead, AsyncWrite, AsyncWriteExt, ReadBuf, duplex},
        sync::Notify,
    };

    use crate::{configuration::NetworkKey, protocol::oll::sync_envelope};

    use super::*;
    use crate::sync::derive_noise_psk;

    fn psk(value: u8) -> NoisePsk {
        derive_noise_psk(&NetworkKey::new_for_test(vec![value; 32]))
    }

    struct ReadPause {
        reached: Notify,
        released: AtomicBool,
    }

    impl ReadPause {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                reached: Notify::new(),
                released: AtomicBool::new(false),
            })
        }

        fn release(&self) {
            self.released.store(true, Ordering::Release);
        }
    }

    struct PausingStream<S> {
        inner: S,
        bytes_before_pause: usize,
        pause: Arc<ReadPause>,
    }

    impl<S> AsyncRead for PausingStream<S>
    where
        S: AsyncRead + Unpin,
    {
        fn poll_read(
            self: Pin<&mut Self>,
            context: &mut Context<'_>,
            output: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            let this = self.get_mut();
            if this.bytes_before_pause == 0 && !this.pause.released.load(Ordering::Acquire) {
                this.pause.reached.notify_one();
                return Poll::Pending;
            }

            let maximum = if this.pause.released.load(Ordering::Acquire) {
                output.remaining()
            } else {
                output.remaining().min(this.bytes_before_pause)
            };
            let mut bytes = vec![0_u8; maximum];
            let mut limited = ReadBuf::new(&mut bytes);
            match Pin::new(&mut this.inner).poll_read(context, &mut limited) {
                Poll::Ready(Ok(())) => {
                    let read = limited.filled().len();
                    output.put_slice(limited.filled());
                    this.bytes_before_pause = this.bytes_before_pause.saturating_sub(read);
                    Poll::Ready(Ok(()))
                }
                Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
                Poll::Pending => Poll::Pending,
            }
        }
    }

    impl<S> AsyncWrite for PausingStream<S>
    where
        S: AsyncWrite + Unpin,
    {
        fn poll_write(
            self: Pin<&mut Self>,
            context: &mut Context<'_>,
            bytes: &[u8],
        ) -> Poll<io::Result<usize>> {
            Pin::new(&mut self.get_mut().inner).poll_write(context, bytes)
        }

        fn poll_flush(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
            Pin::new(&mut self.get_mut().inner).poll_flush(context)
        }

        fn poll_shutdown(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
            Pin::new(&mut self.get_mut().inner).poll_shutdown(context)
        }
    }

    async fn assert_read_envelope_is_cancellation_safe(bytes_before_pause: usize) {
        let (initiator_stream, responder_stream) = duplex(4096);
        let deadline = Instant::now() + HANDSHAKE_DEADLINE;
        let key = psk(9);
        let (initiator, responder) = tokio::join!(
            NoiseTransport::connect(initiator_stream, &key, deadline),
            NoiseTransport::accept(responder_stream, &key, deadline),
        );
        let mut initiator = initiator.unwrap();
        let NoiseTransport {
            stream,
            noise,
            handshake_hash,
            read_buffer,
        } = responder.unwrap();
        let pause = ReadPause::new();
        let mut responder = NoiseTransport {
            stream: PausingStream {
                inner: stream,
                bytes_before_pause,
                pause: Arc::clone(&pause),
            },
            noise,
            handshake_hash,
            read_buffer,
        };
        let envelope = SyncEnvelope {
            message_id: 41,
            reply_to: None,
            correlation_id: "cancel-safe-frame-test".to_owned(),
            payload: Some(sync_envelope::Payload::Close(
                crate::protocol::oll::SyncClose {
                    code: crate::protocol::oll::SyncCloseCode::Normal as i32,
                    message: String::new(),
                },
            )),
        };
        initiator.write_envelope(&envelope, None).await.unwrap();

        {
            let read = responder.read_envelope(None);
            tokio::pin!(read);
            tokio::select! {
                () = pause.reached.notified() => {}
                result = &mut read => panic!("frame read completed before the forced pause: {result:?}"),
            }
        }

        pause.release();
        assert_eq!(responder.read_envelope(None).await.unwrap(), envelope);
    }

    #[tokio::test]
    async fn exact_preface_noise_pattern_and_protobuf_frames_round_trip() {
        assert_eq!(PREFACE, b"OLLSYNC\x01");
        assert_eq!(NOISE_PARAMS, "Noise_NNpsk0_25519_ChaChaPoly_BLAKE2s");
        let (initiator_stream, responder_stream) = duplex(4096);
        let deadline = Instant::now() + HANDSHAKE_DEADLINE;
        let key = psk(7);
        let (initiator, responder) = tokio::join!(
            NoiseTransport::connect(initiator_stream, &key, deadline),
            NoiseTransport::accept(responder_stream, &key, deadline),
        );
        let mut initiator = initiator.unwrap();
        let mut responder = responder.unwrap();
        assert_eq!(responder.read_buffer.capacity(), 2);
        assert_eq!(initiator.handshake_hash(), responder.handshake_hash());

        let envelope = SyncEnvelope {
            message_id: 1,
            reply_to: None,
            correlation_id: "transport-test".to_owned(),
            payload: Some(sync_envelope::Payload::Close(
                crate::protocol::oll::SyncClose {
                    code: crate::protocol::oll::SyncCloseCode::Normal as i32,
                    message: String::new(),
                },
            )),
        };
        initiator.write_envelope(&envelope, None).await.unwrap();
        assert_eq!(responder.read_envelope(None).await.unwrap(), envelope);
    }

    #[tokio::test]
    async fn cancelling_after_a_partial_frame_header_preserves_the_next_read() {
        assert_read_envelope_is_cancellation_safe(1).await;
    }

    #[tokio::test]
    async fn cancelling_after_a_partial_frame_body_preserves_the_next_read() {
        assert_read_envelope_is_cancellation_safe(7).await;
    }

    #[tokio::test]
    async fn preface_and_handshake_lengths_are_rejected_before_body_allocation() {
        let (mut attacker, responder_stream) = duplex(64);
        let deadline = Instant::now() + HANDSHAKE_DEADLINE;
        attacker.write_all(PREFACE).await.unwrap();
        attacker
            .write_all(&(MAX_HANDSHAKE_MESSAGE as u16 + 1).to_be_bytes())
            .await
            .unwrap();
        let key = psk(1);
        assert!(matches!(
            NoiseTransport::accept(responder_stream, &key, deadline).await,
            Err(TransportError::InvalidFrameLength)
        ));

        let (mut attacker, responder_stream) = duplex(64);
        attacker.write_all(b"NOTSYNC!").await.unwrap();
        assert!(matches!(
            NoiseTransport::accept(responder_stream, &key, deadline).await,
            Err(TransportError::InvalidPreface)
        ));
    }

    #[tokio::test]
    async fn wrong_psk_fails_before_an_authenticated_application_frame_exists() {
        let (initiator_stream, responder_stream) = duplex(4096);
        let deadline = Instant::now() + HANDSHAKE_DEADLINE;
        let initiator_key = psk(1);
        let responder_key = psk(2);
        let (initiator, responder) = tokio::join!(
            NoiseTransport::connect(initiator_stream, &initiator_key, deadline),
            NoiseTransport::accept(responder_stream, &responder_key, deadline),
        );
        assert!(initiator.is_err());
        assert!(responder.is_err());
    }

    #[tokio::test(start_paused = true)]
    async fn every_handshake_step_uses_one_absolute_deadline() {
        let (mut silent_peer, responder_stream) = duplex(64);
        let deadline = Instant::now() + HANDSHAKE_DEADLINE;
        silent_peer.write_all(PREFACE).await.unwrap();
        let key = psk(1);
        let task =
            tokio::spawn(
                async move { NoiseTransport::accept(responder_stream, &key, deadline).await },
            );
        tokio::time::advance(HANDSHAKE_DEADLINE).await;
        assert!(matches!(
            task.await.unwrap(),
            Err(TransportError::DeadlineExceeded)
        ));
    }

    #[tokio::test]
    async fn local_envelope_size_is_checked_before_noise_or_io() {
        let (initiator_stream, responder_stream) = duplex(4096);
        let deadline = Instant::now() + HANDSHAKE_DEADLINE;
        let key = psk(3);
        let (initiator, responder) = tokio::join!(
            NoiseTransport::connect(initiator_stream, &key, deadline),
            NoiseTransport::accept(responder_stream, &key, deadline),
        );
        let mut initiator = initiator.unwrap();
        drop(responder.unwrap());
        let oversized = SyncEnvelope {
            message_id: 1,
            reply_to: None,
            correlation_id: "x".repeat(MAX_PLAINTEXT),
            payload: None,
        };
        assert_eq!(
            initiator
                .write_envelope(&oversized, None)
                .await
                .unwrap_err(),
            TransportError::EnvelopeTooLarge
        );
    }
}
