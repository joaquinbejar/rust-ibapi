//! Async stream abstraction for `AsyncConnection` / `AsyncTcpMessageBus`.
//!
//! Mirrors the sync `transport::sync::{Io, Reconnect, Stream}` triple, but
//! method-async via `#[async_trait]`. Frame-level: `read_message` returns the
//! already-unframed body so callers don't repeat the length-prefix dance.

use std::net::Shutdown;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;
use tokio::sync::{Mutex, Notify};

use super::super::write_gate::{Admit, Deadline, OutgoingMeta, Refusal, Waiting, WriteGate};
use crate::errors::Error;

#[async_trait]
pub(crate) trait AsyncIo {
    async fn read_message(&self) -> Result<Vec<u8>, Error>;
    async fn write_all(&self, buf: &[u8]) -> Result<(), Error>;
    /// [`AsyncIo::write_all`], for a write described by `meta`: a stream with
    /// a write gate asks it first (see `transport::write_gate`).
    async fn write_frame(&self, _meta: &OutgoingMeta, buf: &[u8]) -> Result<(), Error> {
        self.write_all(buf).await
    }
}

#[async_trait]
pub(crate) trait AsyncReconnect {
    async fn reconnect(&self) -> Result<(), Error>;
    async fn sleep(&self, duration: Duration);
    /// Close the stream for real, at once and for good: refuse every write
    /// from now on, and shut down both directions of the socket without
    /// waiting for a read or a write in progress. Idempotent.
    fn shutdown(&self);
}

pub(crate) trait AsyncStream: AsyncIo + AsyncReconnect + Send + Sync + 'static + std::fmt::Debug {}

/// Production async stream over `tokio::net::TcpStream`. Holds the split halves
/// behind `Mutex` so reads and writes can run concurrently from the dispatcher
/// task and the request senders.
///
/// # Closing
///
/// `write_all` holds the writer's lock across the write and the flush, so a
/// close that waited for that lock could hang behind a write blocked by
/// backpressure. The close therefore goes through `closer`, a second handle
/// on the same socket taken at connect, independent of both locks: it shuts
/// down both directions, which fails a blocked write and ends a blocked read.
/// A write checks `closed` before taking the writer's lock and again after,
/// so a write that had not started when the close began, queued writers
/// included, is refused with [`Error::Closed`] without touching the socket.
/// Bytes the kernel accepted before the close are not undone.
#[derive(Debug)]
pub(crate) struct AsyncTcpSocket {
    reader: Mutex<OwnedReadHalf>,
    writer: Mutex<OwnedWriteHalf>,
    closer: std::sync::Mutex<std::net::TcpStream>,
    closed: AtomicBool,
    /// Wakes a write waiting on its gate when the socket closes.
    closing: Notify,
    gate: Option<Arc<dyn WriteGate>>,
    connection_url: String,
    tcp_no_delay: bool,
}

impl std::fmt::Debug for dyn WriteGate {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("WriteGate")
    }
}

/// Connect, and take the independent handle the close goes through.
async fn open(address: &str, tcp_no_delay: bool) -> Result<(OwnedReadHalf, OwnedWriteHalf, std::net::TcpStream), Error> {
    let stream = TcpStream::connect(address).await?;
    stream.set_nodelay(tcp_no_delay)?;
    // A duplicate of the socket's descriptor: shutting it down shuts down the
    // socket itself, and it is reachable without the halves' locks.
    let stream = stream.into_std()?;
    let closer = stream.try_clone()?;
    let (read_half, write_half) = TcpStream::from_std(stream)?.into_split();
    Ok((read_half, write_half, closer))
}

impl AsyncTcpSocket {
    pub async fn connect(address: &str, tcp_no_delay: bool, gate: Option<Arc<dyn WriteGate>>) -> Result<Self, Error> {
        let (read_half, write_half, closer) = open(address, tcp_no_delay).await?;
        Ok(Self {
            reader: Mutex::new(read_half),
            writer: Mutex::new(write_half),
            closer: std::sync::Mutex::new(closer),
            closed: AtomicBool::new(false),
            closing: Notify::new(),
            gate,
            connection_url: address.to_string(),
            tcp_no_delay,
        })
    }

    fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }
}

#[async_trait]
impl AsyncIo for AsyncTcpSocket {
    async fn read_message(&self) -> Result<Vec<u8>, Error> {
        let mut reader = self.reader.lock().await;
        let mut length_bytes = [0u8; 4];
        reader.read_exact(&mut length_bytes).await?;
        let message_length = u32::from_be_bytes(length_bytes) as usize;
        let mut data = vec![0u8; message_length];
        reader.read_exact(&mut data).await?;
        Ok(data)
    }

    async fn write_all(&self, buf: &[u8]) -> Result<(), Error> {
        if self.is_closed() {
            return Err(Error::Closed);
        }
        let mut writer = self.writer.lock().await;
        // A writer that queued on the lock before the close began is refused
        // here, still without a byte handed to the socket.
        if self.is_closed() {
            return Err(Error::Closed);
        }
        writer.write_all(buf).await?;
        writer.flush().await?;
        Ok(())
    }

    async fn write_frame(&self, meta: &OutgoingMeta, buf: &[u8]) -> Result<(), Error> {
        let Some(gate) = self.gate.as_ref() else {
            return self.write_all(buf).await;
        };
        // One call for the whole write, begun before the wait for the
        // writer's lock and kept across every retry; dropped on every exit,
        // the drop of this future included. Its deadline is asked once.
        let mut call = gate.begin(meta);
        let deadline = call.deadline();
        // What the write was last waiting for, so a deadline that passes is
        // named by the wait it ended, across every release and retake.
        let mut waiting = Waiting::Start;
        loop {
            // Armed before the closed flag is read, so a close in between
            // still wakes the wait below.
            let closing = self.closing.notified();
            tokio::pin!(closing);
            closing.as_mut().enable();
            if self.is_closed() {
                return Err(Error::Closed);
            }
            let mut writer = match deadline {
                // One try, now: the lock free and the gate willing, or nothing.
                Deadline::NoWait => match self.writer.try_lock() {
                    Ok(writer) => writer,
                    Err(_) => return Err(Error::Refused(Refusal::Busy)),
                },
                // A deadline that has passed is never tried, first attempt
                // included; the wait for the writer's lock counts against it.
                Deadline::At(at) => {
                    if Instant::now() >= at {
                        return Err(Error::Refused(Refusal::Expired { waiting_for: waiting }));
                    }
                    waiting = Waiting::Writer;
                    match tokio::time::timeout_at(at.into(), self.writer.lock()).await {
                        Ok(writer) => writer,
                        Err(_) => return Err(Error::Refused(Refusal::Expired { waiting_for: waiting })),
                    }
                }
            };
            // The close may have begun while this waited for the lock.
            if self.is_closed() {
                return Err(Error::Closed);
            }
            if let Deadline::At(at) = deadline {
                if Instant::now() >= at {
                    return Err(Error::Refused(Refusal::Expired { waiting_for: waiting }));
                }
            }
            match call.admit() {
                Admit::Write => {
                    writer.write_all(buf).await?;
                    writer.flush().await?;
                    return Ok(());
                }
                Admit::Refuse(reason) => return Err(Error::Refused(Refusal::Gate(reason))),
                Admit::Later { retry_at } => {
                    // Never wait holding the writer's lock: other writes on
                    // this connection go ahead meanwhile.
                    drop(writer);
                    let Deadline::At(at) = deadline else {
                        return Err(Error::Refused(Refusal::NoSlot));
                    };
                    waiting = Waiting::Slot;
                    if Instant::now() >= at {
                        return Err(Error::Refused(Refusal::Expired { waiting_for: waiting }));
                    }
                    let until = retry_at.min(at);
                    tokio::select! {
                        () = tokio::time::sleep_until(until.into()) => {}
                        () = &mut closing => {}
                    }
                }
            }
        }
    }
}

#[async_trait]
impl AsyncReconnect for AsyncTcpSocket {
    async fn reconnect(&self) -> Result<(), Error> {
        if self.is_closed() {
            return Err(Error::Closed);
        }
        let (new_reader, new_writer, new_closer) = open(&self.connection_url, self.tcp_no_delay).await?;
        *self.reader.lock().await = new_reader;
        *self.writer.lock().await = new_writer;
        *self.closer.lock()? = new_closer;
        // A close that ran while this reconnected shut down the old socket:
        // do the same to the new one rather than bring it to life.
        if self.is_closed() {
            self.close_socket();
            return Err(Error::Closed);
        }
        Ok(())
    }

    async fn sleep(&self, duration: Duration) {
        tokio::time::sleep(duration).await
    }

    fn shutdown(&self) {
        self.closed.store(true, Ordering::Release);
        self.close_socket();
        // A write waiting on its gate learns of the close at once.
        self.closing.notify_waiters();
    }
}

impl AsyncTcpSocket {
    fn close_socket(&self) {
        let Ok(closer) = self.closer.lock() else {
            return;
        };
        // An error here means the socket is already shut down or gone.
        if let Err(e) = closer.shutdown(Shutdown::Both) {
            log::debug!("closing the socket: {e}");
        }
    }
}

impl AsyncStream for AsyncTcpSocket {}
