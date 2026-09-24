//! A gate every write asks, under the connection's writer lock, before its
//! first byte.
//!
//! A caller that paces its messages cannot count them on entry to a request:
//! the request may then wait for the writer's lock, and several requests
//! counted in different windows may reach the wire together. The gate is
//! asked at the write itself, with the writer's lock held and nothing yet
//! sent, so what it counts is what goes out.
//!
//! The gate never makes the writer wait while holding the lock. It answers
//! at once:
//!
//! - [`Admit::Write`]: write now.
//! - [`Admit::Refuse`]: do not write; the call fails with
//!   [`Error::Refused`](crate::Error::Refused), and no byte of it reached the
//!   socket.
//! - [`Admit::Later`]: not yet. The writer's lock is released, the call waits
//!   until `retry_at` or its deadline, whichever comes first, takes the lock
//!   again, and asks again. Other writes on the connection go ahead meanwhile.
//!
//! Each write is one [`WriteCall`], begun with [`WriteGate::begin`] before the
//! write waits for the writer's lock, kept across every release and retake of
//! it, and dropped when the write ends, however it ends: written, refused,
//! past its deadline, closed, or its future dropped while it waits. A gate
//! that queues writes keeps each one's place in its call, and its `Drop` is
//! where the place is given up.
//!
//! Each call has one [`Deadline`], asked of it once, before the write waits
//! for the writer's lock, and kept across every release and retake:
//!
//! - [`Deadline::At`]: waiting for the lock counts against it, and a call past
//!   it is refused without writing, even on its first try, so an expired
//!   deadline of a mutation is never overlooked;
//! - [`Deadline::NoWait`]: one try, now; if the lock is free, the gate is
//!   asked once and a `Later` answer is a refusal. Nothing is waited for.
//!
//! Async transport only.

use std::time::Instant;

pub use crate::errors::{GateReason, Refusal, Waiting};
use crate::messages::{OutgoingMessages, PROTOBUF_MSG_ID};

/// What a write is about to send, for a [`WriteGate`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct OutgoingMeta {
    /// The message's outgoing id, when the frame carries one.
    pub message: Option<OutgoingMessages>,
    /// The order id, for a placement or an order cancel.
    pub order_id: Option<i32>,
    /// The request id, when the request was sent with one.
    pub request_id: Option<i32>,
    /// Whether the write belongs to the connect handshake (the version
    /// prefix, `startApi`) rather than to the session.
    pub handshake: bool,
}

/// A [`WriteGate`]'s answer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Admit {
    /// Write now.
    Write,
    /// Do not write this message.
    Refuse(GateReason),
    /// Not yet: ask again at `retry_at`, or when the deadline comes.
    Later {
        /// When asking again may succeed.
        retry_at: Instant,
    },
}

/// How long a write may wait to go out.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Deadline {
    /// Until this instant, the wait for the writer's lock included. Past it,
    /// the write is refused without being tried.
    At(Instant),
    /// Not at all: one try, now, or a refusal.
    NoWait,
}

/// Decides, at each write, whether it may go out now.
pub trait WriteGate: Send + Sync + 'static {
    /// Begin the write described by `meta`. Called once per write, before it
    /// waits for the writer's lock.
    fn begin(&self, meta: &OutgoingMeta) -> Box<dyn WriteCall>;
}

/// One write's call to the gate, from its beginning to its end.
///
/// Every answer the write gets comes from this call, and it is dropped
/// exactly once, when the write ends, whatever ended it.
pub trait WriteCall: Send {
    /// The write's deadline. Asked once, before the write waits for the
    /// writer's lock.
    fn deadline(&self) -> Deadline;

    /// Whether the write may go out now. Called with the writer's lock held
    /// and before any byte: it must not block.
    fn admit(&mut self) -> Admit;

    /// A notification that wakes the write while it waits after `Later`,
    /// besides its `retry_at`, its deadline and the connection's close. Asked
    /// once, when the write begins. A gate that grants turns notifies with
    /// `notify_one`, whose permit is kept if the write is not waiting yet, so
    /// a wake between `Later` and the wait is never lost. A wake never moves
    /// the deadline. None, the default, is a write woken by time alone.
    fn wakeup(&self) -> Option<std::sync::Arc<tokio::sync::Notify>> {
        None
    }
}

/// Describe a message body (without its length prefix) for a gate.
///
/// A protobuf message starts with its id plus 200, big-endian; a text message
/// with its id in digits before the first NUL. The order id of a placement or
/// an order cancel is read from the protobuf request itself.
pub(crate) fn describe(body: &[u8], request_id: Option<i32>, handshake: bool) -> OutgoingMeta {
    let message = message_of(body);
    let order_id = match message {
        Some(OutgoingMessages::PlaceOrder) => proto_body(body).and_then(|proto| {
            use prost::Message as _;
            crate::proto::PlaceOrderRequest::decode(proto).ok().and_then(|request| request.order_id)
        }),
        Some(OutgoingMessages::CancelOrder) => proto_body(body).and_then(|proto| {
            use prost::Message as _;
            crate::proto::CancelOrderRequest::decode(proto).ok().and_then(|request| request.order_id)
        }),
        _ => None,
    };
    OutgoingMeta {
        message,
        order_id,
        request_id,
        handshake,
    }
}

/// The id of a protobuf message, if the body is one. Its big-endian prefix
/// is a small number, so its first byte is zero; a text message starts with
/// an ASCII digit.
fn proto_id(body: &[u8]) -> Option<i32> {
    let prefix: [u8; 4] = body.get(..4)?.try_into().ok()?;
    if prefix[0] != 0 {
        return None;
    }
    let raw = i32::from_be_bytes(prefix);
    (raw > PROTOBUF_MSG_ID).then(|| raw - PROTOBUF_MSG_ID)
}

fn proto_body(body: &[u8]) -> Option<&[u8]> {
    proto_id(body)?;
    body.get(4..)
}

fn message_of(body: &[u8]) -> Option<OutgoingMessages> {
    let id = match proto_id(body) {
        Some(id) => id,
        None => {
            let text = body.split(|byte| *byte == 0).next()?;
            std::str::from_utf8(text).ok()?.parse::<i32>().ok()?
        }
    };
    id.to_string().parse().ok()
}
