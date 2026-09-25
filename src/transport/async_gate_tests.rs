//! The write gate against real sockets: `Later` releases the writer, a call
//! keeps one deadline, a refusal hands no byte to the socket, a shutdown ends
//! a wait at once, and what the gate is told matches the frame.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use prost::Message as _;
use tokio::io::AsyncReadExt;
use tokio::net::{TcpListener, TcpStream};

use super::io::{AsyncIo, AsyncReconnect, AsyncTcpSocket};
use crate::messages::{encode_protobuf_message, encode_raw_length, OutgoingMessages};
use crate::transport::write_gate::{describe, Admit, Deadline, GateReason, OutgoingMeta, Refusal, Waiting, WriteCall, WriteGate};
use crate::Error;

async fn pair(gate: Arc<dyn WriteGate>) -> (Arc<AsyncTcpSocket>, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let address = listener.local_addr().expect("local address").to_string();
    let (socket, accepted) = tokio::join!(AsyncTcpSocket::connect(&address, true, Some(gate)), listener.accept());
    (Arc::new(socket.expect("connects")), accepted.expect("accepts").0)
}

fn place(order_id: i32) -> Vec<u8> {
    let request = crate::proto::PlaceOrderRequest {
        order_id: Some(order_id),
        ..Default::default()
    };
    encode_protobuf_message(OutgoingMessages::PlaceOrder as i32, &request.encode_to_vec())
}

fn cancel(order_id: i32) -> Vec<u8> {
    let request = crate::proto::CancelOrderRequest {
        order_id: Some(order_id),
        ..Default::default()
    };
    encode_protobuf_message(OutgoingMessages::CancelOrder as i32, &request.encode_to_vec())
}

/// Write one body through the socket's gate, as the connection does.
async fn write(socket: &AsyncTcpSocket, body: &[u8]) -> Result<(), Error> {
    let meta = describe(body, None, false);
    socket.write_frame(&meta, &encode_raw_length(body)).await
}

/// Every frame the peer receives until the connection ends, as bodies.
async fn frames(peer: &mut TcpStream) -> Vec<Vec<u8>> {
    let mut all = Vec::new();
    let _ = tokio::time::timeout(Duration::from_secs(5), peer.read_to_end(&mut all)).await;
    let mut bodies = Vec::new();
    let mut rest = all.as_slice();
    while rest.len() >= 4 {
        let length = u32::from_be_bytes([rest[0], rest[1], rest[2], rest[3]]) as usize;
        let Some(body) = rest.get(4..4 + length) else { break };
        bodies.push(body.to_vec());
        rest = &rest[4 + length..];
    }
    bodies
}

/// A gate that answers from the message alone. [`Calls`] makes it a
/// [`WriteGate`], one [`WriteCall`] per write, counting how many were begun
/// and how many ended.
pub(super) trait SimpleGate: Send + Sync + 'static {
    fn deadline(&self, meta: &OutgoingMeta) -> Deadline;
    fn admit(&self, meta: &OutgoingMeta) -> Admit;
}

pub(super) struct Calls<G> {
    gate: Arc<G>,
    begun: Arc<AtomicUsize>,
    ended: Arc<AtomicUsize>,
}

impl<G: SimpleGate> Calls<G> {
    pub(super) fn new(gate: &Arc<G>) -> Arc<Self> {
        Arc::new(Self {
            gate: Arc::clone(gate),
            begun: Arc::new(AtomicUsize::new(0)),
            ended: Arc::new(AtomicUsize::new(0)),
        })
    }
    fn begun(&self) -> usize {
        self.begun.load(Ordering::SeqCst)
    }
    fn ended(&self) -> usize {
        self.ended.load(Ordering::SeqCst)
    }
}

struct Call<G> {
    gate: Arc<G>,
    meta: OutgoingMeta,
    admits: usize,
    ended: Arc<AtomicUsize>,
}

impl<G: SimpleGate> WriteGate for Calls<G> {
    fn begin(&self, meta: &OutgoingMeta) -> Box<dyn WriteCall> {
        self.begun.fetch_add(1, Ordering::SeqCst);
        Box::new(Call {
            gate: Arc::clone(&self.gate),
            meta: *meta,
            admits: 0,
            ended: Arc::clone(&self.ended),
        })
    }
}

impl<G: SimpleGate> WriteCall for Call<G> {
    fn deadline(&self) -> Deadline {
        self.gate.deadline(&self.meta)
    }
    fn admit(&mut self) -> Admit {
        self.admits += 1;
        self.gate.admit(&self.meta)
    }
}

impl<G> Drop for Call<G> {
    fn drop(&mut self) {
        self.ended.fetch_add(1, Ordering::SeqCst);
    }
}

/// A gate as the SDK sees it, one call per write.
pub(super) fn gated<G: SimpleGate>(gate: &Arc<G>) -> Arc<dyn WriteGate> {
    Calls::new(gate)
}

/// A gate driven by closures, counting its calls.
struct Scripted {
    deadline: Box<dyn Fn(&OutgoingMeta) -> Deadline + Send + Sync>,
    admit: Box<dyn Fn(&OutgoingMeta) -> Admit + Send + Sync>,
    deadlines: AtomicUsize,
    admits: AtomicUsize,
}

impl Scripted {
    fn new(
        deadline: impl Fn(&OutgoingMeta) -> Deadline + Send + Sync + 'static,
        admit: impl Fn(&OutgoingMeta) -> Admit + Send + Sync + 'static,
    ) -> Arc<Self> {
        Arc::new(Self {
            deadline: Box::new(deadline),
            admit: Box::new(admit),
            deadlines: AtomicUsize::new(0),
            admits: AtomicUsize::new(0),
        })
    }
}

impl SimpleGate for Scripted {
    fn deadline(&self, meta: &OutgoingMeta) -> Deadline {
        self.deadlines.fetch_add(1, Ordering::SeqCst);
        (self.deadline)(meta)
    }
    fn admit(&self, meta: &OutgoingMeta) -> Admit {
        self.admits.fetch_add(1, Ordering::SeqCst);
        (self.admit)(meta)
    }
}

fn soon() -> Deadline {
    Deadline::At(Instant::now() + Duration::from_secs(10))
}

fn in_ms(ms: u64) -> Deadline {
    Deadline::At(Instant::now() + Duration::from_millis(ms))
}

#[tokio::test]
async fn later_releases_the_writer_so_another_write_goes_first() {
    // A placement is held (`Later`) until a cancel has been written; the
    // cancel must be able to take the writer's lock meanwhile.
    let cancelled = Arc::new(AtomicBool::new(false));
    let seen = Arc::clone(&cancelled);
    let gate = Scripted::new(
        |_| soon(),
        move |meta| match meta.message {
            Some(OutgoingMessages::CancelOrder) => {
                seen.store(true, Ordering::SeqCst);
                Admit::Write
            }
            _ if seen.load(Ordering::SeqCst) => Admit::Write,
            // A long retry: were the lock held through the wait, the cancel
            // below could not be written for two seconds.
            _ => Admit::Later {
                retry_at: Instant::now() + Duration::from_secs(2),
            },
        },
    );
    let (socket, mut peer) = pair(gated(&gate)).await;
    let placing = tokio::spawn({
        let socket = Arc::clone(&socket);
        async move { write(&socket, &place(7)).await }
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    tokio::time::timeout(Duration::from_secs(1), write(&socket, &cancel(7)))
        .await
        .expect("the cancel is not held behind the waiting placement")
        .expect("the cancel is written");
    tokio::time::timeout(Duration::from_secs(4), placing)
        .await
        .expect("the placement is retried")
        .expect("joins")
        .expect("the placement is written after it");
    socket.shutdown();
    let bodies = frames(&mut peer).await;
    assert_eq!(bodies, vec![cancel(7), place(7)]);
    assert!(cancelled.load(Ordering::SeqCst));
}

#[tokio::test]
async fn a_call_keeps_one_deadline_across_every_retry() {
    let gate = Scripted::new(
        |_| in_ms(300),
        |_| Admit::Later {
            retry_at: Instant::now() + Duration::from_millis(20),
        },
    );
    let (socket, mut peer) = pair(gated(&gate)).await;
    let started = Instant::now();
    let outcome = write(&socket, &place(8)).await;
    let elapsed = started.elapsed();
    assert!(
        matches!(outcome, Err(Error::Refused(Refusal::Expired { waiting_for: Waiting::Slot }))),
        "{outcome:?}"
    );
    assert!(elapsed >= Duration::from_millis(280), "{elapsed:?}");
    assert!(elapsed < Duration::from_millis(900), "the deadline was extended: {elapsed:?}");
    assert_eq!(gate.deadlines.load(Ordering::SeqCst), 1, "a new deadline was asked for");
    assert!(gate.admits.load(Ordering::SeqCst) > 1);
    socket.shutdown();
    assert!(frames(&mut peer).await.is_empty());
}

#[tokio::test]
async fn a_refusal_hands_no_byte_to_the_socket() {
    let gate = Scripted::new(|_| soon(), |_| Admit::Refuse(GateReason { code: 1, text: "not now" }));
    let (socket, mut peer) = pair(gated(&gate)).await;
    let outcome = write(&socket, &place(9)).await;
    assert!(
        matches!(outcome, Err(Error::Refused(Refusal::Gate(GateReason { code: 1, .. })))),
        "{outcome:?}"
    );
    socket.shutdown();
    assert!(frames(&mut peer).await.is_empty(), "a refused write reached the peer");
}

#[tokio::test]
async fn a_shutdown_ends_a_wait_at_once() {
    let gate = Scripted::new(
        |_| in_ms(60_000),
        |_| Admit::Later {
            retry_at: Instant::now() + Duration::from_secs(60),
        },
    );
    let (socket, mut peer) = pair(gated(&gate)).await;
    let waiting = tokio::spawn({
        let socket = Arc::clone(&socket);
        async move { write(&socket, &place(10)).await }
    });
    tokio::time::sleep(Duration::from_millis(200)).await;
    socket.shutdown();
    let outcome = tokio::time::timeout(Duration::from_secs(1), waiting)
        .await
        .expect("the shutdown ended the wait")
        .expect("joins");
    assert!(matches!(outcome, Err(Error::Closed)), "{outcome:?}");
    assert!(frames(&mut peer).await.is_empty());
}

#[tokio::test]
async fn the_deadline_covers_the_wait_for_the_writer() {
    // The first write fills every buffer and holds the writer's lock; the
    // second's deadline passes while it waits for that lock.
    let gate = Scripted::new(
        |meta| match meta.message {
            Some(OutgoingMessages::CancelOrder) => in_ms(300),
            _ => soon(),
        },
        |_| Admit::Write,
    );
    let (socket, mut peer) = pair(gated(&gate)).await;
    let big = tokio::spawn({
        let socket = Arc::clone(&socket);
        async move {
            let body = vec![0_u8; 64 << 20];
            let meta = describe(&body, None, false);
            socket.write_frame(&meta, &encode_raw_length(&body)).await
        }
    });
    tokio::time::sleep(Duration::from_millis(300)).await;
    let admits_before = gate.admits.load(Ordering::SeqCst);
    let started = Instant::now();
    // Bounded here too: a wait for the lock that ignored the deadline would
    // otherwise hang the test rather than fail it.
    let outcome = tokio::time::timeout(Duration::from_secs(2), write(&socket, &cancel(11)))
        .await
        .expect("the wait for the writer is bounded by the call's deadline");
    assert!(
        matches!(
            outcome,
            Err(Error::Refused(Refusal::Expired {
                waiting_for: Waiting::Writer
            }))
        ),
        "{outcome:?}"
    );
    assert!(started.elapsed() < Duration::from_millis(900), "{:?}", started.elapsed());
    assert_eq!(
        gate.admits.load(Ordering::SeqCst),
        admits_before,
        "the gate was asked without the writer's lock"
    );
    socket.shutdown();
    let _ = big.await;
    let bodies = frames(&mut peer).await;
    assert!(!bodies.contains(&cancel(11)), "the refused cancel reached the peer");
}

#[tokio::test]
async fn no_wait_is_one_try_now() {
    // Market data's "now or never": the gate is asked once, and a slot means
    // a write.
    let gate = Scripted::new(|_| Deadline::NoWait, |_| Admit::Write);
    let (socket, mut peer) = pair(gated(&gate)).await;
    write(&socket, &place(12)).await.expect("written at once");

    // And `Later` is not waited for.
    let held = Scripted::new(
        |_| Deadline::NoWait,
        |_| Admit::Later {
            retry_at: Instant::now() + Duration::from_secs(5),
        },
    );
    let (other, mut other_peer) = pair(gated(&held)).await;
    let started = Instant::now();
    let refused = tokio::time::timeout(Duration::from_secs(1), write(&other, &place(13)))
        .await
        .expect("a write that does not wait is not held");
    assert!(matches!(refused, Err(Error::Refused(Refusal::NoSlot))), "{refused:?}");
    assert!(started.elapsed() < Duration::from_millis(200), "{:?}", started.elapsed());
    assert_eq!(held.admits.load(Ordering::SeqCst), 1);

    socket.shutdown();
    other.shutdown();
    assert_eq!(frames(&mut peer).await, vec![place(12)]);
    assert!(frames(&mut other_peer).await.is_empty());
}

#[tokio::test]
async fn an_expired_deadline_is_never_tried_even_first() {
    // A mutation whose deadline already passed is refused without the gate
    // being asked, even though the writer is free: NoWait's "one try" is not
    // a way round an expired deadline (ACS 1712).
    let gate = Scripted::new(|_| Deadline::At(Instant::now() - Duration::from_millis(1)), |_| Admit::Write);
    let (socket, mut peer) = pair(gated(&gate)).await;
    let expired = write(&socket, &place(14)).await;
    assert!(
        matches!(expired, Err(Error::Refused(Refusal::Expired { waiting_for: Waiting::Start }))),
        "{expired:?}"
    );
    assert_eq!(gate.admits.load(Ordering::SeqCst), 0, "an expired write was tried");
    socket.shutdown();
    assert!(frames(&mut peer).await.is_empty());
}

#[test]
fn the_gate_is_told_the_message_and_its_order_id() {
    // Protobuf frames: the id is the prefix less 200; the order id is read
    // from the request itself.
    let placed = describe(&place(4242), None, false);
    assert_eq!(placed.message, Some(OutgoingMessages::PlaceOrder));
    assert_eq!(placed.order_id, Some(4242));
    let cancelled = describe(&cancel(4243), None, false);
    assert_eq!(cancelled.message, Some(OutgoingMessages::CancelOrder));
    assert_eq!(cancelled.order_id, Some(4243));

    // A protobuf request that is not an order carries no order id, and keeps
    // the request id it was sent with.
    let time = encode_protobuf_message(OutgoingMessages::RequestCurrentTime as i32, &[]);
    let probe = describe(&time, Some(17), false);
    assert_eq!(probe.message, Some(OutgoingMessages::RequestCurrentTime));
    assert_eq!(probe.order_id, None);
    assert_eq!(probe.request_id, Some(17));

    // A text frame (the older encoding): the id before the first NUL.
    let text = describe(b"49\x001\x00", None, false);
    assert_eq!(text.message, Some(OutgoingMessages::RequestCurrentTime));
    let md = describe(b"1\x0011\x00", Some(3), false);
    assert_eq!(md.message, Some(OutgoingMessages::RequestMarketData));

    // A legacy message with a binary id: the account summary cancel on
    // server 221.
    let legacy = describe(b"\x00\x00\x00\x3f1\x0060002\x00", Some(60002), false);
    assert_eq!(legacy.message, Some(OutgoingMessages::CancelAccountSummary));
    assert_eq!(legacy.request_id, Some(60002));

    // The handshake, and a frame with no recognisable id.
    let handshake = describe(&[], None, true);
    assert!(handshake.handshake);
    assert_eq!(handshake.message, None);
    assert_eq!(describe(b"API\x00", None, true).message, None);
}

#[test]
fn a_gate_is_asked_under_a_lock_it_never_waits_on() {
    // A compile-time reminder of the contract: `admit` is synchronous, so the
    // writer's lock can never be held across a wait inside it.
    fn assert_sync_admit(call: &mut dyn WriteCall) -> Admit {
        call.admit()
    }
    let gate = gated(&Scripted::new(|_| soon(), |_| Admit::Write));
    let mut call = gate.begin(&describe(&place(1), None, false));
    let recorded = Mutex::new(());
    let _guard = recorded.lock().expect("not poisoned");
    assert_eq!(assert_sync_admit(call.as_mut()), Admit::Write);
}

#[tokio::test]
async fn a_write_is_one_call_asked_again_across_every_retry() {
    // Three `Later`s, then a slot: one call is begun for the write, all four
    // answers come from it, and it ends once, when the write does.
    let asked = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&asked);
    let gate = Scripted::new(
        |_| soon(),
        move |_| {
            if counter.fetch_add(1, Ordering::SeqCst) < 3 {
                Admit::Later {
                    retry_at: Instant::now() + Duration::from_millis(10),
                }
            } else {
                Admit::Write
            }
        },
    );
    let calls = Calls::new(&gate);
    let (socket, mut peer) = pair(calls.clone()).await;
    write(&socket, b"49\x001\x00").await.expect("written");
    assert_eq!(asked.load(Ordering::SeqCst), 4);
    assert_eq!(calls.begun(), 1, "a call was begun per answer, not per write");
    assert_eq!(calls.ended(), 1);
    socket.shutdown();
    assert_eq!(frames(&mut peer).await.len(), 1);
}

#[tokio::test]
async fn a_call_ends_once_however_its_write_ends() {
    // Every way a write can end, including its future dropped while it
    // waits: a gate that queues writes gives up the place in its call's
    // drop, so a call that never ends would hold its place forever.
    async fn ends(admit: fn() -> Admit, deadline: fn() -> Deadline, how: &str, run: impl FnOnce(Arc<AsyncTcpSocket>) -> tokio::task::JoinHandle<()>) {
        let gate = Scripted::new(move |_| deadline(), move |_| admit());
        let calls = Calls::new(&gate);
        let (socket, _peer) = pair(calls.clone()).await;
        let handle = run(Arc::clone(&socket));
        // The task's own assertions (the close, the abort) must not be lost:
        // a panic in it fails here, not only a wrong count.
        tokio::time::timeout(Duration::from_secs(3), handle)
            .await
            .expect("the write ends")
            .expect("task did not panic");
        assert_eq!((calls.begun(), calls.ended()), (1, 1), "{how}");
    }
    fn later() -> Admit {
        Admit::Later {
            retry_at: Instant::now() + Duration::from_secs(60),
        }
    }
    let once = |socket: Arc<AsyncTcpSocket>| {
        tokio::spawn(async move {
            let _ = write(&socket, b"49\x001\x00").await;
        })
    };
    ends(|| Admit::Write, soon, "written", once).await;
    ends(|| Admit::Refuse(GateReason { code: 1, text: "test" }), soon, "refused", once).await;
    ends(later, || in_ms(200), "past its deadline", once).await;
    ends(later, || Deadline::NoWait, "no wait", once).await;
    ends(later, soon, "closed while waiting", |socket: Arc<AsyncTcpSocket>| {
        tokio::spawn(async move {
            let closer = Arc::clone(&socket);
            let writing = tokio::spawn(async move { write(&socket, b"49\x001\x00").await });
            tokio::time::sleep(Duration::from_millis(100)).await;
            closer.shutdown();
            let written = writing.await.expect("joins");
            assert!(matches!(written, Err(Error::Closed)), "{written:?}");
        })
    })
    .await;
    ends(later, soon, "dropped while waiting", |socket: Arc<AsyncTcpSocket>| {
        tokio::spawn(async move {
            let writing = tokio::spawn(async move { write(&socket, b"49\x001\x00").await });
            tokio::time::sleep(Duration::from_millis(100)).await;
            writing.abort();
            assert!(writing.await.expect_err("aborted").is_cancelled());
        })
    })
    .await;
}

/// A write of 64 MiB the peer does not read: it fills every buffer and holds
/// the writer's lock until the peer reads.
fn hold_the_writer(socket: &Arc<AsyncTcpSocket>) -> tokio::task::JoinHandle<Result<(), Error>> {
    let socket = Arc::clone(socket);
    tokio::spawn(async move {
        let body = vec![0_u8; 64 << 20];
        let meta = describe(&body, None, false);
        socket.write_frame(&meta, &encode_raw_length(&body)).await
    })
}

#[tokio::test]
async fn a_write_that_does_not_wait_is_refused_as_busy_when_the_writer_is_held() {
    let gate = Scripted::new(
        |meta| match meta.message {
            Some(OutgoingMessages::RequestMarketData) => Deadline::NoWait,
            _ => soon(),
        },
        |_| Admit::Write,
    );
    let (socket, _peer) = pair(gated(&gate)).await;
    let _big = hold_the_writer(&socket);
    tokio::time::sleep(Duration::from_millis(300)).await;
    let busy = write(&socket, b"1\x0011\x00").await;
    assert!(matches!(busy, Err(Error::Refused(Refusal::Busy))), "{busy:?}");
    socket.shutdown();
}

#[tokio::test]
async fn an_expired_write_names_the_wait_it_ended_after_a_slot_wait() {
    // First a `Later`; while it waits for a slot, another write takes the
    // writer's lock and keeps it. The deadline passes waiting for the
    // writer, not for the slot it waited for before.
    let asked = Arc::new(AtomicUsize::new(0));
    let first = Arc::clone(&asked);
    let gate = Scripted::new(
        |meta| match meta.order_id {
            Some(20) => in_ms(600),
            _ => soon(),
        },
        move |meta| {
            if meta.order_id == Some(20) && first.fetch_add(1, Ordering::SeqCst) == 0 {
                Admit::Later {
                    retry_at: Instant::now() + Duration::from_millis(200),
                }
            } else {
                Admit::Write
            }
        },
    );
    let (socket, _peer) = pair(gated(&gate)).await;
    let writing = tokio::spawn({
        let socket = Arc::clone(&socket);
        async move { write(&socket, &place(20)).await }
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    let _big = hold_the_writer(&socket);
    let expired = tokio::time::timeout(Duration::from_secs(3), writing)
        .await
        .expect("bounded by its deadline")
        .expect("joins");
    assert!(
        matches!(
            expired,
            Err(Error::Refused(Refusal::Expired {
                waiting_for: Waiting::Writer
            }))
        ),
        "{expired:?}"
    );
    socket.shutdown();
}

#[tokio::test]
async fn an_expired_write_names_the_wait_it_ended_after_a_writer_wait() {
    // First the writer's lock, held by a write the peer then drains; then
    // `Later` with no slot before the deadline. The deadline passes waiting
    // for a slot, not for the writer it waited for before.
    let gate = Scripted::new(
        |meta| match meta.order_id {
            Some(21) => in_ms(1500),
            _ => soon(),
        },
        |meta| {
            if meta.order_id == Some(21) {
                Admit::Later {
                    retry_at: Instant::now() + Duration::from_secs(60),
                }
            } else {
                Admit::Write
            }
        },
    );
    let (socket, mut peer) = pair(gated(&gate)).await;
    let big = hold_the_writer(&socket);
    tokio::time::sleep(Duration::from_millis(200)).await;
    let writing = tokio::spawn({
        let socket = Arc::clone(&socket);
        async move { write(&socket, &place(21)).await }
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    // The peer reads: the big write finishes and releases the writer.
    let mut sink = vec![0_u8; 1 << 20];
    let mut drained = 0_usize;
    while drained < (64 << 20) {
        match tokio::time::timeout(Duration::from_secs(5), peer.read(&mut sink)).await {
            Ok(Ok(0)) | Ok(Err(_)) | Err(_) => break,
            Ok(Ok(n)) => drained += n,
        }
    }
    big.await.expect("joins").expect("the big write went out");
    let expired = tokio::time::timeout(Duration::from_secs(3), writing)
        .await
        .expect("bounded by its deadline")
        .expect("joins");
    assert!(
        matches!(expired, Err(Error::Refused(Refusal::Expired { waiting_for: Waiting::Slot }))),
        "{expired:?}"
    );
    socket.shutdown();
}

/// A gate whose calls answer `Later` (retry far away) until `open` is set,
/// and hand the fork a wake the test controls.
struct Woken {
    open: Arc<AtomicBool>,
    notify: Arc<tokio::sync::Notify>,
    admits: Arc<AtomicUsize>,
    with_wake: bool,
    deadline_ms: u64,
    // Notify from inside `admit`, before the write waits: the permit must
    // be kept.
    notify_in_admit: bool,
}

struct WokenCall {
    open: Arc<AtomicBool>,
    notify: Arc<tokio::sync::Notify>,
    admits: Arc<AtomicUsize>,
    with_wake: bool,
    deadline: Deadline,
    notify_in_admit: bool,
}

impl WriteGate for Woken {
    fn begin(&self, _meta: &OutgoingMeta) -> Box<dyn WriteCall> {
        Box::new(WokenCall {
            open: Arc::clone(&self.open),
            notify: Arc::clone(&self.notify),
            admits: Arc::clone(&self.admits),
            with_wake: self.with_wake,
            deadline: in_ms(self.deadline_ms),
            notify_in_admit: self.notify_in_admit,
        })
    }
}

impl WriteCall for WokenCall {
    fn deadline(&self) -> Deadline {
        self.deadline
    }
    fn admit(&mut self) -> Admit {
        let n = self.admits.fetch_add(1, Ordering::SeqCst);
        if self.open.load(Ordering::SeqCst) {
            return Admit::Write;
        }
        if self.notify_in_admit && n == 0 {
            // The turn is granted while the write still holds the lock and
            // has not begun to wait.
            self.open.store(true, Ordering::SeqCst);
            self.notify.notify_one();
        }
        Admit::Later {
            retry_at: Instant::now() + Duration::from_secs(60),
        }
    }
    fn wakeup(&self) -> Option<Arc<tokio::sync::Notify>> {
        self.with_wake.then(|| Arc::clone(&self.notify))
    }
}

fn woken(with_wake: bool, deadline_ms: u64, notify_in_admit: bool) -> (Arc<Woken>, Arc<AtomicBool>, Arc<tokio::sync::Notify>, Arc<AtomicUsize>) {
    let open = Arc::new(AtomicBool::new(false));
    let notify = Arc::new(tokio::sync::Notify::new());
    let admits = Arc::new(AtomicUsize::new(0));
    let gate = Arc::new(Woken {
        open: Arc::clone(&open),
        notify: Arc::clone(&notify),
        admits: Arc::clone(&admits),
        with_wake,
        deadline_ms,
        notify_in_admit,
    });
    (gate, open, notify, admits)
}

#[tokio::test]
async fn a_wake_ends_the_wait_before_retry_at() {
    let (gate, open, notify, admits) = woken(true, 10_000, false);
    let (socket, _peer) = pair(gate).await;
    let started = Instant::now();
    let writing = tokio::spawn({
        let socket = Arc::clone(&socket);
        async move { write(&socket, b"49\x001\x00").await }
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    open.store(true, Ordering::SeqCst);
    notify.notify_one();
    tokio::time::timeout(Duration::from_secs(2), writing)
        .await
        .expect("woken, not left until retry_at")
        .expect("task did not panic")
        .expect("written");
    assert!(started.elapsed() < Duration::from_secs(2));
    assert_eq!(admits.load(Ordering::SeqCst), 2, "one Later, then the woken admit");
    socket.shutdown();
}

#[tokio::test]
async fn a_wake_given_before_the_wait_is_not_lost() {
    // The gate grants the turn from inside `admit`: the write has not begun
    // to wait yet. `notify_one` keeps the permit, so the wait ends at once.
    let (gate, _open, _notify, admits) = woken(true, 10_000, true);
    let (socket, _peer) = pair(gate).await;
    tokio::time::timeout(Duration::from_secs(2), write(&socket, b"49\x001\x00"))
        .await
        .expect("the kept permit ends the wait")
        .expect("written");
    assert_eq!(admits.load(Ordering::SeqCst), 2);
    socket.shutdown();
}

#[tokio::test]
async fn without_a_wake_the_write_waits_for_time_alone() {
    // The control: the same gate with no wake, notified all the same. The
    // write is not woken, and ends at its deadline.
    let (gate, open, notify, _admits) = woken(false, 400, false);
    let (socket, _peer) = pair(gate).await;
    let started = Instant::now();
    let writing = tokio::spawn({
        let socket = Arc::clone(&socket);
        async move { write(&socket, b"49\x001\x00").await }
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    open.store(true, Ordering::SeqCst);
    notify.notify_one();
    let outcome = tokio::time::timeout(Duration::from_secs(3), writing)
        .await
        .expect("bounded by its deadline")
        .expect("task did not panic");
    assert!(
        matches!(outcome, Err(Error::Refused(Refusal::Expired { waiting_for: Waiting::Slot }))),
        "{outcome:?}"
    );
    assert!(started.elapsed() >= Duration::from_millis(390));
    socket.shutdown();
}

#[tokio::test]
async fn wakes_never_move_the_deadline() {
    // Woken over and over, and never admitted: refused at the deadline fixed
    // when the write began, not later.
    let (gate, _open, notify, admits) = woken(true, 400, false);
    let (socket, _peer) = pair(gate).await;
    let started = Instant::now();
    let writing = tokio::spawn({
        let socket = Arc::clone(&socket);
        async move { write(&socket, b"49\x001\x00").await }
    });
    let waking = tokio::spawn(async move {
        for _ in 0..40 {
            notify.notify_one();
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    });
    let outcome = tokio::time::timeout(Duration::from_secs(3), writing)
        .await
        .expect("bounded by its deadline")
        .expect("task did not panic");
    let took = started.elapsed();
    waking.await.expect("waker did not panic");
    assert!(
        matches!(outcome, Err(Error::Refused(Refusal::Expired { waiting_for: Waiting::Slot }))),
        "{outcome:?}"
    );
    assert!(took < Duration::from_millis(700), "the deadline was extended: {took:?}");
    assert!(admits.load(Ordering::SeqCst) > 2, "the wakes woke it");
    socket.shutdown();
}
