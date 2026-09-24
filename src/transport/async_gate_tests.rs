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
use crate::transport::write_gate::{describe, Admit, Deadline, OutgoingMeta, WriteGate};
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

impl WriteGate for Scripted {
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
    let (socket, mut peer) = pair(gate).await;
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
    let (socket, mut peer) = pair(Arc::clone(&gate) as Arc<dyn WriteGate>).await;
    let started = Instant::now();
    let outcome = write(&socket, &place(8)).await;
    let elapsed = started.elapsed();
    assert!(matches!(outcome, Err(Error::Refused(_))), "{outcome:?}");
    assert!(elapsed >= Duration::from_millis(280), "{elapsed:?}");
    assert!(elapsed < Duration::from_millis(900), "the deadline was extended: {elapsed:?}");
    assert_eq!(gate.deadlines.load(Ordering::SeqCst), 1, "a new deadline was asked for");
    assert!(gate.admits.load(Ordering::SeqCst) > 1);
    socket.shutdown();
    assert!(frames(&mut peer).await.is_empty());
}

#[tokio::test]
async fn a_refusal_hands_no_byte_to_the_socket() {
    let gate = Scripted::new(|_| soon(), |_| Admit::Refuse("not now"));
    let (socket, mut peer) = pair(gate).await;
    let outcome = write(&socket, &place(9)).await;
    assert!(matches!(outcome, Err(Error::Refused(ref reason)) if reason == "not now"), "{outcome:?}");
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
    let (socket, mut peer) = pair(gate).await;
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
    let (socket, mut peer) = pair(Arc::clone(&gate) as Arc<dyn WriteGate>).await;
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
    assert!(matches!(outcome, Err(Error::Refused(_))), "{outcome:?}");
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
    let (socket, mut peer) = pair(gate).await;
    write(&socket, &place(12)).await.expect("written at once");

    // And `Later` is not waited for.
    let held = Scripted::new(
        |_| Deadline::NoWait,
        |_| Admit::Later {
            retry_at: Instant::now() + Duration::from_secs(5),
        },
    );
    let (other, mut other_peer) = pair(Arc::clone(&held) as Arc<dyn WriteGate>).await;
    let started = Instant::now();
    let refused = tokio::time::timeout(Duration::from_secs(1), write(&other, &place(13)))
        .await
        .expect("a write that does not wait is not held");
    assert!(matches!(refused, Err(Error::Refused(_))), "{refused:?}");
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
    let (socket, mut peer) = pair(Arc::clone(&gate) as Arc<dyn WriteGate>).await;
    assert!(matches!(write(&socket, &place(14)).await, Err(Error::Refused(_))));
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
    fn assert_sync_admit<G: WriteGate>(gate: &G, meta: &OutgoingMeta) -> Admit {
        gate.admit(meta)
    }
    let gate = Scripted::new(|_| soon(), |_| Admit::Write);
    let recorded = Mutex::new(());
    let _guard = recorded.lock().expect("not poisoned");
    assert_eq!(assert_sync_admit(gate.as_ref(), &describe(&place(1), None, false)), Admit::Write);
}
