//! The write gate seen from the message bus: a request the gate refuses, or
//! one dropped while it waits, leaves no response channel behind, and never
//! removes an entry that is not its own.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use prost::Message as _;
use tokio::net::{TcpListener, TcpStream};

use super::gate_tests::{gated, SimpleGate};
use super::io::AsyncTcpSocket;
use super::{AsyncMessageBus, AsyncTcpMessageBus, Table};
use crate::connection::r#async::AsyncConnection;
use crate::messages::{encode_protobuf_message, IncomingMessages, OutgoingMessages};
use crate::transport::write_gate::{Admit, Deadline, OutgoingMeta};
use crate::Error;

type Bus = Arc<AsyncTcpMessageBus<AsyncTcpSocket>>;

async fn bus<G: SimpleGate>(gate: Arc<G>) -> (Bus, TcpStream) {
    let gate = gated(&gate);
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let address = listener.local_addr().expect("local address").to_string();
    let (socket, accepted) = tokio::join!(AsyncTcpSocket::connect(&address, true, Some(gate)), listener.accept());
    let connection = AsyncConnection::stubbed(socket.expect("connects"), 1);
    let bus = AsyncTcpMessageBus::new(connection).expect("bus");
    (Arc::new(bus), accepted.expect("accepts").0)
}

fn time() -> Vec<u8> {
    encode_protobuf_message(OutgoingMessages::RequestCurrentTime as i32, &[])
}

fn market_data() -> Vec<u8> {
    b"1\x0011\x00".to_vec()
}

fn place(order_id: i32) -> Vec<u8> {
    let request = crate::proto::PlaceOrderRequest {
        order_id: Some(order_id),
        ..Default::default()
    };
    encode_protobuf_message(OutgoingMessages::PlaceOrder as i32, &request.encode_to_vec())
}

/// A gate that answers by message: `Later` for a current-time request (held
/// far beyond any test), and for everything else `Write` or `Refuse`
/// according to a switch. It counts the `Later` answers so a test can wait
/// for a request to be parked.
struct Switch {
    refuse: AtomicBool,
    hold_placements: AtomicBool,
    held: AtomicUsize,
    deadline: Duration,
}

impl Switch {
    fn new(refuse: bool, deadline: Duration) -> Arc<Self> {
        Arc::new(Self {
            refuse: AtomicBool::new(refuse),
            hold_placements: AtomicBool::new(false),
            held: AtomicUsize::new(0),
            deadline,
        })
    }

    async fn parked(&self, count: usize) {
        tokio::time::timeout(Duration::from_secs(2), async {
            while self.held.load(Ordering::SeqCst) < count {
                tokio::task::yield_now().await;
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("the request is parked on the gate");
    }
}

impl SimpleGate for Switch {
    fn deadline(&self, _meta: &OutgoingMeta) -> Deadline {
        Deadline::At(Instant::now() + self.deadline)
    }

    fn admit(&self, meta: &OutgoingMeta) -> Admit {
        let hold = match meta.message {
            Some(OutgoingMessages::RequestCurrentTime) => true,
            Some(OutgoingMessages::PlaceOrder) => self.hold_placements.load(Ordering::SeqCst),
            _ => false,
        };
        if hold {
            self.held.fetch_add(1, Ordering::SeqCst);
            return Admit::Later {
                retry_at: Instant::now() + Duration::from_secs(60),
            };
        }
        if self.refuse.load(Ordering::SeqCst) {
            Admit::Refuse("test")
        } else {
            Admit::Write
        }
    }
}

async fn requests(bus: &Bus) -> usize {
    bus.request_channels.read().await.len()
}

async fn orders(bus: &Bus) -> usize {
    bus.order_channels.read().await.len()
}

/// Wait, boundedly, for the cleanup task to have emptied both tables.
async fn settles_empty(bus: &Bus) {
    tokio::time::timeout(Duration::from_secs(2), async {
        while requests(bus).await + orders(bus).await > 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("no registration is left behind");
}

fn family_codes_receivers(bus: &Bus) -> usize {
    let senders = bus.shared_channel_senders.try_read().expect("uncontended");
    senders.get(&IncomingMessages::FamilyCodes).expect("configured")[0].receiver_count()
}

#[tokio::test]
async fn refused_requests_leave_no_registration() {
    let (bus, _peer) = bus(Switch::new(true, Duration::from_secs(10))).await;
    let shared_before = family_codes_receivers(&bus);

    for id in 0..200 {
        let request = bus.send_request(id, market_data()).await;
        assert!(matches!(request, Err(Error::Refused(_))), "{:?}", request.as_ref().err());
        let order = bus.send_order_request(id, place(id)).await;
        assert!(matches!(order, Err(Error::Refused(_))), "{:?}", order.as_ref().err());
        let shared = bus
            .send_shared_request(OutgoingMessages::RequestFamilyCodes, b"80\x001\x00".to_vec())
            .await;
        assert!(matches!(shared, Err(Error::Refused(_))), "{:?}", shared.as_ref().err());
    }

    // Undone on the error path itself, not later by the cleanup task.
    assert_eq!(requests(&bus).await, 0);
    assert_eq!(orders(&bus).await, 0);
    // A shared request registers nothing; its receiver went with the error.
    assert_eq!(family_codes_receivers(&bus), shared_before);
}

#[tokio::test]
async fn a_request_dropped_while_its_write_waits_leaves_no_registration() {
    let switch = Switch::new(false, Duration::from_secs(30));
    switch.hold_placements.store(true, Ordering::SeqCst);
    let (bus, _peer) = bus(switch.clone()).await;

    let request = tokio::spawn({
        let bus = bus.clone();
        async move { bus.send_request(9, time()).await }
    });
    let order = tokio::spawn({
        let bus = bus.clone();
        async move { bus.send_order_request(10, place(10)).await }
    });
    switch.parked(2).await;
    assert_eq!(requests(&bus).await, 1, "registered before the write, so no answer is lost");
    assert_eq!(orders(&bus).await, 1);

    request.abort();
    order.abort();
    assert!(matches!(request.await, Err(e) if e.is_cancelled()));
    assert!(matches!(order.await, Err(e) if e.is_cancelled()));

    settles_empty(&bus).await;
}

/// How many receivers listen on the entry under `id`, if there is one.
fn listeners(bus: &Bus, table: Table, id: i32) -> Option<usize> {
    let channels = match table {
        Table::Request => &bus.request_channels,
        Table::Order => &bus.order_channels,
    };
    channels.try_read().expect("uncontended").get(&id).map(|sender| sender.receiver_count())
}

/// Wait, boundedly, until `probe` holds.
async fn eventually(what: &str, probe: impl Fn() -> bool) {
    tokio::time::timeout(Duration::from_secs(2), async {
        while !probe() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("{what}"));
}

/// Give the cleanup task every chance to act, for a check that nothing
/// happened.
async fn settle() {
    tokio::time::sleep(Duration::from_millis(100)).await;
}

#[tokio::test]
async fn a_modify_refused_or_dropped_keeps_the_order_subscription() {
    let switch = Switch::new(false, Duration::from_secs(10));
    let (bus, _peer) = bus(switch.clone()).await;

    let placed = bus.send_order_request(7, place(7)).await.expect("placed");
    // A subscription holds its receivers; how many is its own business.
    let placement = listeners(&bus, Table::Order, 7);
    assert!(matches!(placement, Some(n) if n > 0), "{placement:?}");

    // A modify of the same order, refused before any byte.
    switch.refuse.store(true, Ordering::SeqCst);
    let modify = bus.send_order_request(7, place(7)).await;
    assert!(matches!(modify, Err(Error::Refused(_))), "{:?}", modify.as_ref().err());
    assert_eq!(listeners(&bus, Table::Order, 7), placement, "the placement's channel, as it was");

    // A modify dropped while it waits: it listened on the placement's channel
    // (joined, not replaced), and only its own receiver goes.
    switch.refuse.store(false, Ordering::SeqCst);
    switch.hold_placements.store(true, Ordering::SeqCst);
    let waiting = tokio::spawn({
        let bus = bus.clone();
        async move { bus.send_order_request(7, place(7)).await }
    });
    switch.parked(1).await;
    assert_eq!(
        listeners(&bus, Table::Order, 7),
        placement.map(|n| n + 1),
        "the modify joined the placement's channel"
    );
    waiting.abort();
    let _ = waiting.await;
    settle().await;
    assert_eq!(listeners(&bus, Table::Order, 7), placement, "the placement's channel, as it was");

    // And it is the placement's: released when the placement's subscription is.
    drop(placed);
    eventually("the placement's entry is released", || listeners(&bus, Table::Order, 7).is_none()).await;
}

#[tokio::test]
async fn a_written_modify_shares_the_order_channel_with_the_placement() {
    let (bus, _peer) = bus(Switch::new(false, Duration::from_secs(10))).await;

    let mut placed = bus.send_order_request(8, place(8)).await.expect("placed");
    let placement = listeners(&bus, Table::Order, 8).expect("registered");
    let mut modified = bus.send_order_request(8, place(8)).await.expect("modified");
    assert!(listeners(&bus, Table::Order, 8).expect("registered") > placement, "joined, not replaced");

    // What IB says about the order reaches both.
    let sender = bus.order_channels.read().await.get(&8).expect("registered").clone();
    sender.send(Error::Cancelled.into()).expect("listened to");
    drop(sender);
    assert!(matches!(placed.next().await, Some(Err(Error::Cancelled))));
    assert!(matches!(modified.next().await, Some(Err(Error::Cancelled))));

    // One of them ending does not end the other's.
    drop(placed);
    settle().await;
    assert!(matches!(listeners(&bus, Table::Order, 8), Some(n) if n > 0), "the modify still listens");
    drop(modified);
    eventually("the entry is released with its last listener", || {
        listeners(&bus, Table::Order, 8).is_none()
    })
    .await;
}

#[tokio::test]
async fn the_cleanup_of_an_earlier_subscription_leaves_its_successor_alone() {
    let (bus, _peer) = bus(Switch::new(false, Duration::from_secs(10))).await;

    let first = bus.send_request(11, market_data()).await.expect("written");
    // A request id reused while the first still listens replaces it, as it
    // always has.
    let second = bus.send_request(11, market_data()).await.expect("written");
    let held = listeners(&bus, Table::Request, 11);
    drop(first);
    settle().await;
    assert_eq!(listeners(&bus, Table::Request, 11), held, "the successor's entry is untouched");
    drop(second);
    eventually("the successor's entry is released", || listeners(&bus, Table::Request, 11).is_none()).await;
}

#[tokio::test]
async fn a_clone_keeps_the_channel_its_original_released() {
    let (bus, _peer) = bus(Switch::new(false, Duration::from_secs(10))).await;

    let original = bus.send_request(12, market_data()).await.expect("written");
    let clone = original.clone();
    drop(original);
    settle().await;
    assert!(matches!(listeners(&bus, Table::Request, 12), Some(n) if n > 0), "the clone still listens");
    drop(clone);
    eventually("the entry is released with its last listener", || {
        listeners(&bus, Table::Request, 12).is_none()
    })
    .await;
}

#[tokio::test]
async fn a_successor_under_the_same_id_is_left_alone() {
    // The first request waits (`Later`) and then fails at its deadline; a
    // second one under the same id is written meanwhile.
    let (bus, _peer) = bus(Switch::new(false, Duration::from_millis(400))).await;

    let first = tokio::spawn({
        let bus = bus.clone();
        async move { bus.send_request(5, time()).await }
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    let successor = bus.send_request(5, market_data()).await.expect("written");
    let held = listeners(&bus, Table::Request, 5);
    assert!(matches!(held, Some(n) if n > 0), "{held:?}");
    let first = first.await.expect("joins");
    assert!(matches!(first, Err(Error::Refused(_))), "{:?}", first.as_ref().err());
    assert_eq!(listeners(&bus, Table::Request, 5), held, "the successor's entry is untouched");
    drop(successor);
    eventually("the successor's entry is released", || listeners(&bus, Table::Request, 5).is_none()).await;
}

#[tokio::test]
async fn a_successor_is_left_alone_by_a_dropped_request_too() {
    let switch = Switch::new(false, Duration::from_secs(30));
    let (bus, _peer) = bus(switch.clone()).await;

    let first = tokio::spawn({
        let bus = bus.clone();
        async move { bus.send_request(6, time()).await }
    });
    switch.parked(1).await;
    let successor = bus.send_request(6, market_data()).await.expect("written");
    let held = listeners(&bus, Table::Request, 6);
    assert!(matches!(held, Some(n) if n > 0), "{held:?}");
    first.abort();
    let _ = first.await;
    settle().await;
    assert_eq!(listeners(&bus, Table::Request, 6), held, "the successor's entry is untouched");
    drop(successor);
    eventually("the successor's entry is released", || listeners(&bus, Table::Request, 6).is_none()).await;
}

#[tokio::test]
async fn a_refused_request_puts_back_the_entry_it_replaced() {
    let switch = Switch::new(false, Duration::from_secs(10));
    let (bus, _peer) = bus(switch.clone()).await;

    let earlier = bus.send_request(13, market_data()).await.expect("written");
    let held = listeners(&bus, Table::Request, 13);
    switch.refuse.store(true, Ordering::SeqCst);
    let refused = bus.send_request(13, market_data()).await;
    assert!(matches!(refused, Err(Error::Refused(_))), "{:?}", refused.as_ref().err());
    // Nothing was sent, so nothing replaced the earlier request.
    assert_eq!(listeners(&bus, Table::Request, 13), held, "the earlier entry is back");
    drop(earlier);
    eventually("the earlier entry is released", || listeners(&bus, Table::Request, 13).is_none()).await;
}
