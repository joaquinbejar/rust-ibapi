//! A real close of `TcpSocket`: the peer sees the connection end, no message
//! that had not started is sent, and a write blocked by backpressure neither
//! holds the close nor reports success.

use std::io::Read;
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use super::{Io, Reconnect, TcpSocket};
use crate::Error;

fn pair() -> (TcpSocket, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let address = listener.local_addr().expect("local address").to_string();
    let socket = TcpSocket::connect(&address, true).expect("connects");
    let (peer, _) = listener.accept().expect("accepts");
    (socket, peer)
}

/// Everything the peer receives until the connection ends, within `limit`.
fn drain(peer: &mut TcpStream, limit: Duration) -> Vec<u8> {
    let deadline = Instant::now() + limit;
    let mut received = Vec::new();
    let mut buffer = vec![0u8; 1 << 16];
    loop {
        let left = deadline
            .checked_duration_since(Instant::now())
            .expect("the connection ends within the limit");
        peer.set_read_timeout(Some(left)).expect("read timeout");
        match peer.read(&mut buffer) {
            Ok(0) => return received,
            Ok(n) => received.extend_from_slice(&buffer[..n]),
            Err(e) if matches!(e.kind(), std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut) => {
                panic!("the connection did not end within {limit:?}")
            }
            Err(_) => return received,
        }
    }
}

#[test]
fn the_peer_sees_the_connection_end_and_nothing_after_it() {
    let (socket, mut peer) = pair();
    socket.write_all(b"before").expect("written");
    socket.shutdown().expect("closes");
    assert!(matches!(socket.write_all(b"after"), Err(Error::Closed)));
    assert_eq!(drain(&mut peer, Duration::from_secs(2)), b"before");
    // Idempotent: a second close may report the socket already shut down,
    // and changes nothing.
    let _ = socket.shutdown();
    assert!(matches!(socket.write_all(b"again"), Err(Error::Closed)));
}

#[test]
fn a_close_does_not_wait_for_a_write_blocked_by_backpressure() {
    let (socket, mut peer) = pair();
    let socket = Arc::new(socket);
    // The peer reads nothing, so this fills every buffer and blocks inside
    // the writer's lock.
    let blocked = thread::spawn({
        let socket = socket.clone();
        move || socket.write_all(&vec![0u8; 64 << 20])
    });
    thread::sleep(Duration::from_millis(300));
    assert!(!blocked.is_finished(), "the write should be blocked by backpressure");
    // A second writer queues on the writer's lock behind it.
    let marker = b"QUEUED-WRITER-MARKER";
    let queued = thread::spawn({
        let socket = socket.clone();
        move || socket.write_all(marker)
    });
    thread::sleep(Duration::from_millis(100));

    let started = Instant::now();
    socket.shutdown().expect("closes");
    assert!(started.elapsed() < Duration::from_secs(1), "the close waited for the writer");

    let blocked = blocked.join().expect("joins");
    // It had started: whatever it handed the kernel may have gone out, so it
    // is an ambiguous I/O error, never success and never "not sent".
    assert!(matches!(blocked, Err(Error::Io(_))), "{blocked:?}");
    let queued = queued.join().expect("joins");
    assert!(matches!(queued, Err(Error::Closed)), "{queued:?}");

    let received = drain(&mut peer, Duration::from_secs(10));
    assert!(
        !received.windows(marker.len()).any(|window| window == marker),
        "the queued writer's bytes reached the peer"
    );
}

#[test]
fn a_closed_socket_does_not_reconnect() {
    let (socket, _peer) = pair();
    socket.shutdown().expect("closes");
    assert!(matches!(socket.reconnect(), Err(Error::Closed)));
}
