//! A real close of `AsyncTcpSocket`: the peer sees the connection end, no
//! message that had not started is sent, and a write blocked by backpressure
//! neither holds the close nor reports success.

use std::time::Duration;

use tokio::io::AsyncReadExt;
use tokio::net::{TcpListener, TcpStream};

use super::io::{AsyncIo, AsyncReconnect, AsyncTcpSocket};
use crate::Error;

async fn pair() -> (AsyncTcpSocket, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let address = listener.local_addr().expect("local address").to_string();
    let (socket, accepted) = tokio::join!(AsyncTcpSocket::connect(&address, true, None), listener.accept());
    (socket.expect("connects"), accepted.expect("accepts").0)
}

/// Everything the peer receives until the connection ends, within `limit`.
async fn drain(peer: &mut TcpStream, limit: Duration) -> Vec<u8> {
    let mut received = Vec::new();
    let mut buffer = vec![0u8; 1 << 16];
    let deadline = tokio::time::Instant::now() + limit;
    loop {
        match tokio::time::timeout_at(deadline, peer.read(&mut buffer)).await {
            Ok(Ok(0)) | Ok(Err(_)) => return received,
            Ok(Ok(n)) => received.extend_from_slice(&buffer[..n]),
            Err(_) => panic!("the connection did not end within {limit:?}"),
        }
    }
}

#[tokio::test]
async fn the_peer_sees_the_connection_end_and_nothing_after_it() {
    let (socket, mut peer) = pair().await;
    socket.write_all(b"before").await.expect("written");
    socket.shutdown();
    let after = socket.write_all(b"after").await;
    assert!(matches!(after, Err(Error::Closed)), "{after:?}");
    assert_eq!(drain(&mut peer, Duration::from_secs(2)).await, b"before");
    // Idempotent.
    socket.shutdown();
}

#[tokio::test]
async fn a_close_does_not_wait_for_a_write_blocked_by_backpressure() {
    let (socket, mut peer) = pair().await;
    let socket = std::sync::Arc::new(socket);
    // The peer reads nothing, so this fills every buffer and blocks inside
    // the writer's lock.
    let blocked = tokio::spawn({
        let socket = socket.clone();
        async move { socket.write_all(&vec![0u8; 64 << 20]).await }
    });
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(!blocked.is_finished(), "the write should be blocked by backpressure");
    // A second writer queues on the writer's lock behind it.
    let marker = b"QUEUED-WRITER-MARKER";
    let queued = tokio::spawn({
        let socket = socket.clone();
        async move { socket.write_all(marker).await }
    });
    tokio::time::sleep(Duration::from_millis(100)).await;

    socket.shutdown();

    let blocked = tokio::time::timeout(Duration::from_secs(2), blocked)
        .await
        .expect("the blocked write ends once the socket is closed")
        .expect("joins");
    // It had started: whatever it handed the kernel may have gone out, so it
    // is an ambiguous I/O error, never success and never "not sent".
    assert!(matches!(blocked, Err(Error::Io(_))), "{blocked:?}");
    let queued = tokio::time::timeout(Duration::from_secs(2), queued)
        .await
        .expect("the queued write ends")
        .expect("joins");
    assert!(matches!(queued, Err(Error::Closed)), "{queued:?}");

    let received = drain(&mut peer, Duration::from_secs(10)).await;
    assert!(
        !received.windows(marker.len()).any(|window| window == marker),
        "the queued writer's bytes reached the peer"
    );
}

#[tokio::test]
async fn a_closed_socket_does_not_reconnect() {
    let (socket, _peer) = pair().await;
    socket.shutdown();
    assert!(matches!(socket.reconnect().await, Err(Error::Closed)));
}
