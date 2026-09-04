//! The upgrade handler and the per-connection frame loop.

use std::sync::Arc;

use axum::extract::ws::{CloseFrame, Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Query, State};
use axum::response::Response;
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use testbed_core::{ConnId, Dir};

use crate::hub::{ConnTrace, Hub, Outbound, Subscription};

/// Close code for a disconnect the *server* chose to perform.
///
/// 1000 (Normal) would tell a client the exchange completed, and 1006 is the
/// abnormal code a client synthesises for itself when a connection vanishes —
/// which is precisely the case T6 exists to keep distinguishable. 1001 (Going
/// Away) is the honest one: the endpoint is going away, deliberately.
pub const CLOSE_CODE: u16 = 1001;

#[derive(Debug, Deserialize)]
pub struct Params {
    /// Topic to subscribe to. Defaults so `ws://host/ws` connects without one,
    /// which is the shape most WS smoke tests take.
    #[serde(default = "default_topic")]
    pub topic: String,
}

fn default_topic() -> String {
    "default".to_string()
}

/// `GET /ws?topic=demo`.
pub async fn upgrade(
    State(hub): State<Arc<Hub>>,
    Query(params): Query<Params>,
    ws: WebSocketUpgrade,
) -> Response {
    let conn = ConnId::new();
    let topic = params.topic;

    // The connection span covers the whole session, so it is deliberately *not*
    // the parent of anything per-frame. Frames link to it instead; see
    // `Hub::frame_span`.
    let span = tracing::info_span!(
        "ws.connection",
        otel.name = %format!("ws connect {topic}"),
        testbed.ws.topic = %topic,
        testbed.ws.conn = %conn,
    );
    let conn_trace = span.in_scope(testbed_telemetry::propagation::current_ids);

    tracing::info!(parent: &span, %conn, %topic, "websocket connected");
    ws.on_upgrade(move |socket| serve(socket, hub, topic, conn, conn_trace))
}

/// The frame loop. Ends on a client close, a socket error, or a
/// [`Outbound::Close`] from the hub.
async fn serve(socket: WebSocket, hub: Arc<Hub>, topic: String, conn: ConnId, trace: ConnTrace) {
    let subscription = hub.join(&topic, conn, trace);
    let (sink, stream) = socket.split();

    let closed_by_server = pump(sink, stream, &hub, &topic, conn, trace, subscription).await;

    // Harmless after a `kill`, which already removed the member; needed after
    // every other exit.
    hub.leave(&topic, conn);
    tracing::info!(%conn, %topic, closed_by_server, "websocket closed");
}

/// How long to wait for the peer's Close echo before giving up on it.
///
/// Short: a client that is not going to reply is not worth holding a task for,
/// and by this point the Close frame is already on the wire.
const CLOSE_HANDSHAKE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);

/// Completes the RFC 6455 closing handshake after *we* sent the Close frame.
///
/// # Trap T6, one layer down
///
/// Sending the Close and immediately returning is not enough. Returning drops
/// both halves of the socket, the TCP connection is torn down, and a client
/// with unread data in flight gets a reset — so it reports an aborted read and
/// never sees the clean close it was just sent. That is the same wrong signal
/// T6 is about, arrived at from the other direction: the frame was correct and
/// the client still cannot tell a deliberate disconnect from a broken network.
///
/// So the socket stays open until the peer echoes the Close, or the stream
/// ends, or the timeout expires.
async fn finish_close(stream: &mut futures_util::stream::SplitStream<WebSocket>) {
    let echo = async {
        while let Some(Ok(message)) = stream.next().await {
            if matches!(message, Message::Close(_)) {
                return;
            }
        }
    };

    if tokio::time::timeout(CLOSE_HANDSHAKE_TIMEOUT, echo)
        .await
        .is_err()
    {
        tracing::debug!("peer did not echo the close frame; closing anyway");
    }
}

/// Returns whether the server initiated the close.
async fn pump(
    mut sink: futures_util::stream::SplitSink<WebSocket, Message>,
    mut stream: futures_util::stream::SplitStream<WebSocket>,
    hub: &Arc<Hub>,
    topic: &str,
    conn: ConnId,
    trace: ConnTrace,
    mut subscription: Subscription,
) -> bool {
    loop {
        tokio::select! {
            // Server -> client. Biased so a queued kill wins a tie against
            // whatever the client happens to be sending: a forced disconnect
            // that waits its turn behind inbound traffic is not forced.
            biased;

            outbound = subscription.recv() => match outbound {
                Some(Outbound::Text(body)) => {
                    if sink.send(Message::Text(body.into())).await.is_err() {
                        return false;
                    }
                }
                Some(Outbound::Close(reason)) => {
                    // Trap T6. Dropping the sink here would leave the client
                    // blocked on a read until its own timeout fired, which
                    // reads as a network failure rather than a disconnect and
                    // silently invalidates reconnection-logic tests.
                    let _ = sink
                        .send(Message::Close(Some(CloseFrame {
                            code: CLOSE_CODE,
                            reason: reason.into(),
                        })))
                        .await;
                    finish_close(&mut stream).await;
                    return true;
                }
                // The hub dropped the member without asking for a close, which
                // only happens if the hub itself went away.
                None => return false,
            },

            // Client -> server.
            inbound = stream.next() => match inbound {
                Some(Ok(Message::Text(body))) => {
                    hub.frame_span(topic, conn, Dir::In, body.len(), trace);
                    // A topic hub, so an inbound frame fans out to the rest of
                    // the topic. `Some(conn)` keeps it off the sender.
                    hub.publish(topic, &body, Some(conn));
                }
                Some(Ok(Message::Binary(bytes))) => {
                    hub.frame_span(topic, conn, Dir::In, bytes.len(), trace);
                }
                // Ping/Pong are answered by axum; a client Close ends the loop.
                Some(Ok(Message::Ping(_) | Message::Pong(_))) => {}
                Some(Ok(Message::Close(_))) | None => return false,
                Some(Err(e)) => {
                    tracing::debug!(%conn, "websocket read failed: {e}");
                    return false;
                }
            },
        }
    }
}
