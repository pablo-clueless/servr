use axum::{
    extract::ws::{Message, WebSocket, WebSocketUpgrade},
    response::Response,
};
use tracing::{error, info};

pub async fn ws_handler(ws: WebSocketUpgrade) -> Response {
    ws.on_upgrade(handle_socket)
}

async fn handle_socket(mut socket: WebSocket) {
    info!("WebSocket connection established");

    while let Some(msg) = socket.recv().await {
        match msg {
            Ok(Message::Text(text)) => {
                info!("WebSocket received text: {}", text);
                if let Err(e) = socket.send(Message::Text(format!("Echo: {}", text).into())).await {
                    error!("WebSocket send error while echoing text: {}", e);
                    break;
                }
            }
            Ok(Message::Binary(bin)) => {
                info!("WebSocket received binary data of length {}", bin.len());
                if let Err(e) = socket.send(Message::Binary(bin)).await {
                    error!("WebSocket send error while echoing binary: {}", e);
                    break;
                }
            }
            Ok(Message::Close(frame)) => {
                if let Some(cf) = frame {
                    info!("WebSocket closing with code {} and reason: {}", cf.code, cf.reason);
                } else {
                    info!("WebSocket closing without frame");
                }
                break;
            }
            Err(e) => {
                error!("WebSocket connection error: {}", e);
                break;
            }
            _ => {}
        }
    }

    info!("WebSocket connection closed");
}
