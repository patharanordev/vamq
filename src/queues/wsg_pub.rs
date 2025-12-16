use anyhow::Result;
use futures::StreamExt;

use futures_util::SinkExt;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::{Error as WsError, Message};
use tracing::{info, warn};

pub type WsSender = Arc<Mutex<Option<tokio::sync::mpsc::UnboundedSender<Message>>>>;

pub async fn connect_ws(url: &str) -> WsSender {
    let sender: WsSender = Arc::new(Mutex::new(None));
    let url = url.to_string();
    let sender_clone = sender.clone();

    tokio::spawn(async move {
        let mut backoff = std::time::Duration::from_secs(1);
        loop {
            match connect_async(&url).await {
                Ok((ws, _resp)) => {
                    info!("WS connected to {}", url);

                    // Reset backoff after a successful connection
                    backoff = std::time::Duration::from_secs(1);

                    let (mut write, mut read) = ws.split();

                    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Message>();
                    {
                        let mut guard = sender_clone.lock().await;
                        *guard = Some(tx)
                    }

                    // Write outbound messages
                    let write_task = tokio::spawn(async move {
                        while let Some(msg) = rx.recv().await {
                            if let Err(e) = write.send(msg).await {
                                warn!("WS write error: {e}");
                                break;
                            };
                        }
                    });

                    // Drain incoming messages (ignored)
                    let read_task = tokio::spawn(async move {
                        while let Some(msg) = read.next().await {
                            match msg {
                                Ok(_msg) => {
                                    // handle incoming messages if you need them
                                    // debug!("WS recv: {:?}", msg);
                                }
                                Err(e) => {
                                    warn!("WS read error: {e}");
                                    break;
                                }
                            }
                        }
                    });

                    // Wait for either side to end (async just only one end task)
                    tokio::select! {
                        _ = write_task => {},
                        _ = read_task => {},
                    };

                    // Clear sender so ws_send_pcm16 stops sending
                    {
                        let mut guard = sender_clone.lock().await;
                        *guard = None;
                    }

                    warn!(
                        "WS disconnected from {}. Reconnecting in {:?}...",
                        url, backoff
                    );
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(std::time::Duration::from_secs(60));
                }
                Err(e) => {
                    match &e {
                        WsError::Io(ioe) if ioe.kind() == std::io::ErrorKind::ConnectionRefused => {
                            warn!(
                                "WS connect error (ConnectionRefused) to {}: {}. Retrying in {:?}...",
                                url, ioe, backoff
                            );
                        }
                        _ => {
                            warn!(
                                "WS connect error to {}: {}. Retrying in {:?}...",
                                url, e, backoff
                            );
                        }
                    }

                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(std::time::Duration::from_secs(60));
                }
            }
        }
    });

    sender
}

/// Send raw PCM16 bytes to UE
pub async fn ws_send_pcm16(ws: &WsSender, pcm: &[u8]) -> Result<()> {
    let guard = ws.lock().await;
    if let Some(tx) = &*guard {
        tx.send(Message::Binary(pcm.to_vec()))?;
    }
    Ok(())
}
