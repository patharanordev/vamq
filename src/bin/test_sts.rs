use anyhow::{Context, Result};
use dotenvy::dotenv;
use futures::StreamExt;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};
use vamq::{
    providers::openai::{
        config::OpenAiConfig,
        realtime::{RealtimeClient, SharedClient},
        schema::{RealtimeClientOptions, RealtimeFeatures, RealtimeProfile, RtEvent},
    },
    queues::wsg_pub::{WsSender, connect_ws, ws_send_bytes},
};

#[tokio::main]
async fn main() -> Result<()> {
    dotenv().ok();

    // 1. Idiomatic tracing setup (Rust Best Practice #8)
    // Allows fine-grained control via RUST_LOG env var
    tracing_subscriber::fmt()
        .with_env_filter(
            std::env::var("RUST_LOG").unwrap_or_else(|_| "test_sts=debug,vamq=info".into()),
        )
        .init();

    info!("--- OpenAI Realtime S2S Bridge Test (Improved) ---");

    // 2. Mock WebSocket Gateway
    // Simulates the destination for the Speech-to-Speech bridge
    let mock_addr = "127.0.0.1:9001";
    let listener = TcpListener::bind(mock_addr)
        .await
        .context("Failed to bind mock listener on 9001")?;
    info!("Mock WS Gateway listening on ws://{}", mock_addr);

    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            tokio::spawn(async move {
                if let Ok(ws_stream) = tokio_tungstenite::accept_async(stream).await {
                    let (_write, mut read) = ws_stream.split();
                    info!("[Mock Gateway] Client connected");
                    while let Some(msg) = read.next().await {
                        match msg {
                            Ok(m) if m.is_binary() => {
                                debug!(
                                    "[Mock Gateway] Received audio chunk: {} bytes",
                                    m.into_data().len()
                                );
                            }
                            _ => {}
                        }
                    }
                    info!("[Mock Gateway] Client disconnected");
                }
            });
        }
    });

    // 3. VAMQ Bridge Connection
    let ws_url = format!("ws://{}", mock_addr);
    let ws_sender: WsSender = connect_ws(&ws_url).await;

    // Verification: Ensure the internal sender connects (Karpathy Guideline #4)
    let mut connected = false;
    for _ in 0..10 {
        if ws_sender.lock().await.is_some() {
            connected = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    if !connected {
        warn!(
            "WS Sender did not connect to mock gateway within timeout; test may fail forwarding."
        );
    }

    // 4. OpenAI Client Configuration
    // Using GA model and S2S profile
    let api_key = std::env::var("OPENAI_API_KEY")
        .context("OPENAI_API_KEY must be set in .env or environment")?;

    let config = OpenAiConfig {
        api_key: secrecy::SecretString::from(api_key),
        model_realtime: "gpt-realtime-2".to_string(),
        ..Default::default()
    };

    let options = RealtimeClientOptions {
        features: RealtimeFeatures::from_profile(RealtimeProfile::S2S),
        ..Default::default()
    };

    let client = RealtimeClient::connect(&config, options)
        .await
        .context("Failed to establish OpenAI Realtime connection")?;
    info!("Connected to OpenAI Realtime GA");

    let client: SharedClient = Arc::new(Mutex::new(client));
    let (event_tx, mut event_rx) = mpsc::unbounded_channel::<RtEvent>();

    // Start background event pump
    RealtimeClient::listen(&client, event_tx);

    // 5. Test Interaction
    // Triggers the assistant to speak, verifying the audio output stream
    {
        let mut c = client.lock().await;
        info!("Sending test prompt...");
        c.tts("<<<READ>>>Bridge verified. Initializing secure Speech-to-Speech tunnel. Verification code: Alpha-Niner.<<<END>>>")
            .await
            .context("Failed to send TTS command")?;
        c.request_speech(None)
            .await
            .context("Failed to request speech response")?;
    }

    // 6. Success Metrics & Verification (Karpathy Guideline #4)
    let mut deltas_received = 0;
    let mut total_bytes = 0;
    let min_expected_deltas = 5;

    info!("Streaming audio to bridge...");
    while let Some(ev) = event_rx.recv().await {
        match ev {
            RtEvent::AudioDelta(bytes) => {
                deltas_received += 1;
                total_bytes += bytes.len();

                // FORWARDING: Core bridge functionality
                ws_send_bytes(&ws_sender, &bytes)
                    .await
                    .context("Failed to forward audio delta to bridge")?;

                if deltas_received % 5 == 0 {
                    info!("Forwarded {} chunks...", deltas_received);
                }
            }
            RtEvent::AssistantTranscriptDelta(t) => {
                debug!("Assistant text: {}", t);
            }
            RtEvent::ResponseDone(_) => {
                info!(
                    "Response completed: {} chunks, {} KB",
                    deltas_received,
                    total_bytes / 1024
                );
                break;
            }
            RtEvent::Error(e) => {
                anyhow::bail!("API Error reported by OpenAI: {}", e);
            }
            RtEvent::Closed => {
                info!("Realtime connection closed by server");
                break;
            }
            _ => {}
        }
    }

    // Final Success Check
    if deltas_received < min_expected_deltas {
        anyhow::bail!(
            "Test failed: received only {} deltas, expected at least {}",
            deltas_received,
            min_expected_deltas
        );
    }

    info!(
        "S2S Bridge Test passed successfully ({} KB bridged).",
        total_bytes / 1024
    );
    Ok(())
}
