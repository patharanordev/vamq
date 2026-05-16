use anyhow::{Context, Result};
use secrecy::SecretString;
use std::env;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};
use vamq::providers::openai::{
    config::OpenAiConfig,
    realtime::RealtimeClient,
    schema::{RealtimeClientOptions, RealtimeFeatures, RealtimeProfile, RtEvent},
};

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();

    // Idiomatic tracing setup
    tracing_subscriber::fmt()
        .with_env_filter(
            std::env::var("RUST_LOG").unwrap_or_else(|_| "test_openai=info,vamq=debug".into()),
        )
        .init();

    info!("--- OpenAI Realtime API Test (Improved) ---");

    // 1. Configuration & Validation (Karpathy Guideline #4)
    let api_key = env::var("OPENAI_API_KEY").context(
        "OPENAI_API_KEY environment variable is not set. Please add it to your .env file.",
    )?;

    let cfg = OpenAiConfig {
        api_key: SecretString::from(api_key),
        model_realtime: "gpt-realtime-2".to_string(), // Updated to GA model
        sample_rate: 24_000,
        ..Default::default()
    };

    let options = RealtimeClientOptions::new(RealtimeFeatures::from_profile(RealtimeProfile::S2S))
        .with_instructions("You are a helpful assistant. Keep your answers brief.");

    info!("Connecting to OpenAI Realtime API...");

    // 2. Connection with explicit context (Rust Best Practice #4)
    let client = RealtimeClient::connect(&cfg, options)
        .await
        .context("Handshake with OpenAI Realtime failed")?;

    info!("Connection established successfully.");

    let shared_client = Arc::new(tokio::sync::Mutex::new(client));
    let (event_tx, mut event_rx) = mpsc::unbounded_channel::<RtEvent>();

    // Start background event pump
    RealtimeClient::listen(&shared_client, event_tx);

    let mut session_ready = false;
    let mut response_started = false;

    info!("Waiting for session ready...");

    // 3. Robust Event Loop
    let timeout_duration = std::time::Duration::from_secs(45);
    let start = std::time::Instant::now();

    while start.elapsed() < timeout_duration {
        match tokio::time::timeout(std::time::Duration::from_millis(100), event_rx.recv()).await {
            Ok(Some(event)) => match event {
                RtEvent::SessionCreated(v) | RtEvent::SessionUpdated(v) => {
                    if !session_ready {
                        let session_id = v["session"]["id"].as_str().unwrap_or("unknown");
                        info!("Session Ready. ID: {}", session_id);
                        session_ready = true;

                        // Trigger test interaction
                        info!("Sending test message...");
                        let mut c = shared_client.lock().await;
                        c.tts("<<<READ>>>Hello, can you hear me? Just checking the connection.<<<END>>>")
                            .await
                            .context("Failed to send test TTS")?;
                        c.request_speech(None)
                            .await
                            .context("Failed to request assistant speech")?;
                    }
                }
                RtEvent::TextDelta(t) => {
                    if !response_started {
                        print!("[Assistant Text] ");
                        response_started = true;
                    }
                    print!("{}", t);
                    std::io::Write::flush(&mut std::io::stdout())?;
                }
                RtEvent::AudioDelta(_) => {
                    if !response_started {
                        print!("[Assistant Audio] ");
                        response_started = true;
                    }
                    print!(".");
                    std::io::Write::flush(&mut std::io::stdout())?;
                }
                RtEvent::ResponseDone(_) => {
                    println!("\n[Done] Response completed.");
                    return Ok(()); // Success exit
                }
                RtEvent::Error(e) => {
                    error!("API Error: {}", e);
                    anyhow::bail!("OpenAI API error: {}", e);
                }
                RtEvent::Closed => {
                    warn!("Connection closed by server.");
                    break;
                }
                RtEvent::Other(v) => {
                    debug!("Other event: {}", v["type"]);
                }
                _ => {}
            },
            Ok(None) => break, // Channel closed
            Err(_) => {
                // Heartbeat/timeout - just continue
                if session_ready && !response_started && start.elapsed().as_secs() > 15 {
                    warn!("Waiting for assistant response timed out.");
                    break;
                }
            }
        }
    }

    if !session_ready {
        anyhow::bail!("Test failed: session was never established.");
    }

    warn!("Test finished without completion event.");
    Ok(())
}
