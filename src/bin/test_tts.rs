use anyhow::{Context, Result};
use dotenvy::dotenv;
use std::sync::Arc;
use tokio::sync::{Mutex, mpsc};
use tracing::{debug, info, warn};
use vamq::providers::openai::{
    config::OpenAiConfig,
    realtime::{RealtimeClient, SharedClient},
    schema::{RealtimeClientOptions, RealtimeFeatures, RealtimeProfile, RtEvent},
    tts::guard::TtsChunkGuard,
};

#[tokio::main]
async fn main() -> Result<()> {
    dotenv().ok();

    tracing_subscriber::fmt()
        .with_env_filter(
            std::env::var("RUST_LOG").unwrap_or_else(|_| "test_tts=debug,vamq=info".into()),
        )
        .init();

    info!("--- OpenAI Realtime Streaming TTS Test (Improved) ---");

    // 1. Setup OpenAI Client
    let api_key = std::env::var("OPENAI_API_KEY")
        .context("OPENAI_API_KEY must be set in .env or environment")?;

    let config = OpenAiConfig {
        api_key: secrecy::SecretString::from(api_key),
        model_realtime: "gpt-realtime-2".to_string(),
        ..Default::default()
    };

    let client = RealtimeClient::connect(
        &config,
        RealtimeClientOptions {
            features: RealtimeFeatures::from_profile(RealtimeProfile::TTS),
            ..Default::default()
        },
    )
    .await?;
    info!("Connected to OpenAI Realtime GA");

    let client: SharedClient = Arc::new(Mutex::new(client));
    let (event_tx, mut event_rx) = mpsc::unbounded_channel::<RtEvent>();
    RealtimeClient::listen(&client, event_tx);

    // 2. Concurrency Control: Sequential Speech Queue
    // OpenAI Realtime only allows one active response at a time.
    let (speech_tx, mut speech_rx) = mpsc::channel::<String>(10);
    let (done_tx, mut done_rx) = mpsc::channel::<()>(1);

    let speech_client = Arc::clone(&client);
    tokio::spawn(async move {
        while let Some(text) = speech_rx.recv().await {
            debug!("Processing speech request: '{}'", text);
            let mut c = speech_client.lock().await;
            if let Err(e) = c.tts(&text).await {
                warn!("Failed to send TTS: {}", e);
                continue;
            }
            if let Err(e) = c.request_speech(None).await {
                warn!("Failed to request speech: {}", e);
                continue;
            }
            drop(c);

            // Wait for completion before next sentence
            debug!("Waiting for response to finish...");
            let _ = done_rx.recv().await;
        }
    });

    // 3. Simulate LLM Streaming
    let tokens = vec![
        "Hello",
        " there!",
        " This",
        " is",
        " a",
        " test",
        " of",
        " the",
        " streaming",
        " TTS",
        " system.",
        " It",
        " uses",
        " a",
        " guard",
        " to",
        " ensure",
        " that",
        " we",
        " don't",
        " send",
        " tiny",
        " fragments",
        " to",
        " the",
        " voice",
        " engine,",
        " which",
        " helps",
        " maintain",
        " a",
        " natural",
        " flow",
        " and",
        " prevents",
        " jittery",
        " speech.",
    ];

    let mut guard = TtsChunkGuard::new();
    let speech_tx_clone = speech_tx.clone();

    tokio::spawn(async move {
        info!("Simulating LLM token stream...");
        for token in tokens {
            if let Some(buffered_text) = guard.push(token) {
                info!("Guard Flushed: '{}'", buffered_text);
                let _ = speech_tx_clone.send(buffered_text).await;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }

        if let Some(final_text) = guard.finish() {
            info!("Guard Final Flush: '{}'", final_text);
            let _ = speech_tx_clone.send(final_text).await;
        }
    });

    // 4. Monitor Events
    let mut total_chunks = 0;
    while let Some(ev) = event_rx.recv().await {
        match ev {
            RtEvent::AudioDelta(_) => {
                total_chunks += 1;
            }
            RtEvent::ResponseDone(_) => {
                debug!("Response finished event received.");
                let _ = done_tx.send(()).await;
            }
            RtEvent::Error(e) => {
                warn!("API Error: {}", e);
                // Even on error, we should unlock the queue
                let _ = done_tx.try_send(());
            }
            RtEvent::Closed => break,
            _ => {}
        }

        // Exit test after some time or if tokens finished
        if total_chunks > 50 && speech_tx.capacity() == 10 {
            // This is a rough exit condition for the test
        }
    }

    info!("TTS Test finished. Total audio chunks: {}", total_chunks);
    Ok(())
}
