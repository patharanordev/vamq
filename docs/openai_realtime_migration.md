# OpenAI Realtime GA Migration Guide (v0.3.0)

This document outlines the major changes introduced in `vamq` v0.3.0 to support the **OpenAI Realtime Generally Available (GA)** API. This update migrates the library from the legacy beta protocol to the stable `gpt-realtime-2` specification.

## 1. Protocol Changes & Handshake

The GA API requires an asynchronous handshake where you must wait for the server's `session.created` before sending any configuration.

### Usage Example: Connect & Automatic Handshake
```rust
use vamq::providers::openai::{RealtimeClient, RealtimeClientOptions, RealtimeFeatures, RealtimeProfile};

async fn example_connect() -> anyhow::Result<()> {
    let cfg = OpenAiConfig {
        api_key: "your-key".into(),
        model_realtime: "gpt-4o-realtime-preview-2024-12-17".into(),
        ..Default::default()
    };

    // VAMQ 0.3.0 internally awaits session.created and then sends this configuration
    let options = RealtimeClientOptions::new(RealtimeFeatures::from_profile(RealtimeProfile::TTS));
    let client = RealtimeClient::connect(&cfg, options).await?;
    
    println!("Connected and session configured automatically!");
    Ok(())
}
```

## 2. Shared Client Pattern

For thread-safe operations across different tasks (e.g., listening for audio while sending text), always use the `SharedClient` (Arc-Mutex) pattern.

### Usage Example: Multi-tasking
```rust
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};
use vamq::providers::openai::{RealtimeClient, RtEvent, SharedClient};

async fn example_multitask(client: RealtimeClient) {
    let shared_client: SharedClient = Arc::new(Mutex::new(client));
    let (tx, mut rx) = mpsc::unbounded_channel::<RtEvent>();

    // Task 1: Event Listener
    RealtimeClient::listen(&shared_client, tx);

    // Task 2: Event Handler
    tokio::spawn(async move {
        while let Some(event) = rx.recv().await {
            match event {
                RtEvent::AudioDelta(bytes) => println!("Received {} bytes of audio", bytes.len()),
                RtEvent::ResponseDone(v) => println!("AI finished speaking"),
                _ => {}
            }
        }
    });

    // Task 3: Triggering Speech
    let mut c = shared_client.lock().await;
    c.tts("Hello, this is a multi-tasking test.").await.unwrap();
    c.request_speech(None).await.unwrap();
}
```

## 3. Streaming TTS with `TtsChunkGuard`

To prevent robotic speech caused by tiny token fragments, use the `TtsChunkGuard` to smooth the text stream.

### Usage Example: Smoothing tokens
```rust
use vamq::providers::openai::tts::guard::{TtsChunkGuard, TtsPreset, guard_preset};

async fn example_tts_stream(shared_client: SharedClient) {
    let mut guard = guard_preset(TtsPreset::Balanced);
    let tokens = vec!["Hello", " world", "!", " How", " can", " I", " help", " you", " today", "?"];

    for token in tokens {
        // Guard buffers tokens and only flushes at natural boundaries (punctuation)
        if let Some((text_to_speak, emotion)) = guard.push(token, None) {
            let mut c = shared_client.lock().await;
            c.tts(&text_to_speak).await.unwrap();
            c.request_speech(None).await.unwrap();
            // IMPORTANT: In production, wait for ResponseDone before sending next chunk
        }
    }

    // Always flush remaining text at the end
    if let Some((final_text, emotion)) = guard.finish() {
        let mut c = shared_client.lock().await;
        c.tts(&final_text).await.unwrap();
        c.request_speech(None).await.unwrap();
    }
}
```

## 4. Speech-to-Speech (S2S) Bridge

The bridge pattern combines local VAD audio ingestion with the Realtime API.

### Usage Example: S2S Audio Loop
```rust
use vamq::audio::vad::consumer::VadConsumer;

async fn example_s2s_bridge(shared_client: SharedClient, mut consumer: VadConsumer) {
    loop {
        // 1. Receive VAD-gated audio from local ZeroMQ source
        if let Some(mut commit) = consumer.recv(10).unwrap() {
            let mut c = shared_client.lock().await;
            
            // 2. Stream PCM16 bytes to OpenAI
            c.send_input_pcm16(&commit.pcm24k_s16le).await.unwrap();
            
            // 3. Commit and request response
            c.commit().await.unwrap();
            c.request_response(true, None).await.unwrap();
        }
    }
}
```

## 5. Summary of New Features
- **GA Handshake**: No more `beta_api_shape_disabled` errors.
- **Improved Errors**: `RtEvent::Error` now provides detailed server feedback.
- **Audio Scaling**: Hierarchical configuration ensures high-fidelity 24kHz audio by default.
- **State Safety**: Atomic state tracking for `is_response_active`.
