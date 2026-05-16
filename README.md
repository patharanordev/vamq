# VAMQ

**OpenAI Realtime GA Support Released!**  
Version 0.3.0 marks the official migration to the stable OpenAI Realtime Generally Available (GA) API.  
👉 **[Read the Migration Guide](./docs/openai_realtime_migration.md)** for details on protocol changes and new features.

Consume audio chunks from Voice Activity Messaging via ZeroMQ to support speech-to-X.

Currently, VAMQ only supports voice activity input from [Silero VAD](https://github.com/snakers4/silero-vad).

![overview](./assets/overview.jpg)

## AI providers

- ✅ OpenAI (GA Support)
- ⬜ Gemini

## Usage

### Prerequisites

VAMQ retrieve data from Silero-VAD service, sender should push header & payload to your target service via ZeroMQ look like this:

```py
speech_timestamps = get_speech_ts(
    audio_float32,              # 1D torch.float32 in [-1, 1]
    model,
    sampling_rate=cfg.sampling_rate,
    threshold=ARGS.trig_sum,                # was trig_sum; consider 0.5 if too sensitive
    min_speech_duration_ms=min_speech_ms,   # from min_speech_samples
    min_silence_duration_ms=min_silence_ms, # from min_silence_samples
    window_size_samples=win,                 # from num_samples_per_window
    speech_pad_ms=30                         # small context pad; tune as you like
)

if(len(speech_timestamps)>0):
    print("silero VAD has detected a possible speech")

    for seg in speech_timestamps:
        s = int(seg['start'])
        e = int(seg['end'])

        # Pre-roll: take up to 200ms before start, from the *original int16* buffer
        pre_start = max(0, s - preroll_samples)
        # Why newsound is safer than wav_data:
        # - Index units match (samples with samples) → no off-by-two errors.
        # - You won’t accidentally cut mid-sample.
        # - It stays correct if you change frame size; you don’t have to redo the math each time.
        preroll_i16 = newsound[pre_start:s]
        preroll_bytes = preroll_i16.astype(np.int16).tobytes()

        # Segment bytes (int16 → bytes)
        seg_i16 = newsound[s:e]
        seg_bytes = seg_i16.astype(np.int16).tobytes()

        # 1) START (+ optional PREROLL payload)
        flags = 0b001 | (0b100 if len(preroll_bytes) > 0 else 0)
        sender.send(sender.header(session_id, flags), preroll_bytes)

        # 2) STREAM FRAMES (20ms each)
        for chunk in chunks_20ms(seg_bytes, sr=cfg.sampling_rate, ch=cfg.channels, bytes_per_sample=cfg.bytes_per_sample):
            if not chunk:
                continue

            # middle frames (flags=0)
            sender.send(sender.header(session_id, 0), chunk)

        # 3) END (empty payload)
        sender.send(sender.header(session_id, 0b010), b"")

else:
    print("silero VAD has detected a noise")

```

#### Ex. `header` method in `sender` function

```py
def header(self, session_id:str, flags:int):
    '''
    flags:int
    
        - bit0=start
        - bit1=end
        - bit2=preroll
    '''
    return {
        "session_id": session_id,
        "seq": self.seq,
        "ts_ns": time.monotonic_ns(), # Wall clock can jump (NTP). Use time.monotonic_ns() for your header timestamp.
        "sr": self.configs.sampling_rate,
        "ch": self.configs.channels,
        "fmt": "s16le",
        "flags": flags
    }
```

#### Ex. `send` method in `sender` function

It used socket with `zmq.PUSH` method, receiver will `zmq.PULL` the request:

```py
def send(self, header:dict, payload:bytes):
    self.sock.send(json.dumps(header).encode("utf-8"), zmq.SNDMORE)
    self.sock.send(payload, 0)
    self.seq += 1
```

### OpenAI: Speech-to-X (v0.3.0+)

To use the Speech-to-Speech (S2S) mode, initialize the client with the appropriate profile. The library handles the GA protocol handshake automatically.

```rust
use std::sync::Arc;
use tokio::sync::Mutex;
use vamq::providers::openai::{RealtimeClient, RealtimeClientOptions, RealtimeFeatures, RealtimeProfile, SharedClient};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cfg = OpenAiConfig {
        api_key: secrecy::SecretString::from("sk-..."),
        model_realtime: "gpt-4o-realtime-preview-2024-12-17".into(),
        ..Default::default()
    };

    // 1. Connect with S2S profile
    let options = RealtimeClientOptions::new(RealtimeFeatures::from_profile(RealtimeProfile::S2S));
    let client = RealtimeClient::connect(&cfg, options).await?;

    // 2. Wrap in SharedClient for multi-tasking
    let shared_client: SharedClient = Arc::new(Mutex::new(client));

    // 3. Start listening for audio deltas and events
    let (tx, mut rx) = mpsc::unbounded_channel();
    RealtimeClient::listen(&shared_client, tx);

    Ok(())
}
```

### OpenAI: Text-to-Speech (v0.3.0+)

The `TtsChunkGuard` prevents robotic prosody by buffering LLM tokens into natural sentences before requesting audio generation.

```rust
use vamq::providers::openai::tts::guard::{TtsChunkGuard, TtsPreset, guard_preset};

let mut guard = guard_preset(TtsPreset::Balanced);
let llm_chunks = vec!["Hello!", " This is a streaming", " test."];

for chunk in llm_chunks {
    // Guard flushes text only at natural boundaries
    if let Some((text, emotion)) = guard.push(chunk, None) {
        let mut c = shared_client.lock().await;
        // Synthesize and play immediately
        c.tts(&text).await?;
        c.request_speech(None).await?;
    }
}
```

> [!IMPORTANT]
> **Sequential Constraints**: The OpenAI Realtime API allows only one active response. In production, ensure you wait for `RtEvent::ResponseDone` before triggering the next speech chunk from the guard.

---

## License

MIT
