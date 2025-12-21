use std::{future::Future, sync::Arc};

use crate::providers::openai::{
    config::OpenAiConfig,
    schema::{RealtimeFeatures, RtEvent},
};
use crate::queues::wsg_pub::WsSender;
use anyhow::{Context, Result};
use base64::{Engine as _, engine::general_purpose};
use futures::{SinkExt, StreamExt};
use secrecy::ExposeSecret;
use serde_json::{self, json};
use tokio::{
    sync::mpsc::{UnboundedReceiver, UnboundedSender},
    time::{Duration, Instant},
};
use tokio_tungstenite::tungstenite::{handshake::client::generate_key, http::Request};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{debug, error, info, warn};

const OPENAI_REALTIME_WS: &str = "wss://api.openai.com/v1/realtime";

pub type SharedClient = Arc<tokio::sync::Mutex<RealtimeClient>>;
pub struct RealtimeClient {
    ws: tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    /// last ping time for keepalive
    last_ping: Instant,
}

impl RealtimeClient {
    /// Helper to send a JSON message as a WS text frame.
    #[inline]
    async fn send_json(&mut self, v: serde_json::Value) -> Result<()> {
        self.ws.send(Message::Text(v.to_string())).await?;
        Ok(())
    }

    /// Connects and immediately configures default output audio.
    /// `sample_rate`: usually **24000** for OpenAI Realtime (safe default).
    pub async fn connect(cfg: &OpenAiConfig, features: RealtimeFeatures) -> Result<Self> {
        let mut ws_url = format!("{OPENAI_REALTIME_WS}?model={}", cfg.model_realtime);
        if features.enable_transcribe {
            ws_url = format!("{OPENAI_REALTIME_WS}?intent=transcription");
        }

        // You MUST include the standard WS upgrade headers when you pass a Request.
        let req = Request::builder()
            .method("GET")
            .uri(&ws_url)
            .header("Host", "api.openai.com")
            .header("Connection", "Upgrade")
            .header("Upgrade", "websocket")
            .header("Sec-WebSocket-Version", "13")
            .header("Sec-WebSocket-Key", generate_key())
            // OpenAI headers:
            .header(
                "Authorization",
                format!("Bearer {}", cfg.api_key.expose_secret()),
            )
            .header("OpenAI-Beta", "realtime=v1")
            .header("User-Agent", "realtime-s2s-a2f/1.0")
            .body(())?;

        // establish TLS WS
        let (mut ws, _resp) = connect_async(req)
            .await
            .context("connect_async failed (check TLS features and model name)")?;

        // Configure output audio (pcm16 @ sample_rate)
        let mut session_cfg = json!({});
        let mut session = serde_json::Map::new();

        if features.enable_conversation {
            session.insert("voice".into(), json!("alloy"));
            session.insert("input_audio_format".into(), json!("pcm16"));

            session.insert("instructions".into(), json!(
                "You are a helpful multiple languages speaking assistant, reply in the user’s language."
            ));
            session.insert("modalities".into(), json!(["audio", "text"]));
            session.insert("output_audio_format".into(), json!("pcm16"));

            // Auto-VAD
            session.insert(
                "turn_detection".into(),
                RealtimeClient::turn_detection(&features),
            );

            // Input transcription of user (defaults off) :contentReference[oaicite:8]{index=8}
            if features.enable_input_transcription {
                session.insert(
                    "input_audio_transcription".into(),
                    json!({
                        "model": cfg.model_transcribe
                    }),
                );
            }

            // Output audio transcript (for debug)
            // Ref. event response.output_audio_transcript.delta :contentReference[oaicite:10]{index=10}
            if features.enable_output_audio_transcript {
                session.insert(
                    "include".into(),
                    json!(["response.output_audio_transcript"]),
                );
            }

            session_cfg = json!({
                "type": "session.update",
                "session": session
            });
        } else if features.enable_transcribe {
            session_cfg = json!({
                "type": "transcription_session.update",
                "session": {
                    "input_audio_format": "pcm16",
                    "input_audio_transcription": {
                        "model": cfg.model_transcribe,
                        // "prompt": "",
                        // "language": ""
                    },
                    "turn_detection": RealtimeClient::turn_detection(&features),
                    "input_audio_noise_reduction": {
                        "type": "near_field"
                    },
                    // "include": [
                    //     "item.input_audio_transcription.logprobs"
                    // ]
                }
            });
        }

        debug!("session_cfg: {:?}", session_cfg.to_string());
        ws.send(Message::Text(session_cfg.to_string())).await?;

        Ok(Self {
            ws,
            last_ping: Instant::now(),
        })
    }

    /// Minimal reconnect helper (exponential backoff handled by caller)
    pub async fn reconnect(
        &mut self,
        cfg: &OpenAiConfig,
        features: RealtimeFeatures,
    ) -> Result<()> {
        *self = Self::connect(cfg, features).await?;
        Ok(())
    }

    fn turn_detection(features: &RealtimeFeatures) -> serde_json::Value {
        if features.use_server_vad {
            json!({
                "type": "server_vad",
                "threshold": 0.5,
                "prefix_padding_ms": 300,
                "silence_duration_ms": 500
            })
        } else {
            serde_json::Value::Null
        }
    }

    /// Append an input PCM16 mono chunk encoded as base64 (what Realtime expects).
    /// Typical chunk = 20–40 ms @ 24 kHz (480–960 samples → 960–1920 bytes).
    pub async fn send_input_pcm16(&mut self, bytes: &[u8]) -> Result<()> {
        if !bytes.len().is_multiple_of(2) {
            anyhow::bail!("unaligned PCM16 chunk (odd byte length: {})", bytes.len());
        }

        let b64 = general_purpose::STANDARD.encode(bytes);
        let msg = json!({
            "type": "input_audio_buffer.append",
            "audio": b64
        });
        self.ws.send(Message::Text(msg.to_string())).await?;
        Ok(())
    }

    /// Commit current buffered audio → triggers transcription + synthesis.
    pub async fn commit(&mut self) -> Result<()> {
        let msg = json!({ "type": "input_audio_buffer.commit" });
        self.send_json(msg).await?;
        Ok(())
    }

    /// Request response from OpenAI:
    /// - want_text: audio with text (true) or audio only (false)
    /// - style: your instructions, ex. "Speak the reply clearly, naturally."
    pub async fn request_response(&mut self, want_text: bool, style: Option<&str>) -> Result<()> {
        let instructions = style.unwrap_or("Speak the reply clearly, naturally.");
        let modalities = if want_text {
            vec!["audio", "text"]
        } else {
            vec!["audio"]
        };
        let msg = json!({
            "type": "response.create",
            "response": {
                "modalities": modalities,
                // optional steering:
                "instructions": instructions
            }
        });
        self.send_json(msg).await?;
        Ok(())
    }

    /// Non-blocking heartbeat/ping (~15s) to keep connection alive through proxies/NATs.
    pub async fn maybe_ping(&mut self) -> Result<()> {
        if self.last_ping.elapsed() >= Duration::from_secs(15) {
            // some proxies drop idle WS; ping/pong keeps it open
            let _ = self.ws.send(Message::Ping(Vec::new())).await;
            self.last_ping = Instant::now();
        }
        Ok(())
    }

    /// Reads the next meaningful event with timeout. Returns:
    /// - `AudioDelta(Vec<u8>)` for audio chunks
    /// - `Completed` when a response end arrives
    /// - `Error` if the server emits an error frame
    /// - `Other(v)` for other control frames you may want to log
    /// - `Closed` if the socket ends
    pub async fn next_event(&mut self, wait_ms: u64) -> Result<RtEvent> {
        self.maybe_ping().await?;
        let fut = self.ws.next();

        match tokio::time::timeout(Duration::from_millis(wait_ms), fut).await {
            Ok(Some(msg)) => {
                let msg = msg?;
                match msg {
                    Message::Text(txt) => {
                        let v: serde_json::Value =
                            serde_json::from_str(&txt).unwrap_or_else(|_| json!({"raw": txt}));

                        let etype = v.get("type").and_then(|t| t.as_str());

                        match etype {
                            // ============================================================
                            // SESSION EVENTS
                            // ============================================================
                            Some("session.created") => Ok(RtEvent::SessionCreated(v)),

                            // ============================================================
                            // AUDIO CHUNKS (new + old)
                            //
                            // NEW:
                            //   "type": "response.audio.delta"
                            //   "delta": "<b64>"
                            //
                            // OLD:
                            //   "type": "response.output_audio.delta"
                            //   "audio": "<b64>"
                            // ============================================================
                            Some("response.audio.delta") | Some("response.output_audio.delta") => {
                                let b64 = v
                                    .get("delta")
                                    .and_then(|d| d.as_str())
                                    .or_else(|| v.get("audio").and_then(|a| a.as_str()));

                                if let Some(b64) = b64 {
                                    let bytes = general_purpose::STANDARD.decode(b64)?;
                                    Ok(RtEvent::AudioDelta(bytes))
                                } else {
                                    Ok(RtEvent::Other(v))
                                }
                            }

                            // ============================================================
                            // TEXT DELTAS (new + old)
                            //
                            // NEW:
                            //   "type": "response.text.delta"
                            //
                            // OLD:
                            //   "type": "response.output_text.delta"
                            // ============================================================
                            Some("response.text.delta") | Some("response.output_text.delta") => {
                                if let Some(s) = v.get("delta").and_then(|d| d.as_str()) {
                                    Ok(RtEvent::TextDelta(s.to_string()))
                                } else {
                                    Ok(RtEvent::Other(v))
                                }
                            }

                            // ============================================================
                            // TRANSCRIPT DELTAS (transcript INPUT speech transcript while listening)
                            // ============================================================
                            Some("conversation.item.input_audio_transcription.delta") => {
                                let delta = v
                                    .get("delta")
                                    .and_then(|x| x.as_str())
                                    .unwrap_or("")
                                    .to_string();
                                Ok(RtEvent::UserTranscriptDelta(delta))
                            }

                            Some("conversation.item.input_audio_transcription.completed") => {
                                let text = v
                                    .get("transcript")
                                    .and_then(|x| x.as_str())
                                    .or_else(|| v.get("text").and_then(|x| x.as_str()))
                                    .unwrap_or("")
                                    .to_string();
                                Ok(RtEvent::UserTranscriptFinal(text))
                            }

                            // ============================================================
                            // TRANSCRIPT DELTAS (transcript OUTPUT speech transcript while listening)
                            //
                            // Example:
                            //   "type": "response.audio_transcript.delta"
                            //   "delta": "What"
                            // ============================================================
                            Some("response.audio_transcript.delta") => {
                                if let Some(s) = v.get("delta").and_then(|d| d.as_str()) {
                                    Ok(RtEvent::TextDelta(s.to_string()))
                                } else {
                                    Ok(RtEvent::Other(v))
                                }
                            }

                            // ============================================================
                            // END OF INDIVIDUAL AUDIO PART
                            //
                            // "response.audio.done"
                            // "response.audio_transcript.done"
                            // ============================================================
                            Some("response.audio.done")
                            | Some("response.audio_transcript.done")
                            | Some("response.content_part.done") => Ok(RtEvent::Completed),

                            // ============================================================
                            // COMPLETION OF ENTIRE RESPONSE
                            //
                            // NEW: "response.done"
                            // OLD: "response.completed"
                            // ============================================================
                            Some("response.done") | Some("response.completed") => {
                                Ok(RtEvent::Completed)
                            }

                            // ============================================================
                            // INPUT AUDIO BUFFER COMMITTED
                            //
                            //   "type": "input_audio_buffer.committed"
                            // ============================================================
                            Some("input_audio_buffer.committed") => Ok(RtEvent::Other(v)),

                            // ============================================================
                            // RESPONSE/ITEM CREATION EVENTS
                            //
                            //   "response.created"
                            //   "response.output_item.added"
                            //   "response.output_item.done"
                            //   "conversation.item.created"
                            // ============================================================
                            Some("response.created")
                            | Some("response.output_item.added")
                            | Some("response.output_item.done")
                            | Some("conversation.item.created")
                            | Some("response.content_part.added") => Ok(RtEvent::Other(v)),

                            // ============================================================
                            // ERROR FRAMES
                            // ============================================================
                            Some("error")
                            | Some("conversation.item.input_audio_transcription.failed") => {
                                let msg = v
                                    .get("error")
                                    .and_then(|e| e.get("message"))
                                    .and_then(|m| m.as_str())
                                    .unwrap_or("Unknown error")
                                    .to_string();
                                Ok(RtEvent::Error(msg))
                            }

                            // ============================================================
                            // ANYTHING ELSE
                            // ============================================================
                            _ => Ok(RtEvent::Other(v)),
                        }
                    }

                    // ================================================================
                    // BINARY FALLBACK
                    // ================================================================
                    Message::Binary(bin) => Ok(RtEvent::AudioDelta(bin)),

                    Message::Ping(payload) => {
                        let _ = self.ws.send(Message::Pong(payload)).await;
                        Ok(RtEvent::Other(json!({"event": "ping"})))
                    }

                    Message::Pong(_) => Ok(RtEvent::Other(json!({"event": "pong"}))),

                    Message::Close(_) => Ok(RtEvent::Closed),

                    _ => Ok(RtEvent::Other(json!({"event": "unhandled"}))),
                }
            }

            Ok(None) => Ok(RtEvent::Closed),
            Err(_) => Ok(RtEvent::Idle),
        }
    }

    pub fn listen(client: &SharedClient, tx: UnboundedSender<RtEvent>) {
        let client_for_render = Arc::clone(client);
        tokio::spawn(async move {
            loop {
                let ev_res = {
                    let mut cli = client_for_render.lock().await;
                    cli.next_event(100).await
                };
                let ev = match ev_res {
                    Ok(ev) => ev,
                    Err(e) => {
                        error!("realtime event loop error: {:?}", e);
                        break;
                    }
                };

                // If nobody is listening anymore, just stop
                if tx.send(ev).is_err() {
                    warn!("realtime event channel closed; stopping event loop");
                    break;
                }
            }
        });

        info!("realtime event listening...");
    }

    /// Graceful close
    pub async fn close(mut self) -> Result<()> {
        let _ = self.ws.close(None).await;
        Ok(())
    }

    /// Anything moved into a thread → must be Send
    /// Anything shared between tasks/threads → must be Sync
    /// Anything stored longer than the current stack frame → must be 'static
    pub fn recv_event<F, Fut>(
        mut rx: UnboundedReceiver<RtEvent>,
        ws_sender: WsSender,
        mut callback: F,
    ) where
        F: FnMut(RtEvent, &WsSender) -> Fut + Send + 'static,
        Fut: Future<Output = anyhow::Result<()>> + Send + 'static,
    {
        tokio::spawn(async move {
            loop {
                match rx.recv().await {
                    Some(ev) => {
                        if let Err(e) = callback(ev, &ws_sender).await {
                            error!("event callback error: {:?}", e);
                        }
                    }
                    None => {
                        warn!("event channel closed");
                        break;
                    }
                }

                tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
            }
        });
    }

    // ----------------------------------------------------------------
    // Support TTS accumuration

    /// One-shot TTS using the SAME Realtime session.
    ///
    /// This is the best-practice Realtime pattern:
    /// 1) create a conversation item with `input_text`
    /// 2) request an audio response
    pub async fn tts(&mut self, text: &str, style: Option<&str>) -> Result<()> {
        let t = text.trim();
        if t.is_empty() {
            return Ok(());
        }

        // Put the exact text in a tag to reduce accidental edits
        let say = format!("<say>{}</say>", t);
        let msg = json!({
            "type": "conversation.item.create",
            "item": {
                "type": "message",
                "role": "user",
                "content": [
                    { "type": "input_text", "text": text }
                ]
            }
        });
        self.send_json(msg).await?;
        // Keep using conversation item create (as you already do)
        let msg = json!({
            "type": "conversation.item.create",
            "item": {
                "type": "message",
                "role": "user",
                "content": [
                    { "type": "input_text", "text": say }
                ]
            }
        });
        self.send_json(msg).await?;

        let modalities = vec!["audio", "text"];
        let instructions = style.unwrap_or(
            "You are a text-to-speech renderer. Read aloud EXACTLY the text inside <say>...</say>. \
            Do not add any words. Do not remove any words. Do not paraphrase. Do not say acknowledgements.",
        );
        let msg = json!({
            "type": "response.create",
            "response": {
                "modalities": modalities,
                "instructions": instructions
            }
        });
        self.send_json(msg).await?;

        Ok(())
    }
}
