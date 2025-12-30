#[cfg(any(
    feature = "audio-upsampling",
    feature = "audio-upsampling-general",
    feature = "vad"
))]
pub mod audio;
#[cfg(feature = "openai")]
pub mod providers;
#[cfg(any(feature = "zmq", feature = "ws"))]
pub mod queues;
#[cfg(any(feature = "transform-audio", feature = "transform-datetime"))]
pub mod transform;

#[cfg(any(feature = "audio-upsampling", feature = "audio-upsampling-general"))]
pub use audio::upsampling::general::rate16to24::min_bytes_24k_pcm16;
#[cfg(any(feature = "vad", feature = "vad-consumer"))]
pub use audio::vad::consumer::VadConsumer;
#[cfg(feature = "openai")]
pub use providers::openai::{
    RealtimeClient, RtEvent, SharedClient,
    schema::{RealtimeFeatures, RealtimeProfile},
    tts::guard::TtsChunkGuard,
};
#[cfg(feature = "ws")]
pub use queues::wsg_pub::{WsSender, connect_ws, ws_send_pcm16};
#[cfg(feature = "transform-audio")]
pub use transform::framing::write_wav_pcm16_mono_24k;
#[cfg(feature = "transform-datetime")]
pub use transform::time::now_unix_nanos;
