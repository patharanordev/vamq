use crate::audio::upsampling::general::rate24to48::Pcm24kTo48kF32;
use crate::transform::unrealengine::framer::UeFramer;

use std::sync::Arc;
use tokio::sync::Mutex;

pub struct UeAudioStream {
    pub resampler: Pcm24kTo48kF32, // 24k PCM16 -> 48k f32
    pub framer: UeFramer,          // 48k f32 -> 160ms frames (30720 bytes)
}

impl UeAudioStream {
    pub fn new() -> anyhow::Result<Self> {
        // 20ms @ 24k = 480 samples as resampler processing chunk
        Ok(Self {
            resampler: Pcm24kTo48kF32::new(480)?,
            framer: UeFramer::new(),
        })
    }

    pub fn reset(&mut self) {
        self.framer.reset();
        self.resampler.reset();
    }
}

pub type SharedUeAudioStream = Arc<Mutex<UeAudioStream>>;
