use crate::audio::upsampling::general::rate16to24::pcm16le_bytes_to_f32_mono;

use anyhow::Result;
use rubato::{FftFixedInOut, Resampler};

#[inline]
pub fn f32_to_le_bytes(input: &[f32]) -> Vec<u8> {
    let mut out = Vec::with_capacity(input.len() * 4);
    for &x in input {
        out.extend_from_slice(&x.to_le_bytes());
    }
    out
}

/// Streaming mono resampler 24k PCM16 -> 48k F32
pub struct Pcm24kTo48kF32 {
    inner: FftFixedInOut<f32>,
    in_chunk: usize,
    buf_in: Vec<f32>,
}

impl Pcm24kTo48kF32 {
    /// `in_chunk` is the processing block at 24k (e.g. 480 samples = 20ms).
    pub fn new(in_chunk: usize) -> Result<Self> {
        // Same constructor style as your old file (4 args).
        let inner = FftFixedInOut::<f32>::new(24_000, 48_000, in_chunk, 1)?;
        Ok(Self {
            inner,
            in_chunk,
            buf_in: Vec::with_capacity(in_chunk * 4),
        })
    }

    /// Push PCM16LE bytes (24k mono) -> return produced F32 samples (48k mono)
    pub fn push(&mut self, src_24k_pcm16: &[u8]) -> Result<Vec<f32>> {
        let mut out_f32 = Vec::<f32>::new();

        let mut f32_in = pcm16le_bytes_to_f32_mono(src_24k_pcm16);
        self.buf_in.append(&mut f32_in);

        while self.buf_in.len() >= self.in_chunk {
            let block = self.buf_in[..self.in_chunk].to_vec();
            self.buf_in.drain(..self.in_chunk);

            let ch = vec![block];
            let produced = self.inner.process(&ch, None)?;
            out_f32.extend_from_slice(&produced[0]);
        }

        Ok(out_f32)
    }

    pub fn flush(&mut self) -> Result<Vec<f32>> {
        if self.buf_in.is_empty() {
            return Ok(Vec::new());
        }
        let need = self.in_chunk - self.buf_in.len();
        self.buf_in.extend(std::iter::repeat_n(0.0, need));

        let ch = vec![self.buf_in.split_off(0)];
        let produced = self.inner.process(&ch, None)?;
        Ok(produced[0].clone())
    }

    pub fn reset(&mut self) {
        self.buf_in.clear();
    }
}
