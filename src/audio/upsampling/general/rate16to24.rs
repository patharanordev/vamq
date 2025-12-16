use anyhow::Result;
use rubato::{FftFixedInOut, Resampler};

// streaming mono resampler (16k → 24k)

#[inline]
pub fn min_bytes_24k_pcm16(ms: u32) -> usize {
    // 24_000 * 2 bytes * ms/1000
    (24_000usize * 2 * ms as usize) / 1000
}

#[inline]
pub fn pcm16le_bytes_to_f32_mono(input: &[u8]) -> Vec<f32> {
    // input length must be even; assume LE
    let mut out = Vec::with_capacity(input.len() / 2);
    for i in (0..input.len()).step_by(2) {
        let v = i16::from_le_bytes([input[i], input[i + 1]]);
        out.push(v as f32 / 32768.0);
    }
    out
}

#[inline]
pub fn f32_to_pcm16_bytes(input: &[f32]) -> Vec<u8> {
    let mut out = Vec::with_capacity(input.len() * 2);
    for &x in input {
        let s = (x.clamp(-1.0, 1.0) * i16::MAX as f32).round() as i16;
        out.extend_from_slice(&s.to_le_bytes());
    }
    out
}

/// Streaming mono resampler 16k -> 24k using FFT-based fixed-ratio resampler.
/// Feeds audio in small pieces; processes in `in_chunk` blocks when available.
pub struct Pcm16kTo24k {
    inner: FftFixedInOut<f32>,
    in_chunk: usize,
    buf_in: Vec<f32>,
}

impl Pcm16kTo24k {
    /// `in_chunk` is the processing block at 16k (e.g. 480 samples ≈ 30 ms).
    pub fn new(in_chunk: usize) -> Result<Self> {
        let inner = FftFixedInOut::<f32>::new(16_000, 24_000, in_chunk, 1)?;
        Ok(Self {
            inner,
            in_chunk,
            buf_in: Vec::with_capacity(in_chunk * 2),
        })
    }

    /// Push arbitrary PCM16LE bytes at 16k, get back ready 24k bytes.
    pub fn push(&mut self, src_16k_bytes: &[u8]) -> Result<Vec<u8>> {
        let mut out_bytes = Vec::new();

        // Convert to f32 mono and append to staging buffer
        let mut f32_in = pcm16le_bytes_to_f32_mono(src_16k_bytes);
        self.buf_in.append(&mut f32_in);

        // Process as many full in_chunk blocks as available
        while self.buf_in.len() >= self.in_chunk {
            let block = self.buf_in[..self.in_chunk].to_vec();
            self.buf_in.drain(..self.in_chunk);

            let ch = vec![block];
            let produced = self.inner.process(&ch, None)?;
            let out_f32 = &produced[0];
            let mut bytes = f32_to_pcm16_bytes(out_f32);
            out_bytes.append(&mut bytes);
        }

        Ok(out_bytes)
    }

    /// Flush tail (pad with zeros to nearest block), return any remaining output.
    pub fn flush(&mut self) -> Result<Vec<u8>> {
        if self.buf_in.is_empty() {
            return Ok(Vec::new());
        }
        // Zero-pad to full block
        let need = self.in_chunk - self.buf_in.len();
        self.buf_in.extend(std::iter::repeat_n(0.0, need));

        let ch = vec![self.buf_in.split_off(0)];
        let produced = self.inner.process(&ch, None)?;
        Ok(f32_to_pcm16_bytes(&produced[0]))
    }

    /// Clear staging buffer (use on VAD start/end to reset).
    pub fn reset(&mut self) {
        self.buf_in.clear();
    }
}
