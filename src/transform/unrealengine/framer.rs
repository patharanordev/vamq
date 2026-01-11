use crate::audio::upsampling::general::rate24to48::f32_to_le_bytes;

pub struct UeFramer {
    // 160ms frame @ 48k
    samples_per_frame: usize, // 7680
    buf: Vec<f32>,
}

impl UeFramer {
    pub fn new() -> Self {
        Self {
            samples_per_frame: 48_000 * 160 / 1000,
            buf: Vec::with_capacity(48_000),
        }
    }

    pub fn push(&mut self, src: &[f32]) -> Vec<Vec<u8>> {
        self.buf.extend_from_slice(src);

        let mut frames = Vec::new();
        while self.buf.len() >= self.samples_per_frame {
            let frame = self
                .buf
                .drain(..self.samples_per_frame)
                .collect::<Vec<f32>>();
            frames.push(f32_to_le_bytes(&frame)); // 30720 bytes
        }
        frames
    }

    pub fn flush_pad(&mut self) -> Option<Vec<u8>> {
        if self.buf.is_empty() {
            return None;
        }
        let mut frame = vec![0.0f32; self.samples_per_frame];
        let n = self.buf.len().min(self.samples_per_frame);
        frame[..n].copy_from_slice(&self.buf[..n]);
        self.buf.clear();
        Some(f32_to_le_bytes(&frame))
    }

    pub fn reset(&mut self) {
        self.buf.clear();
    }
}

impl Default for UeFramer {
    fn default() -> Self {
        Self::new()
    }
}
