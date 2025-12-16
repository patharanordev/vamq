use crate::audio::{
    upsampling::general::rate16to24::Pcm16kTo24k,
    vad::schema::{VadCommit, VadHeader},
};
use crate::queues::create_consumer;
use anyhow::Result;
use tracing::{debug, warn};

pub struct VadConsumer {
    pub pull: Option<zmq::Socket>,
    pub up: Pcm16kTo24k,
    pub acc: Vec<u8>,
    pub min_commit_bytes: usize,
    pub target_sr: u32,
}

impl VadConsumer {
    pub fn new(
        address: &str,
        target_sr: u32,          // e.g. cfg.openai.sample_rate (24_000)
        min_commit_bytes: usize, // e.g. min_commit_bytes
        in_chunk_16k: usize,     // e.g. 480 samples @ 16k
    ) -> Result<Self> {
        let maybe_pull = create_consumer(address);
        let up = Pcm16kTo24k::new(in_chunk_16k)?;

        Ok(Self {
            pull: maybe_pull,
            up,
            acc: Vec::with_capacity(min_commit_bytes * 2),
            min_commit_bytes,
            target_sr,
        })
    }

    pub fn recv(&mut self, timeout_ms: i64) -> Result<Option<VadCommit>> {
        if let Some(ref pull) = self.pull {
            let mut items = [pull.as_poll_item(zmq::POLLIN)];
            let n = zmq::poll(&mut items, timeout_ms)?; // 10 ms timeout

            if n > 0 && items[0].is_readable() {
                let header_bytes = pull.recv_bytes(0)?;
                let header: VadHeader = match serde_json::from_slice(&header_bytes) {
                    Ok(h) => h,
                    Err(e) => {
                        warn!("bad VAD header JSON: {e}");
                        return Ok(None);
                    }
                };

                let mut payload = pull.recv_bytes(0)?;
                debug!(
                    "ingress: session={} seq={} bytes={} flags=0b{:03b} sr={} ch={} fmt={}",
                    header.session_id,
                    header.seq,
                    payload.len(),
                    header.flags,
                    header.sr,
                    header.ch,
                    header.fmt
                );

                if !(header.fmt == "s16le" && header.ch == 1) {
                    warn!("unsupported audio fmt/ch: {} ch={}", header.fmt, header.ch);
                    return Ok(None);
                }

                let is_start = (header.flags & 0b001) != 0;
                let is_end = (header.flags & 0b010) != 0;
                let _preroll = (header.flags & 0b100) != 0;

                if is_start {
                    self.acc.clear();
                    self.up.reset();
                }

                if !payload.is_empty() {
                    if header.sr == self.target_sr {
                        self.acc.append(&mut payload);
                    } else if header.sr == 16_000 {
                        let mut up_bytes = self.up.push(&payload)?;
                        if !up_bytes.is_empty() {
                            self.acc.append(&mut up_bytes);
                        }
                    } else {
                        warn!("unsupported input sr={} (expect 16k or 24k)", header.sr);
                    }
                    // last_append_at = Instant::now(); // only if you re-enable silence flush
                }

                if is_end {
                    if header.sr == 16_000 {
                        let mut tail = self.up.flush()?;
                        if !tail.is_empty() {
                            self.acc.append(&mut tail);
                        }
                    }
                    if !self.acc.is_empty() && self.acc.len() < self.min_commit_bytes {
                        self.acc.resize(self.min_commit_bytes, 0);
                    }

                    let pcm24k = std::mem::take(&mut self.acc);

                    return Ok(Some(VadCommit {
                        session_id: header.session_id,
                        seq: header.seq,
                        pcm24k_s16le: pcm24k,
                    }));
                }
            }
        }

        Ok(None)
    }
}
