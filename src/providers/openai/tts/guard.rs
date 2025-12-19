use tokio::time::{Duration, Instant};

/// A tiny guard to smooth LLM chunking before feeding Realtime TTS.
///
/// Why: appending per-token (or very small chunks) tends to produce jittery prosody
/// and can destabilize avatar lipsync. This guard merges tiny chunks and splits
/// overly large ones.
///
/// Recommended defaults for S2S avatars:
/// - flush on punctuation OR >= ~180 chars OR every ~300ms
///
/// Buffers incoming text chunks and emits "safe" chunks for TTS.
/// Goal: avoid per-token appends, avoid giant paragraphs, keep cadence smooth.
pub struct TtsChunkGuard {
    buf: String,
    last_flush: Instant,

    // thresholds (tune as needed)
    min_chars: usize,      // flush if >= this and boundary found
    max_chars: usize,      // flush if exceeds this regardless
    max_wait: Duration,    // flush if waiting too long (cadence)
}

impl Default for TtsChunkGuard {
    fn default() -> Self {
        Self {
            buf: String::new(),
            last_flush: Instant::now(),
            min_chars: 90,                 // ~10–20 words
            max_chars: 260,                // ~1–2 sentences
            max_wait: Duration::from_millis(320),
        }
    }
}

impl TtsChunkGuard {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_limits(min_chars: usize, max_chars: usize, max_wait: Duration) -> Self {
        Self {
            buf: String::new(),
            last_flush: Instant::now(),
            min_chars,
            max_chars,
            max_wait,
        }
    }

    /// Push an incoming LLM chunk. Returns Some(text_to_append) when it's a good time to flush.
    pub fn push(&mut self, chunk: &str) -> Option<String> {
        if chunk.is_empty() {
            return None;
        }

        self.buf.push_str(chunk);

        let waited = self.last_flush.elapsed();
        let len = self.buf.chars().count();

        // If we have too much buffered, flush aggressively at a boundary if possible
        if len >= self.max_chars {
            let out = self.flush_at_best_boundary_or_hard();
            self.last_flush = Instant::now();
            return Some(out);
        }

        // If we've waited long enough, flush (even if no punctuation, but try boundary)
        if waited >= self.max_wait && len >= self.min_chars {
            let out = self.flush_at_best_boundary_or_hard();
            self.last_flush = Instant::now();
            return Some(out);
        }

        // If we hit a sentence/phrase boundary and have enough text, flush
        if len >= self.min_chars && ends_with_boundary(&self.buf) {
            let out = std::mem::take(&mut self.buf);
            self.last_flush = Instant::now();
            return Some(out);
        }

        None
    }

    /// Flush remaining buffered text at end of turn.
    pub fn finish(&mut self) -> Option<String> {
        if self.buf.trim().is_empty() {
            self.buf.clear();
            return None;
        }
        self.last_flush = Instant::now();
        Some(std::mem::take(&mut self.buf))
    }

    fn flush_at_best_boundary_or_hard(&mut self) -> String {
        // Try to cut at last boundary within a window near the end
        // so we don’t cut too early.
        let s = self.buf.as_str();

        if let Some(idx) = find_last_reasonable_boundary(s) {
            let out = s[..idx].to_string();
            let remain = s[idx..].to_string();
            self.buf = remain;
            return out;
        }

        // If no boundary found, hard flush everything
        std::mem::take(&mut self.buf)
    }
}

fn ends_with_boundary(s: &str) -> bool {
    let t = s.trim_end();
    t.ends_with('.') || t.ends_with('?') || t.ends_with('!') || t.ends_with('\n') ||
    t.ends_with(',') || t.ends_with(':') || t.ends_with(';')
}

/// Find a boundary near the end: prefer ".?!\n", fallback to ",:; "
/// Return index (byte index) where we split.
fn find_last_reasonable_boundary(s: &str) -> Option<usize> {
    // search window: last ~320 bytes (ok for ascii-ish text; safe enough)
    let start = s.len().saturating_sub(320);
    let window = &s[start..];

    let mut best: Option<usize> = None;

    for (i, ch) in window.char_indices() {
        let abs = start + i;
        if matches!(ch, '.' | '?' | '!' | '\n') {
            best = Some(abs + ch.len_utf8());
        }
    }
    if best.is_some() {
        return best;
    }

    for (i, ch) in window.char_indices() {
        let abs = start + i;
        if matches!(ch, ',' | ':' | ';') {
            best = Some(abs + ch.len_utf8());
        }
    }
    best
}