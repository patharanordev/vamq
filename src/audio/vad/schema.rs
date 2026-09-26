use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct VadHeader {
    pub session_id: String,
    // Stable id for the physical VAD client, independent of session_id
    // (which is minted fresh per process launch). Optional: older
    // producers won't send it, and serde simply defaults it to `None`.
    #[serde(default)]
    pub device_id: Option<String>,
    pub seq: u64,
    pub ts_ns: u64,
    pub sr: u32,
    pub ch: u16,
    pub fmt: String, // "s16le"
    pub flags: u32,  // bit0=start, bit1=end, bit2=preroll
}

#[derive(Debug, Deserialize)]
pub struct VadCommit {
    pub session_id: String,
    #[serde(default)]
    pub device_id: Option<String>,
    pub seq: u64,
    pub pcm24k_s16le: Vec<u8>,
}
