use byteorder::{ByteOrder, LittleEndian, WriteBytesExt};
use serde::{Deserialize, Serialize};
use std::io::Write;

///  Basic framing for ZMQ payloads:
///  [u64 seq][u64 unix_nanos][u32 sample_rate][u16 channels][u16 format_tag][bytes pcm]
///  format_tag: 1 = PCM16LE, 3 = float32
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AudioFrameHeader {
    pub seq: u64,
    pub ts_unix_nanos: u64,
    pub sample_rate: u32,
    pub channels: u16,
    pub format_tag: u16, // 1=PCM16LE, 3=FLOAT32 (WAVE_FORMAT_IEEE_FLOAT)
}

pub const FORMAT_PCM16LE: u16 = 1;
pub const FORMAT_F32: u16 = 3;

pub fn pack_frame(h: &AudioFrameHeader, payload: &[u8]) -> Vec<u8> {
    let mut out = vec![0u8; 8 + 8 + 4 + 2 + 2];
    LittleEndian::write_u64(&mut out[0..8], h.seq);
    LittleEndian::write_u64(&mut out[8..16], h.ts_unix_nanos);
    LittleEndian::write_u32(&mut out[16..20], h.sample_rate);
    LittleEndian::write_u16(&mut out[20..22], h.channels);
    LittleEndian::write_u16(&mut out[22..24], h.format_tag);
    out.extend_from_slice(payload);
    out
}

pub fn unpack_frame(buf: &[u8]) -> Option<(AudioFrameHeader, &[u8])> {
    if buf.len() < 24 {
        return None;
    }
    let seq = LittleEndian::read_u64(&buf[0..8]);
    let ts_unix_nanos = LittleEndian::read_u64(&buf[8..16]);
    let sample_rate = LittleEndian::read_u32(&buf[16..20]);
    let channels = LittleEndian::read_u16(&buf[20..22]);
    let format_tag = LittleEndian::read_u16(&buf[22..24]);
    Some((
        AudioFrameHeader {
            seq,
            ts_unix_nanos,
            sample_rate,
            channels,
            format_tag,
        },
        &buf[24..],
    ))
}

// Save audio frames to a WAV (24 kHz mono)
pub fn write_wav_pcm16_mono_24k(path: &str, samples: &[u8]) -> anyhow::Result<()> {
    let mut f = std::fs::File::create(path)?;

    let data_len = samples.len() as u32;
    let byte_rate = 24_000u32 * 16 / 8; // ref. 24_000u32 * 1 * 16 / 8; by 1 is number of channel
    let block_align = 16 / 8; // ref. 1 * 16 / 8; by 1 is channels is number of channel
    let subchunk2_size = data_len;

    // RIFF header
    f.write_all(b"RIFF")?;
    f.write_u32::<LittleEndian>(36 + subchunk2_size)?; // file size - 8
    f.write_all(b"WAVE")?;

    // fmt chunk
    f.write_all(b"fmt ")?;
    f.write_u32::<LittleEndian>(16)?; // PCM
    f.write_u16::<LittleEndian>(1)?; // PCM format
    f.write_u16::<LittleEndian>(1)?; // channels
    f.write_u32::<LittleEndian>(24_000)?; // sample rate
    f.write_u32::<LittleEndian>(byte_rate)?; // byte rate
    f.write_u16::<LittleEndian>(block_align as u16)?; // block align
    f.write_u16::<LittleEndian>(16)?; // bits per sample

    // data chunk
    f.write_all(b"data")?;
    f.write_u32::<LittleEndian>(subchunk2_size)?;
    f.write_all(samples)?;
    Ok(())
}
