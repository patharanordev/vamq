pub const OPENAI_REALTIME_WS: &str = "wss://api.openai.com/v1/realtime";
pub const REALTIME_TTS_INSTRUCTION: &str = r#"
Multilingual TTS.
Read ONLY text in <<<READ>>>…<<<END>>>.
No replies, no paraphrasing.
Auto-detect language.
If text starts with [emotion] or [emotion:intensity], DO NOT speak the tag; use prosody only.
"#;