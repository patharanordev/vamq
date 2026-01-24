use secrecy::SecretString;
use serde::Deserialize;

#[derive(Deserialize, Debug, Default, Clone)]
pub struct OpenAiConfig {
    #[serde(default)]
    pub api_key: SecretString,
    #[serde(default = "default_model_realtime")]
    #[allow(dead_code)]
    pub model_realtime: String,
    #[serde(default = "default_model_transcribe")]
    #[allow(dead_code)]
    pub model_transcribe: String,
    #[serde(default = "default_sample_rate")]
    pub sample_rate: u32,
    #[serde(default)]
    pub transcription_prompt: Option<String>,
    #[serde(default)]
    pub transcription_language: Option<String>,
}

fn default_model_realtime() -> String {
    "gpt-4o-realtime-preview-2024-12-17".to_string()
}

fn default_model_transcribe() -> String {
    "whisper-1".to_string()
}

fn default_sample_rate() -> u32 {
    24_000
}
