use serde::{Deserialize, Serialize};
use serde_json::Value;

/// What you get back from `next_event()`
/// #[non_exhaustive] means you can add new variants later without breaking external matches.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum RtEvent {
    /// PCM16 bytes from `response.output_audio.delta`
    AudioDelta(Vec<u8>), // response.output_audio.delta
    TextDelta(String), // response.output_text.delta

    /// Transcript of user (when enable_input_transcription=true)
    UserTranscriptDelta(String),
    UserTranscriptFinal(String),

    /// Transcript of assistant (when enable_output_audio_transcript=true)
    AssistantTranscriptDelta(String),

    /// A part of audio, text, transcription and others done
    PartDone(Value),

    /// The response is done (good place to rotate buffers/timestamps)
    ResponseDone(Value),

    /// response.completed
    Completed,
    SessionCreated(Value),
    /// OpenAI sent an error frame
    Error(String),
    /// Anything else you might want to log
    Other(Value),
    /// Underlying socket closed
    Closed,
    Idle,
}

#[derive(Debug, Clone, Copy)]
pub enum RealtimeProfile {
    S2S,
    TRANSCRIBE,
    TTS,
}

#[derive(Clone, Debug)]
pub struct RealtimeFeatures {
    pub profile: RealtimeProfile,

    pub enable_conversation: bool,

    pub enable_transcribe: bool,

    // transcript user's audio
    pub enable_input_transcription: bool,

    /// transcript output audio (for debug)
    pub enable_output_audio_transcript: bool,

    /// use server VAD or external VAD (if you have own VAD then set to -> false)
    pub use_server_vad: bool,
}

impl Default for RealtimeFeatures {
    fn default() -> Self {
        Self {
            profile: RealtimeProfile::S2S,
            enable_conversation: false,
            enable_transcribe: false,
            enable_input_transcription: false,
            enable_output_audio_transcript: false,
            use_server_vad: false,
        }
    }
}

impl RealtimeFeatures {
    pub fn from_profile(profile: RealtimeProfile) -> Self {
        match profile {
            RealtimeProfile::S2S => Self {
                profile,
                enable_conversation: true,
                enable_transcribe: false,
                enable_input_transcription: false,
                enable_output_audio_transcript: false,
                use_server_vad: false,
            },
            RealtimeProfile::TRANSCRIBE => Self {
                profile,
                enable_conversation: false,
                enable_transcribe: true,
                enable_input_transcription: true,
                enable_output_audio_transcript: false,
                use_server_vad: false,
            },
            RealtimeProfile::TTS => Self {
                profile,
                enable_conversation: true,
                enable_transcribe: false,
                enable_input_transcription: false,
                enable_output_audio_transcript: false,
                use_server_vad: false,
            },
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InputAudioTranscription {
    pub model: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prompt: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub language: Option<String>,
}

impl Default for InputAudioTranscription {
    fn default() -> Self {
        Self {
            model: "whisper-1".to_string(),
            prompt: None,
            language: None,
        }
    }
}

pub struct RealtimeClientOptions {
    pub features: RealtimeFeatures,
    pub instructions: Option<String>,
    pub input_audio_transcription: Option<InputAudioTranscription>,
}

impl RealtimeClientOptions {
    pub fn new(features: RealtimeFeatures) -> Self {
        Self {
            features,
            instructions: None,
            input_audio_transcription: None,
        }
    }

    pub fn with_instructions(mut self, instructions: &str) -> Self {
        self.instructions = Some(instructions.to_string());
        self
    }

    pub fn with_input_audio_transcription(
        mut self,
        input_audio_transcription: InputAudioTranscription,
    ) -> Self {
        self.input_audio_transcription = Some(input_audio_transcription);
        self
    }
}

impl Default for RealtimeClientOptions {
    fn default() -> Self {
        Self {
            features: RealtimeFeatures::default(),
            instructions: Some("You are a helpful multiple languages speaking assistant, reply in the user’s language.".to_string()),
            input_audio_transcription: None,
        }
    }
}

pub struct ResponseOptions {
    pub instructions: String,
    pub item_ids: Option<Vec<String>>,
    pub is_out_of_band: bool, // ถ้า true จะเซต conversation: "none"
}
