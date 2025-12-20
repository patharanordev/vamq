/// What you get back from `next_event()`
/// #[non_exhaustive] means you can add new variants later without breaking external matches.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum RtEvent {
    /// PCM16 bytes from `response.output_audio.delta`
    AudioDelta(Vec<u8>), // response.output_audio.delta
    TextDelta(String), // response.output_text.delta

    // Transcript of user (when enable_input_transcription=true)
    UserTranscriptDelta(String),
    UserTranscriptFinal(String),

    // Transcript of assistant (when enable_output_audio_transcript=true)
    AssistantTranscriptDelta(String),

    /// The response is done (good place to rotate buffers/timestamps)
    Completed, // response.completed
    SessionCreated(serde_json::Value),
    /// OpenAI sent an error frame
    Error(String),
    /// Anything else you might want to log
    Other(serde_json::Value),
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
