import streamlit as st
import asyncio
import json
from streamlit_webrtc import webrtc_streamer, WebRtcMode, RTCConfiguration
import requests
from typing import Dict
import av

# Configure the page
st.set_page_config(
    page_title="Anganwadi Assistant",
    page_icon="🎙️",
    layout="wide"
)

# Custom CSS for better UI
st.markdown("""
<style>
    .main-header {
        text-align: center;
        color: #1f77b4;
        padding: 20px;
    }
    .status-box {
        padding: 10px;
        border-radius: 5px;
        margin: 10px 0;
    }
    .transcript-box {
        background-color: #f0f2f6;
        padding: 15px;
        border-radius: 10px;
        height: 400px;
        overflow-y: auto;
    }
</style>
""", unsafe_allow_html=True)

# Header
st.markdown("<h1 class='main-header'>🎙️ Anganwadi Voice Assistant</h1>", unsafe_allow_html=True)
st.markdown("<p style='text-align: center;'>आंगनवाड़ी और पोषण ट्रैकर के बारे में प्रश्न पूछें</p>", unsafe_allow_html=True)

# Create columns for layout
col1, col2 = st.columns([2, 1])

with col1:
    st.subheader("Voice Interface")
    
    # WebRTC configuration
    rtc_config = RTCConfiguration({
        "iceServers": [{"urls": ["stun:stun.l.google.com:19302"]}]
    })
    
    # Custom audio processor for Pipecat integration
    class PipecatAudioProcessor:
        def __init__(self):
            self.server_url = "http://localhost:6010"
            self.pc_id = None
            
        async def process_offer(self, offer):
            """Send offer to Pipecat server and get answer"""
            response = requests.post(
                f"{self.server_url}/api/offer",
                json={
                    "sdp": offer["sdp"],
                    "type": offer["type"],
                    "pc_id": self.pc_id
                }
            )
            answer = response.json()
            self.pc_id = answer.get("pc_id")
            return answer
    
    # Initialize processor
    if 'processor' not in st.session_state:
        st.session_state.processor = PipecatAudioProcessor()
    
    # WebRTC streamer with custom settings
    webrtc_ctx = webrtc_streamer(
        key="pipecat-voice-assistant",
        mode=WebRtcMode.SENDRECV,
        rtc_configuration=rtc_config,
        audio_receiver_size=256,
        media_stream_constraints={
            "audio": {
                "echoCancellation": True,
                "noiseSuppression": True,
                "autoGainControl": True,
            },
            "video": False,
        },
        async_processing=True,
    )
    
    # Status indicator
    if webrtc_ctx.state.playing:
        st.success("🔴 Connected - Speak now!")
    else:
        st.info("⚪ Click 'START' to begin conversation")

with col2:
    st.subheader("📝 Conversation Transcript")
    
    # Initialize transcript storage
    if 'transcript' not in st.session_state:
        st.session_state.transcript = []
    
    # Display transcript
    transcript_container = st.container()
    with transcript_container:
        st.markdown('<div class="transcript-box">', unsafe_allow_html=True)
        for message in st.session_state.transcript:
            if message['role'] == 'user':
                st.markdown(f"**👤 User:** {message['content']}")
            else:
                st.markdown(f"**🤖 Assistant:** {message['content']}")
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Clear transcript button
    if st.button("🗑️ Clear Transcript"):
        st.session_state.transcript = []
        st.rerun()

# Information section
with st.expander("ℹ️ How to use"):
    st.markdown("""
    1. Click **START** to begin the conversation
    2. Allow microphone access when prompted
    3. Speak your question in Hindi or English
    4. The assistant will respond with voice
    5. Your conversation will appear in the transcript
    
    **Example questions:**
    - आंगनवाड़ी केंद्र क्या है?
    - पोषण ट्रैकर कैसे काम करता है?
    - गर्भवती महिलाओं के लिए क्या सेवाएं हैं?
    """)

# Alternative: Custom WebSocket handler for better integration
st.markdown("---")
st.subheader("Alternative: Direct WebSocket Connection")

if st.button("Connect via WebSocket"):
    st.code("""
    # You can also connect directly using WebSocket
    ws_url = "ws://localhost:6010/ws"
    # Implementation would go here
    """)