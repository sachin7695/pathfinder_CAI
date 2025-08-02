import streamlit as st
import asyncio
import os
import json
import time
from datetime import datetime
from typing import Optional, Dict, Any
import threading
import logging

# LiveKit imports
try:
    from livekit import api, rtc
    from livekit.api import AccessToken, VideoGrants
    LIVEKIT_AVAILABLE = True
except ImportError:
    LIVEKIT_AVAILABLE = False
    st.warning("LiveKit SDK not installed. Running in demo mode.")

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Page configuration
st.set_page_config(
    page_title="LiveKit Sales Demo - Enhanced",
    page_icon="🤖",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS with enhanced styling
st.markdown("""
<style>
    .main-header {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 2rem;
        border-radius: 15px;
        color: white;
        text-align: center;
        margin-bottom: 2rem;
        box-shadow: 0 8px 32px 0 rgba(31, 38, 135, 0.37);
    }
    
    .status-card {
        padding: 1rem;
        border-radius: 10px;
        margin: 1rem 0;
        font-weight: bold;
        text-align: center;
        backdrop-filter: blur(10px);
    }
    
    .status-connected {
        background: rgba(76, 175, 80, 0.3);
        border: 2px solid #4CAF50;
        color: #2E7D32;
    }
    
    .status-disconnected {
        background: rgba(244, 67, 54, 0.3);
        border: 2px solid #f44336;
        color: #C62828;
    }
    
    .status-connecting {
        background: rgba(255, 193, 7, 0.3);
        border: 2px solid #ff9800;
        color: #F57C00;
    }
    
    .participant-card {
        background: linear-gradient(45deg, rgba(255,255,255,0.1), rgba(255,255,255,0.05));
        padding: 15px;
        border-radius: 10px;
        margin: 10px 0;
        border: 1px solid rgba(255,255,255,0.2);
        backdrop-filter: blur(5px);
    }
    
    .metric-card {
        background: rgba(255,255,255,0.1);
        padding: 1rem;
        border-radius: 8px;
        text-align: center;
    }
    
    .audio-visualizer {
        height: 40px;
        background: linear-gradient(90deg, #4CAF50, #8BC34A);
        border-radius: 20px;
        animation: pulse 2s infinite;
    }
    
    @keyframes pulse {
        0% { opacity: 0.6; }
        50% { opacity: 1; }
        100% { opacity: 0.6; }
    }
</style>
""", unsafe_allow_html=True)

class LiveKitManager:
    """Manages LiveKit connection and operations"""
    
    def __init__(self):
        self.room: Optional[rtc.Room] = None
        self.audio_track: Optional[rtc.LocalAudioTrack] = None
        self.connected = False
        self.participants = {}
        
    async def generate_token(self, participant_name: str, room_name: str) -> str:
        """Generate access token for LiveKit"""
        if not LIVEKIT_AVAILABLE:
            return "DEMO_TOKEN"
            
        token = AccessToken(
            api_key=os.getenv("LIVEKIT_API_KEY", "APIAMrTXLVoxLqe"),
            api_secret=os.getenv("LIVEKIT_API_SECRET", "3pFSQsUzLLeEEWrvO1hJaP4QA97CNeoMEkQA6wWSkuS")
        )
        
        token.with_identity(participant_name)
        token.with_name(participant_name)
        token.with_grants(VideoGrants(
            room_join=True,
            room=room_name,
            can_publish=True,
            can_subscribe=True,
            can_publish_data=True
        ))
        
        return token.to_jwt()
    
    async def connect_to_room(self, room_name: str, participant_name: str) -> bool:
        """Connect to LiveKit room"""
        try:
            if not LIVEKIT_AVAILABLE:
                # Simulate connection for demo
                await asyncio.sleep(2)
                self.connected = True
                return True
                
            self.room = rtc.Room()
            
            # Set up event handlers
            self.room.on("participant_connected", self._on_participant_connected)
            self.room.on("participant_disconnected", self._on_participant_disconnected)
            self.room.on("track_subscribed", self._on_track_subscribed)
            self.room.on("data_received", self._on_data_received)
            
            # Generate token and connect
            token = await self.generate_token(participant_name, room_name)
            url = os.getenv("LIVEKIT_URL", "wss://your-livekit-server.livekit.cloud")
            
            await self.room.connect(url, token)
            
            # Enable microphone
            self.audio_track = rtc.create_local_audio_track()
            await self.room.local_participant.publish_track(self.audio_track)
            
            self.connected = True
            logger.info(f"Connected to room {room_name} as {participant_name}")
            return True
            
        except Exception as e:
            logger.error(f"Connection failed: {e}")
            return False
    
    async def disconnect_from_room(self):
        """Disconnect from LiveKit room"""
        if self.room and LIVEKIT_AVAILABLE:
            await self.room.disconnect()
        
        self.room = None
        self.audio_track = None
        self.connected = False
        self.participants = {}
        logger.info("Disconnected from room")
    
    def _on_participant_connected(self, participant):
        """Handle participant connection"""
        self.participants[participant.identity] = {
            "name": participant.identity,
            "audio": False,
            "video": False,
            "connected_at": datetime.now()
        }
        logger.info(f"Participant connected: {participant.identity}")
    
    def _on_participant_disconnected(self, participant):
        """Handle participant disconnection"""
        if participant.identity in self.participants:
            del self.participants[participant.identity]
        logger.info(f"Participant disconnected: {participant.identity}")
    
    def _on_track_subscribed(self, track, publication, participant):
        """Handle track subscription"""
        if participant.identity in self.participants:
            if track.kind == rtc.TrackKind.KIND_AUDIO:
                self.participants[participant.identity]["audio"] = True
            elif track.kind == rtc.TrackKind.KIND_VIDEO:
                self.participants[participant.identity]["video"] = True
        logger.info(f"Subscribed to {track.kind} track from {participant.identity}")
    
    def _on_data_received(self, data, participant):
        """Handle data reception"""
        try:
            message = json.loads(data.decode())
            logger.info(f"Received data from {participant.identity}: {message}")
            # Add message to chat in session state
            if 'chat_messages' in st.session_state:
                timestamp = datetime.now().strftime("%H:%M:%S")
                st.session_state.chat_messages.append({
                    "timestamp": timestamp,
                    "message": message.get("content", str(message)),
                    "sender": "ai" if participant.identity == "AI_Assistant" else "user"
                })
        except Exception as e:
            logger.error(f"Error processing received data: {e}")

# Initialize session state
def initialize_session_state():
    """Initialize Streamlit session state"""
    if 'livekit_manager' not in st.session_state:
        st.session_state.livekit_manager = LiveKitManager()
    
    if 'connected' not in st.session_state:
        st.session_state.connected = False
        st.session_state.muted = False
        st.session_state.chat_messages = []
        st.session_state.connection_status = "disconnected"
        st.session_state.room_stats = {
            "messages_sent": 0,
            "messages_received": 0,
            "connection_time": None
        }

initialize_session_state()

# Main header
st.markdown('''
<div class="main-header">
    <h1>🤖 LiveKit Sales Demo - Enhanced</h1>
    <p>Real-time AI Sales Assistant with WebRTC Integration</p>
</div>
''', unsafe_allow_html=True)

# Sidebar configuration
with st.sidebar:
    st.header("⚙️ LiveKit Configuration")
    
    # Room settings
    room_name = st.text_input("Room Name", value="my_private_sales_room_2024")
    participant_name = st.text_input("Your Name", value=f"Sales_User_{int(time.time())}")
    
    # LiveKit server settings
    st.subheader("🔧 Server Settings")
    api_key = st.text_input("API Key", value="APIAMrTXLVoxLqe", type="password")
    api_secret = st.text_input("API Secret", type="password")
    server_url = st.text_input("Server URL", value="wss://your-livekit-server.livekit.cloud")
    
    # Audio settings
    st.subheader("🎵 Audio Settings")
    audio_quality = st.selectbox("Quality", ["High", "Medium", "Low"])
    echo_cancellation = st.checkbox("Echo Cancellation", value=True)
    noise_suppression = st.checkbox("Noise Suppression", value=True)
    auto_gain = st.checkbox("Auto Gain Control", value=True)
    
    # Debug mode
    debug_mode = st.checkbox("Debug Mode", value=False)
    
    if debug_mode:
        st.subheader("🐛 Debug Info")
        st.json({
            "LiveKit Available": LIVEKIT_AVAILABLE,
            "Connected": st.session_state.connected,
            "Participants": len(st.session_state.livekit_manager.participants),
            "Room Stats": st.session_state.room_stats
        })

# Main content area
col1, col2 = st.columns([1, 1])

with col1:
    st.header("🔗 Connection Management")
    
    # Status display with enhanced styling
    status_text = {
        "connected": "🟢 Connected to Sales Room!",
        "disconnected": "🔴 Disconnected",
        "connecting": "🟡 Connecting..."
    }
    
    status_class = f"status-{st.session_state.connection_status}"
    st.markdown(f'<div class="status-card {status_class}">{status_text[st.session_state.connection_status]}</div>', 
                unsafe_allow_html=True)
    
    # Connection controls
    col_btn1, col_btn2, col_btn3 = st.columns(3)
    
    with col_btn1:
        if st.button("🚀 Connect", disabled=st.session_state.connected, use_container_width=True):
            st.session_state.connection_status = "connecting"
            
            with st.spinner("Connecting to LiveKit room..."):
                # Run async connection
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                
                success = loop.run_until_complete(
                    st.session_state.livekit_manager.connect_to_room(room_name, participant_name)
                )
                
                if success:
                    st.session_state.connected = True
                    st.session_state.connection_status = "connected"
                    st.session_state.room_stats["connection_time"] = datetime.now()
                    
                    # Add system messages
                    timestamp = datetime.now().strftime("%H:%M:%S")
                    st.session_state.chat_messages.extend([
                        {"timestamp": timestamp, "message": "Connected to the sales room!", "sender": "system"},
                        {"timestamp": timestamp, "message": "AI Assistant is ready to help! 🤖", "sender": "ai"}
                    ])
                else:
                    st.session_state.connection_status = "disconnected"
                    st.error("Failed to connect to room")
                
                loop.close()
                st.rerun()
    
    with col_btn2:
        if st.button("⏹️ Disconnect", disabled=not st.session_state.connected, use_container_width=True):
            with st.spinner("Disconnecting..."):
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                
                loop.run_until_complete(st.session_state.livekit_manager.disconnect_from_room())
                
                st.session_state.connected = False
                st.session_state.connection_status = "disconnected"
                st.session_state.muted = False
                
                timestamp = datetime.now().strftime("%H:%M:%S")
                st.session_state.chat_messages.append({
                    "timestamp": timestamp, 
                    "message": "Disconnected from room", 
                    "sender": "system"
                })
                
                loop.close()
                st.rerun()
    
    with col_btn3:
        mute_icon = "🔇" if st.session_state.muted else "🎤"
        mute_text = f"{mute_icon} {'Unmute' if st.session_state.muted else 'Mute'}"
        
        if st.button(mute_text, disabled=not st.session_state.connected, use_container_width=True):
            st.session_state.muted = not st.session_state.muted
            status = "muted" if st.session_state.muted else "unmuted"
            
            timestamp = datetime.now().strftime("%H:%M:%S")
            st.session_state.chat_messages.append({
                "timestamp": timestamp,
                "message": f"Microphone {status}",
                "sender": "system"
            })
            st.rerun()

    # Audio visualization
    if st.session_state.connected and not st.session_state.muted:
        st.markdown('<div class="audio-visualizer"></div>', unsafe_allow_html=True)
        st.caption("🎵 Audio active")

    # Participants section
    st.header("👥 Active Participants")
    
    participants = st.session_state.livekit_manager.participants
    if st.session_state.connected:
        # Add current user
        participants["self"] = {
            "name": participant_name,
            "audio": not st.session_state.muted,
            "video": False,
            "connected_at": st.session_state.room_stats.get("connection_time", datetime.now())
        }
        
        # Add AI assistant if not present
        if "AI_Assistant" not in participants:
            participants["AI_Assistant"] = {
                "name": "AI_Assistant",
                "audio": True,
                "video": False,
                "connected_at": datetime.now()
            }
    
    if participants:
        for participant_id, participant in participants.items():
            icon = "🤖" if "AI" in participant["name"] else "👤"
            audio_icon = "🔊" if participant["audio"] else "🔇"
            
            st.markdown(f'''
                <div class="participant-card">
                    <div style="display: flex; justify-content: space-between; align-items: center;">
                        <span><strong>{icon} {participant['name']}</strong></span>
                        <span>{audio_icon}</span>
                    </div>
                    <small>Connected: {participant.get('connected_at', datetime.now()).strftime('%H:%M:%S')}</small>
                </div>
            ''', unsafe_allow_html=True)
    else:
        st.info("No participants connected")

with col2:
    st.header("💬 AI Sales Assistant")
    
    # Chat messages display with enhanced styling
    chat_container = st.container()
    
    with chat_container:
        if st.session_state.chat_messages:
            for msg in st.session_state.chat_messages[-15:]:  # Show last 15 messages
                if msg["sender"] == "user":
                    with st.chat_message("user"):
                        st.write(f"**{msg['timestamp']}**: {msg['message']}")
                elif msg["sender"] == "ai":
                    with st.chat_message("assistant"):
                        st.write(f"**{msg['timestamp']}**: {msg['message']}")
                else:  # system
                    st.info(f"**{msg['timestamp']}**: {msg['message']}")
        else:
            st.info("💡 Connect to the room and start chatting with the AI assistant!")
    
    # Chat input with enhanced functionality
    if st.session_state.connected:
        # Quick reply buttons
        st.subheader("⚡ Quick Replies")
        quick_replies = [
            "Tell me about your product",
            "What's the pricing?",
            "Schedule a demo",
            "Contact information"
        ]
        
        cols = st.columns(2)
        for i, reply in enumerate(quick_replies):
            with cols[i % 2]:
                if st.button(reply, key=f"quick_{i}", use_container_width=True):
                    # Process quick reply
                    timestamp = datetime.now().strftime("%H:%M:%S")
                    st.session_state.chat_messages.append({
                        "timestamp": timestamp,
                        "message": reply,
                        "sender": "user"
                    })
                    
                    # Simulate AI response
                    ai_responses = {
                        "Tell me about your product": "Our SalesPro AI Assistant helps streamline your sales process with intelligent conversation analysis, lead scoring, and automated follow-ups. It integrates seamlessly with your existing CRM.",
                        "What's the pricing?": "We offer flexible pricing starting at $99/month for small teams. Enterprise plans are available with custom features. Would you like to see a detailed pricing breakdown?",
                        "Schedule a demo": "I'd be happy to schedule a personalized demo! Please provide your preferred time slots and I'll send you a calendar invite.",
                        "Contact information": "You can reach our sales team at sales@example.com or call +1-555-0123. Our team is available 24/7 to assist you."
                    }
                    
                    time.sleep(0.5)  # Brief delay for realism
                    st.session_state.chat_messages.append({
                        "timestamp": datetime.now().strftime("%H:%M:%S"),
                        "message": ai_responses.get(reply, "Thank you for your message! How can I help you further?"),
                        "sender": "ai"
                    })
                    
                    st.session_state.room_stats["messages_sent"] += 1
                    st.session_state.room_stats["messages_received"] += 1
                    st.rerun()
        
        # Text input
        user_message = st.chat_input("Type your message to the AI assistant...")
        
        if user_message:
            # Add user message
            timestamp = datetime.now().strftime("%H:%M:%S")
            st.session_state.chat_messages.append({
                "timestamp": timestamp,
                "message": user_message,
                "sender": "user"
            })
            
            # Send via LiveKit data channel (if available)
            if LIVEKIT_AVAILABLE and st.session_state.livekit_manager.room:
                try:
                    data = json.dumps({
                        "message": user_message,
                        "timestamp": timestamp,
                        "type": "chat"
                    })
                    # In real implementation, this would send data to other participants
                    # await st.session_state.livekit_manager.room.local_participant.publish_data(data.encode())
                except Exception as e:
                    logger.error(f"Failed to send data: {e}")
            
            # Simulate AI response with more sophisticated responses
            with st.spinner("🤖 AI is analyzing your message..."):
                time.sleep(1.5)  # Simulate processing time
                
                # More sophisticated AI responses based on keywords
                ai_response = generate_smart_response(user_message)
                
                st.session_state.chat_messages.append({
                    "timestamp": datetime.now().strftime("%H:%M:%S"),
                    "message": ai_response,
                    "sender": "ai"
                })
            
            st.session_state.room_stats["messages_sent"] += 1
            st.session_state.room_stats["messages_received"] += 1
            st.rerun()
    else:
        st.warning("🔌 Please connect to the room to start chatting")

def generate_smart_response(user_message: str) -> str:
    """Generate contextual AI responses based on user input"""
    message_lower = user_message.lower()
    
    if any(word in message_lower for word in ["price", "cost", "pricing", "fee"]):
        return "Our pricing is competitive and flexible. We offer tiered plans starting from $99/month for small teams, $299/month for growing businesses, and custom enterprise solutions. Each plan includes full access to our AI assistant, analytics dashboard, and 24/7 support. Would you like me to create a custom quote based on your team size?"
    
    elif any(word in message_lower for word in ["demo", "trial", "test", "try"]):
        return "Absolutely! I'd love to show you our platform in action. We offer a 14-day free trial with full features, plus I can schedule a personalized 30-minute demo where we'll configure the system for your specific use case. When would be a good time for you this week?"
    
    elif any(word in message_lower for word in ["feature", "capability", "function", "what can"]):
        return "Great question! Our AI assistant offers: 1) Real-time conversation analysis and sentiment detection, 2) Intelligent lead scoring and qualification, 3) Automated follow-up recommendations, 4) Integration with 50+ CRMs, 5) Custom reporting and analytics, 6) Multi-language support. Which of these features interests you most?"
    
    elif any(word in message_lower for word in ["integrate", "integration", "crm", "salesforce"]):
        return "Yes! We integrate seamlessly with all major CRMs including Salesforce, HubSpot, Pipedrive, Zoho, and 45+ others. Our API allows custom integrations too. The setup typically takes less than 15 minutes, and we provide dedicated integration support. Do you currently use a specific CRM platform?"
    
    elif any(word in message_lower for word in ["support", "help", "training", "onboarding"]):
        return "We pride ourselves on exceptional support! Every customer gets: 1) Free onboarding and training sessions, 2) 24/7 live chat and email support, 3) Dedicated customer success manager for enterprise clients, 4) Comprehensive knowledge base and video tutorials, 5) Weekly office hours with our product experts. What type of support would be most valuable for your team?"
    
    elif any(word in message_lower for word in ["security", "privacy", "gdpr", "compliance"]):
        return "Security is our top priority. We're SOC 2 Type II certified, GDPR compliant, and ISO 27001 certified. All data is encrypted in transit and at rest using AES-256. We offer on-premise deployment options for enterprise clients. We never share your data with third parties and you maintain full ownership of your customer information."
    
    elif any(word in message_lower for word in ["team", "users", "seats", "license"]):
        return "Our platform scales with your team! Plans include: Starter (up to 5 users), Professional (up to 25 users), and Enterprise (unlimited users). You can add or remove users anytime, and we offer volume discounts for larger teams. Each user gets full access to all features in their plan tier."
    
    else:
        responses = [
            "That's an excellent question! I'd be happy to provide more details. Based on what you've shared, I think our platform could be a great fit for your needs. What specific challenges are you looking to solve?",
            "Thank you for that insight! It sounds like you're dealing with some common sales challenges. Our AI assistant has helped similar companies increase their conversion rates by 35% on average. Would you like to hear how?",
            "I appreciate you sharing that with me! Every business has unique needs, and our platform is designed to adapt to your specific workflow. What does your current sales process look like?",
            "That's a great point to consider! Many of our customers had similar concerns before they saw the platform in action. Would you be interested in seeing some case studies from companies in your industry?"
        ]
        
        import random
        return random.choice(responses)

# Statistics and metrics
st.markdown("---")
st.header("📊 Session Analytics")

col_metric1, col_metric2, col_metric3, col_metric4 = st.columns(4)

with col_metric1:
    st.markdown('<div class="metric-card">', unsafe_allow_html=True)
    st.metric("Connection Status", 
              "🟢 Online" if st.session_state.connected else "🔴 Offline")
    st.markdown('</div>', unsafe_allow_html=True)

with col_metric2:
    st.markdown('<div class="metric-card">', unsafe_allow_html=True)
    st.metric("Participants", len(st.session_state.livekit_manager.participants) + (1 if st.session_state.connected else 0))
    st.markdown('</div>', unsafe_allow_html=True)

with col_metric3:
    st.markdown('<div class="metric-card">', unsafe_allow_html=True)
    st.metric("Messages Sent", st.session_state.room_stats["messages_sent"])
    st.markdown('</div>', unsafe_allow_html=True)

with col_metric4:
    st.markdown('<div class="metric-card">', unsafe_allow_html=True)
    connection_time = st.session_state.room_stats.get("connection_time")
    if connection_time and st.session_state.connected:
        duration = datetime.now() - connection_time
        duration_str = str(duration).split('.')[0]  # Remove microseconds
    else:
        duration_str = "Not connected"
    st.metric("Session Duration", duration_str)
    st.markdown('</div>', unsafe_allow_html=True)

# Technical information
with st.expander("🔧 Technical Information & Next Steps"):
    col_tech1, col_tech2 = st.columns(2)
    
    with col_tech1:
        st.subheader("Current Configuration")
        st.json({
            "Room": room_name,
            "Participant": participant_name,
            "LiveKit SDK": "Available" if LIVEKIT_AVAILABLE else "Not Available",
            "Audio Quality": audio_quality,
            "Echo Cancellation": echo_cancellation,
            "Noise Suppression": noise_suppression
        })
    
    with col_tech2:
        st.subheader("Production Setup Guide")
        st.markdown("""
        **To enable full LiveKit functionality:**
        
        1. **Install LiveKit SDK:**
           ```bash
           pip install livekit-api livekit-rtc
           ```
        
        2. **Set Environment Variables:**
           ```bash
           export LIVEKIT_API_KEY="your_api_key"
           export LIVEKIT_API_SECRET="your_secret"
           export LIVEKIT_URL="wss://your-server.livekit.cloud"
           ```
        
        3. **Deploy LiveKit Server:**
           - Use LiveKit Cloud (recommended)
           - Or self-host with Docker
        
        4. **Add AI Backend:**
           - Integrate with OpenAI/Claude API
           - Add speech-to-text processing
           - Implement real-time audio processing
        """)

# Footer
st.markdown("---")
st.markdown("""
<div style="text-align: center; color: #666; padding: 20px;">
    <p>🚀 LiveKit Sales Demo - Enhanced Streamlit Version</p>
    <p>Built with Streamlit • Powered by LiveKit • Enhanced with AI</p>
</div>
""", unsafe_allow_html=True)