# Hiring Potter - Voice AI Platform for Anganwadi Training

A comprehensive voice-AI platform that enables Anganwadi workers to connect with an AI-powered voice agent for real-time guidance, training, and assessment in their native language. The system provides automated post-call analytics to track progress and skill development.

## 🌟 Overview

This platform combines voice AI technology with specialized knowledge management to provide:
- **Real-time voice conversations** in Hindi/local languages
- **Automated training assessments** with scoring and feedback
- **Interactive learning modules** covering ICDS guidelines and Poshan Tracker
- **Post-call analytics** for tracking progress and identifying skill gaps
- **Web-based management dashboard** for administrators

## 🏗️ Architecture

The system consists of several key components:

- **Voice Processing Pipeline**: Real-time speech-to-text and text-to-speech
- **AI Training Assistant**: Context-aware conversational AI with domain knowledge
- **Knowledge Base**: ICDS guidelines, nutrition protocols, and best practices
- **Analytics Engine**: Automated evaluation and progress tracking
- **Web Dashboard**: Administrative interface and data visualization

## 🚀 Features

### Voice AI Assistant
- Native Hindi language support with regional language understanding
- Real-time conversation with context awareness
- Interactive Q&A sessions with immediate feedback
- Role-based training scenarios (Anganwadi Worker vs Helper)

### Training Modules
- **Nutrition Guidelines**: Supplementary nutrition, meal planning, feeding practices
- **Growth Monitoring**: Weight/height measurement, growth chart interpretation
- **Poshan Tracker**: Data entry training, common error prevention
- **Health & Hygiene**: Sanitation practices, immunization tracking
- **Early Child Development**: Age-appropriate activities and assessments

### Analytics & Assessment
- Automatic transcript generation and analysis
- AI-powered scoring based on communication clarity and knowledge demonstration
- Personalized feedback in Hindi
- Progress tracking across multiple sessions
- Skill gap identification and recommendations

### Administrative Dashboard
- Position and question management
- Call tracking and participant management
- Real-time transcript monitoring
- Performance analytics and reporting

## 🛠️ Technology Stack

### Core Technologies
- **Python 3.8+** - Main backend language
- **FastAPI** - Web API framework
- **Streamlit** - Dashboard interface
- **MongoDB** - Database for storing transcripts and analytics
- **WebRTC** - Real-time communication

### AI & Voice Processing
- **Groq LLM** - Language model for assessment and feedback
- **Ultravox STT** - Custom speech-to-text service
- **ChatterBox TTS** - Text-to-speech synthesis
- **LiveKit** - Real-time communication infrastructure

### Frontend & Communication
- **HTML/CSS/JavaScript** - Client interface
- **WebSocket** - Real-time data streaming
- **Base64 Audio Encoding** - Audio data transmission

## 📋 Prerequisites

- Python 3.8 or higher
- MongoDB instance (local or Atlas)
- Required API keys:
  - OpenAI API key
  - Groq API key
  - Deepgram API key
  - LiveKit credentials (if using LiveKit transport)

## 🔧 Installation

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd hiring-potter
   ```

2. **Create virtual environment**
   ```bash
   python -m venv env
   source env/bin/activate  # On Windows: env\Scripts\activate
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

4. **Environment Configuration**
   
   Create a `.env` file in the root directory:
   ```env
   
   # Groq Configuration
   GROQ_API_KEY=your_groq_api_key_here
   
   
   # LiveKit Configuration (if using LiveKit)
   LIVEKIT_API_KEY=your_livekit_api_key
   LIVEKIT_API_SECRET=your_livekit_api_secret
   LIVEKIT_URL=wss://your-livekit-server.livekit.cloud
   
   # MongoDB Configuration
   MONGODB_URI=mongodb://localhost:27017/
   
   # Ultravox STT Service (if using custom STT)
   ULTRAVOX_SERVER_URL=http://your-ultravox-server:port
   ```

5. **MongoDB Setup**
   
   For Streamlit frontend, create `frontend/.streamlit/secrets.toml`:
   ```toml
   MONGODB_URI = "mongodb://localhost:27017/"
   ```

## 🚀 Usage

### Running the Voice AI Assistant

1. **Start the main voice assistant**
   ```bash
   python run.py realtime_openai_script.py
   ```

2. **Access the client interface**
   - Open your browser to `http://localhost:6010`
   - Use the web interface to connect and start conversations

### Running the Administrative Dashboard

1. **Start the Streamlit dashboard**
   ```bash
   cd frontend
   streamlit run frontend.py
   ```

2. **Access the dashboard**
   - Open `http://localhost:8501`
   - Manage positions, questions, and view analytics

### Running with LiveKit Transport

1. **Configure LiveKit credentials** in your `.env` file

2. **Start the LiveKit-based assistant**
   ```bash
   python live_script.py
   ```

3. **Use the LiveKit client**
   - Open `client.html` in your browser
   - Enter your LiveKit server details and token

## 📊 API Endpoints

### Main Application (`run.py`)
- `GET /` - Serve client interface
- `POST /api/offer` - WebRTC connection handling
- `GET /api/transcript` - Retrieve conversation transcripts

### Voice Processing Services
- Ultravox STT Service - Custom speech-to-text processing
- Chatterbox TTS Service - Advanced text-to-speech with streaming
- OpenAI Realtime - Complete speech-to-speech processing

## 📁 Project Structure

```
hiring-potter/
├── PCA/                          # Call tracking and processing
│   ├── main.py                   # MongoDB processing pipeline
│   └── llm.py                    # LLM context management
├── frontend/                     # Administrative dashboard
│   ├── frontend.py               # Streamlit application
│   └── .streamlit/
│       └── secrets.toml          # Database configuration
├── realtime_openai_script.py     # Main voice AI application
├── live_script.py               # LiveKit-based voice assistant
├── ultravox_stt_service1.py     # Custom STT service
├── plugin_chatterbox.py         # Advanced TTS service
├── run.py                       # Application runner and WebRTC server
├── client.html                  # Web client interface
├── custom_ui.html               # Custom user interface
├── knowledge_base.txt           # ICDS guidelines and training content
├── prompt.txt                   # AI assistant system prompts
├── requirements.txt             # Python dependencies
└── README.md                    # This file
```

## 🎯 Key Features in Detail

### Voice AI Conversation Flow
1. **Greeting & Information Collection**: AI assistant introduces itself and collects participant details
2. **Interactive Training**: Context-aware conversations covering ICDS guidelines
3. **Real-time Q&A**: Immediate responses to questions with practical examples
4. **Assessment Integration**: Automatic evaluation of responses and knowledge demonstration

### Knowledge Base Integration
- Comprehensive ICDS guidelines coverage
- Nutrition and health protocols
- Poshan Tracker usage instructions
- Common troubleshooting scenarios
- Best practices and field examples

### Automated Analytics
- **Transcript Analysis**: AI-powered content analysis and scoring
- **Performance Metrics**: Communication clarity, knowledge demonstration, engagement levels
- **Progress Tracking**: Session-to-session improvement monitoring
- **Personalized Feedback**: Constructive suggestions in native language

## 🔒 Security & Privacy

- Audio data processed in real-time without persistent storage
- Transcript data encrypted and securely stored
- API keys managed through environment variables
- Session-based authentication for administrative features

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📄 License

This project is licensed under the BSD 2-Clause License - see the LICENSE file for details.

## 🆘 Support & Troubleshooting

### Common Issues

1. **Connection Problems**
   - Verify all API keys are correctly configured
   - Check MongoDB connectivity
   - Ensure required ports are available

2. **Audio Issues**
   - Verify microphone permissions in browser
   - Check WebRTC compatibility
   - Ensure stable internet connection

3. **Performance Optimization**
   - Adjust VAD (Voice Activity Detection) parameters
   - Configure appropriate timeout values
   - Monitor system resources during high usage

### Getting Help
- Check the logs for detailed error messages
- Verify environment configuration
- Ensure all dependencies are properly installed

---

**Made with ❤️ for empowering Anganwadi workers through AI-powered training**