#
# Copyright (c) 2024–2025, Daily
#
# SPDX-License-Identifier: BSD 2-Clause License
#
from typing import List, Optional
import asyncio
import json
import os
import sys
import argparse
import asyncio
import json
import os
import sys
import argparse
import os
import aiohttp
import time
from datetime import datetime, timedelta
import jwt
from dotenv import load_dotenv
from loguru import logger
import sys
import asyncio
from dotenv import load_dotenv
from loguru import logger
import warnings
warnings.filterwarnings("ignore")

from pipecat.services.deepgram.tts import DeepgramTTSService
from pipecat.audio.vad.silero import SileroVADAnalyzer
from pipecat.audio.vad.vad_analyzer import VADParams
from pipecat.pipeline.pipeline import Pipeline
from pipecat.pipeline.parallel_pipeline import ParallelPipeline
from pipecat.pipeline.runner import PipelineRunner
from pipecat.pipeline.task import PipelineParams, PipelineTask
from pipecat.transports.base_transport import TransportParams
from pipecat.transports.network.small_webrtc import SmallWebRTCTransport
from pipecat.transports.network.webrtc_connection import SmallWebRTCConnection
from pipecat.audio.filters.noisereduce_filter import NoisereduceFilter
from pipecat.frames.frames import BotSpeakingFrame,TTSSpeakFrame,LLMFullResponseStartFrame, LLMTextFrame, LLMFullResponseEndFrame, CancelFrame

from pipecat.processors.aggregators.gated_openai_llm_context import GatedOpenAILLMContextAggregator
from pipecat.processors.filters.null_filter import NullFilter
from pipecat.processors.filters.wake_notifier_filter import WakeNotifierFilter
from pipecat.processors.user_idle_processor import UserIdleProcessor
from pipecat.sync.event_notifier import EventNotifier

from pipecat.transcriptions.language import Language
from pipecat.services.groq.llm import GroqLLMService
from pipecat.services.groq.stt import GroqSTTService
from pipecat.processors.aggregators.openai_llm_context import OpenAILLMContext
from pipecat.processors.aggregators.llm_response import LLMUserAggregatorParams

from pipecat.services.llm_service import FunctionCallParams
from pipecat.services.whisper.stt import WhisperSTTService, Model
from pipecat.adapters.schemas.function_schema import FunctionSchema
from pipecat.adapters.schemas.tools_schema import ToolsSchema
from openai.types.chat import ChatCompletionToolParam
from pipecat.audio.turn.base_turn_analyzer import BaseTurnAnalyzer
from pipecat.frames.frames import  TTSSpeakFrame
from pipecat.audio.vad.silero import SileroVADAnalyzer
from pipecat.pipeline.pipeline import Pipeline
from pipecat.pipeline.runner import PipelineRunner
from pipecat.pipeline.task import PipelineParams, PipelineTask
from pipecat.processors.aggregators.openai_llm_context import OpenAILLMContext
from pipecat.transports.services.daily import DailyDialinSettings, DailyParams, DailyTransport

from dotenv import load_dotenv
from loguru import logger

from pipecat.audio.vad.silero import SileroVADAnalyzer
from pipecat.frames.frames import (
    AudioRawFrame,
    CancelFrame,
    EndFrame,
    ErrorFrame,
    Frame,
    LLMFullResponseEndFrame,
    LLMFullResponseStartFrame,
    LLMTextFrame,
    StartFrame,
    UserStartedSpeakingFrame,
    UserStoppedSpeakingFrame,
)
from groq import Groq
from pipecat.services.openai.tts import OpenAITTSService
from pipecat.audio.filters.noisereduce_filter import NoisereduceFilter
from pipecat.processors.transcript_processor import TranscriptProcessor
from pipecat.frames.frames import TranscriptionMessage, TranscriptionUpdateFrame
from pipecat.pipeline.pipeline import Pipeline
from pipecat.pipeline.runner import PipelineRunner
from pipecat.pipeline.task import PipelineParams, PipelineTask
from pipecat.processors.aggregators.openai_llm_context import OpenAILLMContext
from pipecat.transports.services.livekit import LiveKitParams, LiveKitTransport
from pipecat.frames.frames import  TTSSpeakFrame,  LLMFullResponseStartFrame, TextFrame, LLMFullResponseEndFrame
from datetime import datetime
from pymongo import MongoClient
# Import our custom Ultravox API STT service
from ultravox_stt_service1 import UltravoxSTTService #UltravoxAPISTTService, UltravoxWebSocketSTTService
load_dotenv(override=True)

logger.remove(0)
logger.add(sys.stderr, level="DEBUG")


def init_mongodb():
    try:
        # MongoDB connection string - replace with your actual connection string
        mongodb_uri = "mongodb://localhost:27017/"
        client = MongoClient(mongodb_uri, serverSelectionTimeoutMS=2000)
        client.admin.command('ping')  # Force connection check
        db = client['anganwadi']
        return db
    except Exception as e:
        return None

db = init_mongodb()
pid= 1


def save_position_to_db(transcript, score, feedback, participant_name):
    """Save position data to MongoDB"""
    global db
    if db is None:
        return False, "Database connection not available"
    
    try:
        
        position_data = {
            "transcript": transcript,
            "score": score,
            "feedback": feedback,
            "participant_name": participant_name,
            "updated_at": datetime.now(),
        }
        logger.info("########################")
        logger.info(str(position_data))
        result = db.call_tracking.insert_one(position_data)
        return True, str(result.inserted_id)
    except Exception as e:
        return False, str(e)



def load_instrcutions(path:str):
    with open(path, "r", encoding="utf-8") as f:
        return f.read()
instruction_path = os.path.join(os.path.dirname(__file__), "prompt.txt")
instruction_text = load_instrcutions(instruction_path)


def load_knowledge_base(path: str) -> str:
    with open(path, "r", encoding="utf-8") as f:
        return f.read()
    

kb_path = os.path.join(os.path.dirname(__file__), "knowledge_base.txt")
kb_text = load_knowledge_base(kb_path)




groq_api_key = "xxxx"


class TranscriptHandler:
    """Handles real-time transcript processing and output."""

    def __init__(self, output_file: Optional[str] = None):
        """Initialize handler with optional file output."""
        self.messages: List[TranscriptionMessage] = []
        self.output_file: Optional[str] = output_file
        logger.debug(
            f"TranscriptHandler initialized {'with output_file=' + output_file if output_file else 'with log output only'}"
        )

    async def save_message(self, message: TranscriptionMessage):
        """Save a single transcript message."""
        timestamp = f"[{message.timestamp}] " if message.timestamp else ""
        line = f"{timestamp}{message.role}: {message.content}"

        # Always log the message
        logger.info(f"Transcript: {line}")

        # Optionally write to file
        if self.output_file:
            try:
                with open(self.output_file, "a", encoding="utf-8") as f:
                    f.write(line + "\n")
            except Exception as e:
                logger.error(f"Error saving transcript message to file: {e}")

    async def on_transcript_update(
        self, processor: TranscriptProcessor, frame: TranscriptionUpdateFrame
    ):
        """Handle new transcript messages."""
        logger.debug(f"Received transcript update with {len(frame.messages)} new messages")

        for msg in frame.messages:
            self.messages.append(msg)
            await self.save_message(msg)


def generate_livekit_token(api_key: str, api_secret: str, room: str, participant_name: str = "ai_assistant", ttl_seconds: int = 7200):
    """Generate LiveKit token matching your client-side implementation"""
    now = int(time.time())
    
    payload = {
        'iss': api_key,
        'sub': participant_name,  # identity
        'nbf': now,
        'exp': now + ttl_seconds,
        'name': participant_name,
        'video': {
            'roomJoin': True,
            'room': room,
            'canPublish': True,
            'canSubscribe': True,
            'canPublishData': True,
            'hidden': False
        }
    }
    
    return jwt.encode(payload, api_secret, algorithm='HS256')

async def setup_livekit_connection():
    """Setup LiveKit connection using your credentials"""
    # Your LiveKit credentials
    api_key = "xx"
    api_secret = "xx"
    room = "Anganwadi_Training"
    participant_name = "AI_Assistant"
    
    # Generate token for the AI assistant
    token = generate_livekit_token(
        api_key=api_key,
        api_secret=api_secret,
        room=room,
        participant_name=participant_name,
        ttl_seconds=7200
    )
    
    # LiveKit URL
    url = os.getenv("LIVEKIT_URL", "wss://xx-vuq7uhg6.livekit.cloud")
    
    logger.info(f"Generated token for AI assistant in room: {room}")
    logger.info(f"Participant name: {participant_name}")
    
    return url, token, room

async def stop(task, room_name) -> None:
    if task:
        logger.info("Stopping session %s", room_name)
        await task.cancel()


async def evaluate_transcript_with_groq(transcript_text: str) -> tuple[int, str]:
    """
        Evaluate the transcript using Groq LLM and return score and feedback
    """
    try:
        groq_api_key = "xx"

        client = Groq(api_key=groq_api_key)
        
        evaluation_prompt = f"""
        You are an expert evaluator for Anganwadi training sessions. 
        Evaluate the following conversation transcript and provide:
        1. A score from 0-100 based on:
           - Clarity of communication
           - Knowledge demonstrated
           - Engagement level
           - Proper use of Hindi/local language
           - Understanding of Anganwadi concepts
        
        2. Constructive feedback in Hindi for improvement
        
        Transcript:
        {transcript_text}
        
        Respond in JSON format:
        {{
            "score": <number between 0-100>,
            "feedback": "<feedback in Hindi>"
        }}
        """
        
        completion = client.chat.completions.create(
            model="llama3-8b-8192",  # or your preferred model
            messages=[
                {
                    "role": "system",
                    "content": "You are an Anganwadi training evaluator. Respond only in valid JSON format."
                },
                {
                    "role": "user",
                    "content": evaluation_prompt
                }
            ],
            temperature=0.3,
            max_tokens=500,
            response_format={"type": "json_object"}
        )
        
        result = json.loads(completion.choices[0].message.content)
        score = int(result.get("score", 0))
        feedback = result.get("feedback", "No feedback provided")
        
        return score, feedback
        
    except Exception as e:
        logger.error(f"Error evaluating transcript: {e}")
        return 0, f"Error during evaluation: {str(e)}"
    

def read_transcript_file(file_path: str) -> str:
    """Read the complete transcript from file"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return f.read()
    except Exception as e:
        logger.error(f"Error reading transcript file: {e}")
        return ""

async def main():
    (url, token, room_name) = await setup_livekit_connection()

    transport = LiveKitTransport(
        url="wss://xx-vuq7uhg6.livekit.cloud",
        token=token,
        room_name=room_name,
        params=LiveKitParams(
            audio_in_enabled=True,
            audio_out_enabled=True,
            audio_in_sample_rate=16000,
            audio_out_sample_rate=16000,    
            # audio_in_filter=NoisereduceFilter(),
            vad_analyzer=SileroVADAnalyzer(params=
                    VADParams(
                        start_secs=0.10,   # react ~100 ms after speech onset
                        confidence=0.6,
                        stop_secs=0.25,
                        min_volume=0.7,
                    )
                ),
        ),
    )
    # Configure your Ultravox server URL
    ultravox_server_url = "http://103.247.19.245:60032"  # Use HTTP for API endpoints
    
    # Option 1: Use HTTP API-based STT service (recommended for reliability)
    # stt = UltravoxSTTService(
    #     server_url=ultravox_server_url,
    #     # model_name="fixie-ai/ultravox-v0_5-llama-3_1-8b",
    #     temperature=0.7,
    #     max_tokens=200,
    #     # sample_rate=16000,
    #     audio_format="pcm_s16le",
    #     use_streaming=True,  # Set to False for batch mode
    #     session_timeout=30
    # )
    stt = UltravoxSTTService(
    api_base_url=ultravox_server_url,
    temperature=0.5,
    max_tokens=300
)
    # Option 2: Alternative WebSocket-based STT service
    # ultravox_ws_url = "ws://103.247.19.245:60032"
    # stt = UltravoxWebSocketSTTService(
    #     server_url=ultravox_ws_url,
    #     model_name="fixie-ai/ultravox-v0_5-llama-3_1-8b",
    #     temperature=0.7,
    #     max_tokens=100,
    #     sample_rate=16000
    # )

    # Setup Groq LLM (if you want to add LLM capabilities)
    
    api_key="xx-xx-xx"
    # tts = OpenAITTSService(api_key=api_key, voice="nova", model = "gpt-4o-mini-tts")
    tts1 = ChatterboxWebSocketService(
        websocket_url="ws://103.247.19.245:60027",
        # voice_prompt_path="/home/user/voice/audio_data/Base-1.wav",  # Optional
        streaming_mode=True,  # Use native streaming
        chunk_size=25,
        exaggeration=0.8,
        temperature=0.6,
        cfg_weight=0.2,
        context_window=20,
        fade_duration=0.02,
        reconnect_on_interrupt=False  # Fast interruption without reconnect
    )


    
    # Setup transcript processing
    transcript = TranscriptProcessor()
    transcript_handler = TranscriptHandler(output_file="transcript_ultravox.txt")
    # Create the pipeline task
    task = PipelineTask(
        Pipeline(
            [
                transport.input(),
                stt,
                transcript.user(),
                # context_aggregator.user(),
                # llm,
                tts,
                transport.output(),
                transcript.assistant(),
                # context_aggregator.assistant(),
            ],
        ),
        params=PipelineParams(
            enable_metrics=True,
            enable_usage_metrics=True,
            allow_interruptions=True,
            idle_timeout_secs=600,
            idle_timeout_frames=(BotSpeakingFrame,),
            cancel_on_idle_timeout=False
        ),
    )

    async def stop_session():
        logger.info(f"Stopping session in room: {room_name}")
        try:
            # Queue a cancel frame to stop the pipeline
            await task.queue_frame(CancelFrame())
            # Cancel the task
            await task.cancel()
        except Exception as e:
            logger.error(f"Error stopping session: {e}")

    # Register event handlers
    @transport.event_handler("on_first_participant_joined")
    async def on_first_participant_joined(transport, participant_id):
        await asyncio.sleep(0.2)
        
        # Test the Ultravox connection by sending a greeting
        greeting_text = """
        नमस्ते! मैं आपकी एआई सहायिका हूँ, जो आपकी आंगनवाड़ी ट्रेनिंग में मदद करेगी। 
        सबसे पहले, क्या मैं आपका नाम जान सकती हूँ? आप किस गाँव या इलाके में काम करती हैं? 
        और आपकी भूमिका क्या है—आंगनवाड़ी कार्यकर्ता या सहायिका?
        """

        await task.queue_frames([
            LLMFullResponseStartFrame(),
            LLMTextFrame(text=greeting_text),
            LLMFullResponseEndFrame()
        ])
        context = OpenAILLMContext()
        # Add to context
        context.add_message({
            "role": "assistant",
            "content": greeting_text
        })

    @transcript.event_handler("on_transcript_update")
    async def on_transcript_update(processor, frame):
        await transcript_handler.on_transcript_update(processor, frame)

    @task.event_handler("on_idle_timeout")
    async def on_idle_timeout(self):
        await task.queue_frame(TTSSpeakFrame("I haven't heard from you in a while. Feel free to come back anytime for interview practice. Goodbye!"))
        await asyncio.sleep(3)
        await stop_session()




    @transport.event_handler("on_participant_left")
    async def on_participant_left(transport, participant_id, reason):
        logger.info(f"Participant {participant_id} left the room")
        # Read the transcript
        transcript_content = read_transcript_file("transcript_ultravox.txt")
        
        if transcript_content:
            # Evaluate the transcript
            score, feedback = await evaluate_transcript_with_groq(
                transcript_content, 
            )
            # Save to database
            success, result = save_position_to_db(
                transcript=transcript_content,
                score=score,
                feedback=feedback,
                participant_name=participant_id
            )
            
            if success:
                logger.info(f"Successfully saved evaluation to DB: {result}")
            else:
                logger.error(f"Failed to save to DB: {result}")
            await stop_session()

        

    @transport.event_handler("on_disconnected")
    async def on_disconnected(transport):
        global pid
        logger.info("Transport disconnected")
        # Get participant ID if available
        try:
            
            participant_id = getattr(transport, 'participant_id', 'Participant')
        except Exception as e:
            participant_id = "participate_"+str(pid)
            pid+=1 
        
        # Read the transcript
        transcript_content = read_transcript_file("transcript_ultravox.txt")
        
        if transcript_content:
            # Evaluate the transcript
            score, feedback = await evaluate_transcript_with_groq(
                transcript_content, 
            )
            
            # Save to database
            success, result = save_position_to_db(
                transcript=transcript_content,
                score=score,
                feedback=feedback,
                participant_name=participant_id
            )
            
            if success:
                logger.info(f"Successfully saved evaluation to DB: {result}")
            else:
                logger.error(f"Failed to save to DB: {result}")
        await stop_session()




    # Log startup information
    logger.info("🚀 Starting InterviewBuddy AI Assistant")
    logger.info(f"📍 Room: {room_name}")
    logger.info(f"🔗 LiveKit URL: {url}")
    logger.info(f"🎤 STT: Ultravox API ({ultravox_server_url})")
    logger.info(f"🧠 LLM: Groq (llama3-8b-8192)")
    logger.info(f"🔊 TTS: Deepgram")
    logger.info("🤖 Ready to help with interview preparation!")

    # Run the pipeline
    try:
        await PipelineRunner(handle_sigint=False).run(task)
    except KeyboardInterrupt:
        logger.info("Received interrupt signal, shutting down...")
        await stop_session()
    except Exception as e:
        logger.error(f"Pipeline error: {e}")
        await stop_session()


if __name__ == "__main__":
    asyncio.run(main())