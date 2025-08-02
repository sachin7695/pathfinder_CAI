#
# Copyright (c) 2024–2025, Daily
#
# SPDX-License-Identifier: BSD 2-Clause License
#

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

# from pipecat.services.ultravox.stt import UltravoxSTTService

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
from plugin_chatterbox import ChatterboxWebSocketService

from dotenv import load_dotenv
from loguru import logger

from pipecat.audio.vad.silero import SileroVADAnalyzer
from pipecat.frames.frames import (
    BotInterruptionFrame,
    TextFrame,
    TranscriptionFrame,
    UserStartedSpeakingFrame,
    UserStoppedSpeakingFrame,
)
from pipecat.pipeline.pipeline import Pipeline
from pipecat.pipeline.runner import PipelineRunner
from pipecat.pipeline.task import PipelineParams, PipelineTask
from pipecat.processors.aggregators.openai_llm_context import OpenAILLMContext
# from pipecat.runner.livekit import configure
from pipecat.transports.services.livekit import LiveKitParams, LiveKitTransport
from pipecat.frames.frames import  TTSSpeakFrame,  LLMFullResponseStartFrame, TextFrame, LLMFullResponseEndFrame
load_dotenv(override=True)

logger.remove(0)
logger.add(sys.stderr, level="DEBUG")

def generate_livekit_token(api_key: str, api_secret: str, room: str, participant_name: str = "ai_assistant", ttl_seconds: int = 7200):
    """
    Generate LiveKit token matching your client-side implementation
    """
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
    """
    Setup LiveKit connection using your credentials
    """
    # Your LiveKit credentials (same as your client-side code)
    api_key = "APIAMrTXLVoxLqe"
    api_secret = "3pFSQsUzLLeEEWrvO1hJaP4QA97CNeoMEkQA6wWSkuS"
    room = "my_private_sales_room_2024"
    participant_name = "AI_Assistant"  # Different from client participant
    
    # You can also load these from environment variables for security
    # api_key = os.getenv("LIVEKIT_API_KEY", "APIAMrTXLVoxLqe")
    # api_secret = os.getenv("LIVEKIT_API_SECRET", "3pFSQsUzLLeEEWrvO1hJaP4QA97CNeoMEkQA6wWSkuS")
    # room = os.getenv("LIVEKIT_ROOM", "my_private_sales_room_2024")
    
    # Generate token for the AI assistant
    token = generate_livekit_token(
        api_key=api_key,
        api_secret=api_secret,
        room=room,
        participant_name=participant_name,
        ttl_seconds=7200  # 2 hours
    )
    
    # LiveKit URL (replace with your actual LiveKit server URL)
    url = os.getenv("LIVEKIT_URL", "wss://your-livekit-server.livekit.cloud")
    
    logger.info(f"Generated token for AI assistant in room: {room}")
    logger.info(f"Participant name: {participant_name}")
    
    return url, token, room
async def main():
    (url, token, room_name) = await setup_livekit_connection()

    transport = LiveKitTransport(
        url="wss://telecmi-vuq7uhg6.livekit.cloud",
        token=token,
        room_name="my_private_sales_room_2024",
        params=LiveKitParams(
            audio_in_enabled=True,
            audio_out_enabled=True,
            vad_analyzer=SileroVADAnalyzer(),
        ),
    )

    # stt = WhisperSTTService(
    #     model=Model.LARGE,
    #     # model = MLXModel.LARGE_V3_TURBO_Q4,
    #     device="cuda",
    #     compute_type="int8",
    #     no_speech_prob=0.4,
    #     language=Language.EN,
    # )
    
    groq_api_key = "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" #os.getenv("GROQ_API_KEY")

    stt = GroqSTTService(api_key=groq_api_key)
    llm = GroqLLMService(
        api_key=groq_api_key,
        # model= "meta-llama/llama-4-maverick-17b-128e-instruct" #"llama3-8b-8192",
        model="llama3-8b-8192",
    )

    tts = ChatterboxWebSocketService(
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

    messages = [
        {
            "role": "system",
            "content": """
            You are InterviewBuddy — a warm, friendly, and encouraging AI assistant dedicated to helping users prepare for job interviews. You always communicate in **English**.

            **Your role:**
            - Engage users in conversations about their upcoming interviews, career goals, and preparation strategies.
            - Ask thoughtful questions about the roles they’re applying for, their strengths, weaknesses, and recent preparation.
            - Offer support, encouragement, and actionable tips to improve their interview performance.
            - Help users practice common interview questions, discuss their experiences, and boost their confidence.

            **Guidelines for all responses:**
            - Respond only in English.
            - Use a conversational, simple, and motivating tone.
            - Keep answers and questions short — 2-3 sentences per turn.
            - Always prompt the user to share more about their interview prep, recent experiences, or concerns.
            - If the user goes off-topic, gently guide the conversation back to interview preparation and professional growth.
            - When someone first speaks to you, greet them warmly and introduce yourself as InterviewBuddy.

            **Examples:**
            - Hi there! I’m InterviewBuddy, your AI interview assistant. What role are you preparing for right now?
            - What’s your biggest strength, and how do you usually showcase it in interviews?
            - Can you tell me about a recent interview experience you had?
            - Great progress! Would you like to practice some common interview questions together?

            Be supportive, practical, and always eager to help the user succeed in their job search journey!
            """
        },
    ]


    context = OpenAILLMContext(messages)
    context_aggregator = llm.create_context_aggregator(context)

    runner = PipelineRunner()

    task = PipelineTask(
        Pipeline(
            [
                transport.input(),
                stt,
                context_aggregator.user(),
                llm,
                tts,
                transport.output(),
                context_aggregator.assistant(),
            ],
        ),
        params=PipelineParams(
            enable_metrics=True,
            enable_usage_metrics=True,
        ),
    )

    # Register an event handler so we can play the audio when the
    # participant joins.
    @transport.event_handler("on_first_participant_joined")
    async def on_first_participant_joined(transport, participant_id):
        await asyncio.sleep(1)
        # Test TTS connection
        # greeting_text =  "नमस्ते मैं SalesPro हूँ, आपका निजी AI सेल्स असिस्टेंट। मैं यहाँ आपकी मदद के लिए हूँ ।"
        greeting_text = "ahhh, hello i am AI assistant your personal helper!!"

        # TTS frame
        # speak_frame = TTSSpeakFrame(text=greeting_text)


        await task.queue_frames([
            LLMFullResponseStartFrame(),
            TextFrame(text=greeting_text),
            LLMFullResponseEndFrame()
        ])

        # await task.queue_frame(speak_frame)
        # context = OpenAILLMContext()
        context.add_message({
            "role": "assistant",
            "content": greeting_text
        })

    # Register an event handler to receive data from the participant via text chat
    # in the LiveKit room. This will be used to as transcription frames and
    # interrupt the bot and pass it to llm for processing and
    # then pass back to the participant as audio output.
    @transport.event_handler("on_data_received")
    async def on_data_received(transport, data, participant_id):
        logger.info(f"Received data from participant {participant_id}: {data}")
        
        try:
            json_data = json.loads(data)
            
            await task.queue_frames([
                BotInterruptionFrame(),
                UserStartedSpeakingFrame(),
                TranscriptionFrame(
                    user_id=participant_id,
                    timestamp=json_data.get("timestamp", int(time.time() * 1000)),
                    text=json_data.get("message", ""),
                ),
                UserStoppedSpeakingFrame(),
            ])
            
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON data: {e}")
        except Exception as e:
            logger.error(f"Error processing data: {e}")

    logger.info(f"🚀 Starting SalesPro AI assistant")
    logger.info(f"📍 Room: {room_name}")
    logger.info(f"🔗 LiveKit URL: {url}")
    logger.info(f"🤖 Ready to assist with sales inquiries!")

    await runner.run(task)


if __name__ == "__main__":
    asyncio.run(main())