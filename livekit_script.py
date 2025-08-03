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
from pipecat.audio.filters.noisereduce_filter import NoisereduceFilter
from pipecat.processors.transcript_processor import TranscriptProcessor
# from pipecat.runner.types import RunnerArguments
from pipecat.frames.frames import TranscriptionMessage, TranscriptionUpdateFrame
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


class TranscriptHandler:
    """Handles real-time transcript processing and output.

    Maintains a list of conversation messages and outputs them either to a log
    or to a file as they are received. Each message includes its timestamp and role.

    Attributes:
        messages: List of all processed transcript messages
        output_file: Optional path to file where transcript is saved. If None, outputs to log only.
    """

    def __init__(self, output_file: Optional[str] = None):
        """Initialize handler with optional file output.

        Args:
            output_file: Path to output file. If None, outputs to log only.
        """
        self.messages: List[TranscriptionMessage] = []
        self.output_file: Optional[str] = output_file
        logger.debug(
            f"TranscriptHandler initialized {'with output_file=' + output_file if output_file else 'with log output only'}"
        )

    async def save_message(self, message: TranscriptionMessage):
        """Save a single transcript message.

        Outputs the message to the log and optionally to a file.

        Args:
            message: The message to save
        """
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
        """Handle new transcript messages.

        Args:
            processor: The TranscriptProcessor that emitted the update
            frame: TranscriptionUpdateFrame containing new messages
        """
        logger.debug(f"Received transcript update with {len(frame.messages)} new messages")

        for msg in frame.messages:
            self.messages.append(msg)
            await self.save_message(msg)


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

async def stop(task, room_name) -> None:
    if task:
        logger.info("Stopping session %s", room_name)
        await task.cancel()

async def main():
    (url, token, room_name) = await setup_livekit_connection()

    transport = LiveKitTransport(
        url="wss://telecmi-vuq7uhg6.livekit.cloud",
        token=token,
        room_name="my_private_sales_room_2024",
        params=LiveKitParams(
            audio_in_enabled=True,
            audio_out_enabled=True,
            audio_in_sample_rate=16000,
            audio_out_sample_rate=24000,    
            audio_in_filter=NoisereduceFilter(),
            vad_analyzer=SileroVADAnalyzer(params=
                    VADParams(
                        start_secs=0.10,   # react ~100 ms after speech onset
                        # stop_secs=0.25,    # cut off quickly after 250 ms silence
                        # min_volume=0.3,    # make it less strict
                        confidence=0.9,
                        stop_secs=0.25,
                        min_volume=0.6,
                    )
                ),
        ),
    )

    stt = WhisperSTTService(
        model=Model.TINY,
        # model = MLXModel.LARGE_V3_TURBO_Q4,
        device="cpu",
        compute_type="int8",
        no_speech_prob=0.4,
        language=Language.EN,
    )
    
    groq_api_key = "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" #os.getenv("GROQ_API_KEY")

    # stt = GroqSTTService(api_key=groq_api_key)
    llm = GroqLLMService(
        api_key=groq_api_key,
        # model= "meta-llama/llama-4-maverick-17b-128e-instruct" #"llama3-8b-8192",
        model="llama3-8b-8192",
    )

    # tts = ChatterboxWebSocketService(
    #     websocket_url="ws://103.247.19.245:60027",
    #     # voice_prompt_path="/home/user/voice/audio_data/Base-1.wav",  # Optional
    #     streaming_mode=True,  # Use native streaming
    #     chunk_size=60,
    #     exaggeration=0.8,
    #     temperature=0.6,
    #     cfg_weight=0.2,
    #     context_window=30,
    #     fade_duration=0.02,
    #     reconnect_on_interrupt=False  # Fast interruption without reconnect
    # )
    tts = DeepgramTTSService(
            api_key='a81490d2493749e737afed5f70bc67767b700149',
            voice="aura-2-andromeda-en",
            sample_rate=16000
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
    transcript = TranscriptProcessor()
    transcript_handler = TranscriptHandler(output_file="transcript.txt")



    task = PipelineTask(
        Pipeline(
            [
                transport.input(),
                stt,
                transcript.user(),
                context_aggregator.user(),
                llm,
                tts,
                transport.output(),
                transcript.assistant(),
                context_aggregator.assistant(),
            ],
        ),
        params=PipelineParams(
            enable_metrics=True,
            enable_usage_metrics=True,
            allow_interruptions = True,
            idle_timeout_secs = 600,
            idle_timeout_frames = (BotSpeakingFrame,),
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
            # Close the transport
            # await transport.close()
        except Exception as e:
            logger.error(f"Error stopping session: {e}")

    # Register an event handler so we can play the audio when the
    # participant joins.
    @transport.event_handler("on_first_participant_joined")
    async def on_first_participant_joined(transport, participant_id):

        await asyncio.sleep(0.2)
        # Test TTS connection
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

    # Register event handler for transcript updates
    @transcript.event_handler("on_transcript_update")
    async def on_transcript_update(processor, frame):
        await transcript_handler.on_transcript_update(processor, frame)


    @task.event_handler("on_idle_timeout")
    async def on_idle_timeout(self):
        # Queue the goodbye message first
        await task.queue_frame(TTSSpeakFrame("I haven't heard from you in a while. Goodbye!"))
        # Wait a bit for the message to be spoken
        await asyncio.sleep(3)
        # Then stop the session
        await stop_session()

    # Handle participant leaving
    @transport.event_handler("on_participant_left")
    async def on_participant_left(transport, participant_id, reason):
        logger.info(f"Participant {participant_id} left the room")
        await stop_session()

    # Handle transport disconnect
    @transport.event_handler("on_disconnected")
    async def on_disconnected(transport):
        logger.info("Transport disconnected")
        await stop_session()

    
    logger.info(f"🚀 Starting SalesPro AI assistant")
    logger.info(f"📍 Room: {room_name}")
    logger.info(f"🔗 LiveKit URL: {url}")
    logger.info(f"🤖 Ready to assist with sales inquiries!")

    try:
        await PipelineRunner(handle_sigint=True).run(task)
    except KeyboardInterrupt:
        logger.info("Received interrupt signal, shutting down...")
        await stop_session()
    except Exception as e:
        logger.error(f"Pipeline error: {e}")
        await stop_session()

    


if __name__ == "__main__":
    asyncio.run(main())