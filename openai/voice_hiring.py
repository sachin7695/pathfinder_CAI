#
# Copyright (c) 2024–2025, Daily
#
# SPDX-License-Identifier: BSD 2-Clause License
#

import argparse
import os
import time
import jwt
from datetime import datetime

from dotenv import load_dotenv
from loguru import logger

from pipecat.adapters.schemas.function_schema import FunctionSchema
from pipecat.adapters.schemas.tools_schema import ToolsSchema
from pipecat.audio.vad.silero import SileroVADAnalyzer
from pipecat.audio.vad.vad_analyzer import VADParams
from pipecat.pipeline.pipeline import Pipeline
from pipecat.pipeline.runner import PipelineRunner
from pipecat.pipeline.task import PipelineParams, PipelineTask
from pipecat.processors.aggregators.openai_llm_context import OpenAILLMContext
from pipecat.services.llm_service import FunctionCallParams
from pipecat.services.openai_realtime_beta import (
    InputAudioNoiseReduction,
    InputAudioTranscription,
    OpenAIRealtimeBetaLLMService,
    SemanticTurnDetection,
    SessionProperties,
)
from pipecat.transports.base_transport import TransportParams
from pipecat.transports.services.livekit import LiveKitParams, LiveKitTransport
from pipecat.audio.filters.noisereduce_filter import NoisereduceFilter
import warnings 
warnings.filterwarnings("ignore")
load_dotenv(override=True)

def load_instructions(path: str):
    with open(path, "r", encoding="utf-8") as f:
        return f.read()

def load_knowledge_base(path: str) -> str:
    with open(path, "r", encoding="utf-8") as f:
        return f.read()

kb_path = os.path.join(os.path.dirname(__file__), "kb.txt")
kb_text = load_knowledge_base(kb_path)

instruction_path = os.path.join(os.path.dirname(__file__), "prompt.txt")
instruction_text = load_instructions(instruction_path)

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
    # Your LiveKit credentials (can be loaded from env vars)
    api_key = os.getenv("LIVEKIT_API_KEY", "APIAMrTXLVoxLqe")
    api_secret = os.getenv("LIVEKIT_API_SECRET", "3pFSQsUzLLeEEWrvO1hJaP4QA97CNeoMEkQA6wWSkuS")
    room = os.getenv("LIVEKIT_ROOM", "my_private_sales_room_2024")
    participant_name = "AI_Assistant"
    
    # Generate token for the AI assistant
    token = generate_livekit_token(
        api_key=api_key,
        api_secret=api_secret,
        room=room,
        participant_name=participant_name,
        ttl_seconds=7200  # 2 hours
    )
    
    # LiveKit URL - using the one from your second script
    url = os.getenv("LIVEKIT_URL", "wss://telecmi-vuq7uhg6.livekit.cloud")
    
    logger.info(f"Generated token for AI assistant in room: {room}")
    logger.info(f"Participant name: {participant_name}")
    
    return url, token, room

async def fetch_weather_from_api(params: FunctionCallParams):
    temperature = 75 if params.arguments["format"] == "fahrenheit" else 24
    await params.result_callback(
        {
            "conditions": "nice",
            "temperature": temperature,
            "format": params.arguments["format"],
            "timestamp": datetime.now().strftime("%Y%m%d_%H%M%S"),
        }
    )

weather_function = FunctionSchema(
    name="get_current_weather",
    description="Get the current weather",
    properties={
        "location": {
            "type": "string",
            "description": "The city and state, e.g. San Francisco, CA",
        },
        "format": {
            "type": "string",
            "enum": ["celsius", "fahrenheit"],
            "description": "The temperature unit to use. Infer this from the users location.",
        },
    },
    required=["location", "format"],
)

# Create tools schema
tools = ToolsSchema(standard_tools=[weather_function])

async def run_bot():
    logger.info(f"Starting bot")

    # Setup LiveKit connection
    (url, token, room_name) = await setup_livekit_connection()

    # Create LiveKit transport instead of SmallWebRTC
    transport = LiveKitTransport(
        url=url,
        token=token,
        room_name=room_name,
        params=LiveKitParams(
            audio_in_enabled=True,
            audio_out_enabled=True,
            audio_in_sample_rate=16000,
            audio_out_sample_rate=24000,
            audio_in_filter=NoisereduceFilter(),
            vad_analyzer=SileroVADAnalyzer(params=VADParams(stop_secs=0.8)),
        ),
    )

    session_properties = SessionProperties(
        input_audio_transcription=InputAudioTranscription(),
        # Set openai TurnDetection parameters. Not setting this at all will turn it
        # on by default
        turn_detection=SemanticTurnDetection(),
        # Or set to False to disable openai turn detection and use transport VAD
        # turn_detection=False,
        input_audio_noise_reduction=InputAudioNoiseReduction(type="near_field"),
        # tools=tools,
        instructions=f"{instruction_text}\n\nKnowledge Base:\n{kb_text}",
    )

    llm = OpenAIRealtimeBetaLLMService(
        api_key="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",
        session_properties=session_properties,
        start_audio_paused=False,
    )

    # you can either register a single function for all function calls, or specific functions
    # llm.register_function(None, fetch_weather_from_api)
    llm.register_function("get_current_weather", fetch_weather_from_api)

    # Create a standard OpenAI LLM context object using the normal messages format. The
    # OpenAIRealtimeBetaLLMService will convert this internally to messages that the
    # openai WebSocket API can understand.
    context = OpenAILLMContext(
        [{"role": "user", "content": "Say hello!"}],
        tools,
    )

    context_aggregator = llm.create_context_aggregator(context)

    pipeline = Pipeline(
        [
            transport.input(),  # Transport user input
            context_aggregator.user(),
            llm,  # LLM
            transport.output(),  # Transport bot output
            context_aggregator.assistant(),
        ]
    )

    task = PipelineTask(
        pipeline,
        params=PipelineParams(
            allow_interruptions=True,
            enable_metrics=True,
            enable_usage_metrics=True,
            report_only_initial_ttfb=True,
            
        ),
    )

    # LiveKit specific event handlers
    @transport.event_handler("on_first_participant_joined")
    async def on_first_participant_joined(transport, participant_id):
        logger.info(f"First participant joined: {participant_id}")
        # Kick off the conversation.
        await task.queue_frames([context_aggregator.user().get_context_frame()])

    @transport.event_handler("on_participant_left")
    async def on_participant_left(transport, participant_id, reason):
        logger.info(f"Participant {participant_id} left the room: {reason}")
        await task.cancel()

    @transport.event_handler("on_disconnected")
    async def on_disconnected(transport):
        logger.info(f"Transport disconnected")
        await task.cancel()

    runner = PipelineRunner(handle_sigint=False)

    await runner.run(task)


if __name__ == "__main__":
    import asyncio
    asyncio.run(run_bot())