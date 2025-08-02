#
# Copyright (c) 2024–2025, Daily
#
# SPDX-License-Identifier: BSD 2-Clause License
#

"""simple_dialin.py.

Daily PSTN Dial-in Bot.
"""

import argparse
import asyncio
import json
import os
import sys
import argparse
import os
import aiohttp
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


# Setup logging
load_dotenv()
logger.remove(0)
logger.add(sys.stderr, level="DEBUG")


# daily_api_key = os.getenv("DAILY_API_KEY", "")
daily_api_key = "f5e2da08f004fc294fc580cf5d7a092df5390a48f1a9a914be0ff36cea56ad19"
daily_api_url = "https://api.daily.co/v1" #os.getenv("DAILY_API_URL", "https://api.daily.co/v1")

async def fetch_weather_from_api(params: FunctionCallParams):
    await params.llm.push_frame(TTSSpeakFrame("Let me check on that."))
    await params.result_callback({"conditions": "nice", "temperature": "75"})


async def run_bot(
    room_url: str,
    token: str,
    body: dict,
) -> None:
    """Run the voice bot with the given parameters.

    Args:
        room_url: The Daily room URL
        token: The Daily room token
        body: Body passed to the bot from the webhook

    """
    # ------------ CONFIGURATION AND SETUP ------------
    logger.info(f"Starting bot with room: {room_url}")
    logger.info(f"Token: {token}")
    logger.info(f"Body: {body}")
    # Parse the body to get the dial-in settings
    body_data = json.loads(body)

    # Check if the body contains dial-in settings
    logger.debug(f"Body data: {body_data}")

    if not all([body_data.get("callId"), body_data.get("callDomain")]):
        logger.error("Call ID and Call Domain are required in the body.")
        return None

    call_id = body_data.get("callId")
    call_domain = body_data.get("callDomain")
    logger.debug(f"Call ID: {call_id}")
    logger.debug(f"Call Domain: {call_domain}")

    if not call_id or not call_domain:
        logger.error("Call ID and Call Domain are required for dial-in.")
        sys.exit(1)

    daily_dialin_settings = DailyDialinSettings(call_id=call_id, call_domain=call_domain)
    logger.debug(f"Dial-in settings: {daily_dialin_settings}")
    transport_params = DailyParams(
        api_url=daily_api_url,
        api_key=daily_api_key,
        dialin_settings=daily_dialin_settings,
        audio_in_enabled=True,
        audio_out_enabled=True,
        video_out_enabled=False,
        vad_analyzer=SileroVADAnalyzer(),
        transcription_enabled=True,
    )
    logger.debug("setup transport params")

    # Initialize transport with Daily
    transport = DailyTransport(
        room_url,
        token,
        "Simple Dial-in Bot",
        transport_params,
    )
    logger.debug("setup transport")


    stt = WhisperSTTService(
        model=Model.LARGE,
        # model = MLXModel.LARGE_V3_TURBO_Q4,
        device="cuda",
        compute_type="int8",
        no_speech_prob=0.4,
        language=Language.EN,
    )
    
    groq_api_key = "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" #os.getenv("GROQ_API_KEY")


    llm = GroqLLMService(
        api_key=groq_api_key,
        # model= "meta-llama/llama-4-maverick-17b-128e-instruct" #"llama3-8b-8192",
        model="llama3-8b-8192",
    )

    llm.register_function("get_current_weather", fetch_weather_from_api)
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
                "description": "The temperature unit to use. Infer this from the user's location.",
            },
        },
        required=["location"],
    )
    tools = ToolsSchema(standard_tools=[weather_function])
    messages = [
            {
                "role": "system",
                "content": """
                You are SalesPro — a warm, friendly, and proactive AI sales assistant that communicates only in **English**.

                **Your role:**
                - Engage users in meaningful conversations about their sales activities.
                - Ask thoughtful questions about their sales process, targets, leads, challenges, recent wins, and goals.
                - Offer support, encouragement, and actionable suggestions for improving sales performance.
                - Help users track their sales pipeline, discuss follow-ups, and explore new opportunities.

                **Guidelines for all responses:**
                - Respond only in English.
                - Use a conversational, simple, and motivating tone.
                - Keep answers and questions short — 2-3 sentences per turn.
                - Always prompt the user to share more about their sales work, achievements, or current focus.
                - If the user goes off-topic, gently guide the conversation back to sales and professional growth.
                - When someone first speaks to you, greet them warmly and introduce yourself as SalesPro.

                **Examples:**
                - Hi there! I am  SalesPro, your AI sales assistant. How are your sales going this week?
                - What's your current sales target, and how close are you to achieving it?
                - Can you tell me about a recent challenge you faced in closing a deal?
                - Great job! Would you like any tips for following up with leads?

                Be supportive, results-oriented, and always eager to help the user succeed in their sales journey!
                """
            },
        ]
    context = OpenAILLMContext(messages, tools=tools)
    context_aggregator = llm.create_context_aggregator(context)
    # Initialize TTS
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
    logger.debug("setup tts")

    logger.debug("setup context aggregator")

    # ------------ PIPELINE SETUP ------------

    # Build the pipeline
    pipeline = Pipeline(
        [
            transport.input(),  # Transport user input
            stt,
            context_aggregator.user(),  # User responses
            llm,  # LLM
            tts,  # TTS
            transport.output(),  # Transport bot output
            context_aggregator.assistant(),  # Assistant spoken responses
        ]
    )
    logger.debug("setup pipeline")

    # Create pipeline task
    task = PipelineTask(
        pipeline,
        params=PipelineParams(
            enable_metrics=True,
            enable_usage_metrics=True,
        ),
    )
    logger.debug("setup task")

    # ------------ EVENT HANDLERS ------------

    @transport.event_handler("on_first_participant_joined")
    async def on_first_participant_joined(transport, participant):
        logger.debug(f"First participant joined: {participant['id']}")
        await transport.capture_participant_transcription(participant["id"])
        await task.queue_frames([context_aggregator.user().get_context_frame()])

    @transport.event_handler("on_participant_left")
    async def on_participant_left(transport, participant, reason):
        logger.debug(f"Participant left: {participant}, reason: {reason}")
        await task.cancel()

    @transport.event_handler("on_dialin_ready")
    async def on_dialin_ready(transport, cdata):
        logger.debug(f"Dial-in ready: {cdata}")

    @transport.event_handler("on_dialin_connected")
    async def on_dialin_connected(transport, data):
        logger.debug(f"Dial-in connected: {data}")

    @transport.event_handler("on_dialin_stopped")
    async def on_dialin_stopped(transport, data):
        logger.debug(f"Dial-in stopped: {data}")

    @transport.event_handler("on_dialin_error")
    async def on_dialin_error(transport, data):
        logger.error(f"Dial-in error: {data}")
        # If there is an error, the bot should leave the call
        # This may be also handled in on_participant_left with
        # await task.cancel()

    @transport.event_handler("on_dialin_warning")
    async def on_dialin_warning(transport, data):
        logger.warning(f"Dial-in warning: {data}")

    # Run the pipeline
    runner = PipelineRunner()
    await runner.run(task)


async def main():
    """Parse command line arguments and run the bot."""
    parser = argparse.ArgumentParser(description="Simple Dial-in Bot")
    parser.add_argument("-u", "--url", type=str, help="Daily room URL")
    parser.add_argument("-t", "--token", type=str, help="Daily room token")
    parser.add_argument("-b", "--body", type=str, help="JSON configuration string")

    args = parser.parse_args()

    logger.debug(f"url: {args.url}")
    logger.debug(f"token: {args.token}")
    logger.debug(f"body: {args.body}")
    if not all([args.url, args.token, args.body]):
        logger.error("All arguments (-u, -t, -b) are required")
        parser.print_help()
        sys.exit(1)

    await run_bot(args.url, args.token, args.body)


if __name__ == "__main__":
    asyncio.run(main())