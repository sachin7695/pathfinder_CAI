#
# Copyright (c) 2024–2025, Daily
#
# SPDX-License-Identifier: BSD 2-Clause License
#

import argparse
import os
from datetime import datetime
from typing import Optional, List
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
from pipecat.frames.frames import TranscriptionMessage, TranscriptionUpdateFrame
from pipecat.pipeline.runner import PipelineRunner
from pipecat.frames.frames import TranscriptionMessage
from pipecat.processors.transcript_processor import TranscriptProcessor
from pipecat.transports.base_transport import TransportParams
from pipecat.transports.network.small_webrtc import SmallWebRTCTransport
from pipecat.transports.network.webrtc_connection import SmallWebRTCConnection

load_dotenv(override=True)

def load_instrcutions(path:str):
    with open(path, "r", encoding="utf-8") as f:
        return f.read()
def load_knowledge_base(path: str) -> str:
    with open(path, "r", encoding="utf-8") as f:
        return f.read()

kb_path = os.path.join(os.path.dirname(__file__), "kb.txt")
kb_text = load_knowledge_base(kb_path)

instruction_path = os.path.join(os.path.dirname(__file__), "prompt.txt")
instruction_text = load_instrcutions(instruction_path)

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

async def run_bot(webrtc_connection: SmallWebRTCConnection, _: argparse.Namespace):
    logger.info(f"Starting bot")

    transport = SmallWebRTCTransport(
        webrtc_connection=webrtc_connection,
        params=TransportParams(
            audio_in_enabled=True,
            audio_out_enabled=True,
            vad_analyzer=SileroVADAnalyzer(params=VADParams(
                        start_secs=0.10,   # react ~100 ms after speech onset
                        # stop_secs=0.25,    # cut off quickly after 250 ms silence
                        # min_volume=0.3,    # make it less strict
                        confidence=0.5,
                        stop_secs=0.25,
                        min_volume=0.6,
                    )),
        ),
    )

    session_properties = SessionProperties(
        input_audio_transcription=InputAudioTranscription(),
        turn_detection=SemanticTurnDetection(),
        input_audio_noise_reduction=InputAudioNoiseReduction(type="near_field"),
        # tools=tools,
        instructions=f"{instruction_text}\n\nKnowledge Base:\n{kb_text}",
    )

    llm = OpenAIRealtimeBetaLLMService(
        api_key="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",
        session_properties=session_properties,
        start_audio_paused=False,
    )

    transcript = TranscriptProcessor()
    transcript_handler = TranscriptHandler(output_file="transcript_openai.txt")
    # Create a standard OpenAI LLM context object using the normal messages format. The
    # OpenAIRealtimeBetaLLMService will convert this internally to messages that the
    # openai WebSocket API can understand.
    context = OpenAILLMContext(
        [{"role": "user", "content": "Say hello!"}],
        # [{"role": "user", "content": [{"type": "text", "text": "Say hello!"}]}],
        #     [
        #         {
        #             "role": "user",
        #             "content": [
        #                 {"type": "text", "text": "Say"},
        #                 {"type": "text", "text": "yo what's up!"},
        #             ],
        #         }
        #     ],
    )

    context_aggregator = llm.create_context_aggregator(context)

    pipeline = Pipeline(
        [
            transport.input(),  # Transport user input
            context_aggregator.user(),
            llm,  # LLM
            transcript.user(),
            transport.output(),  # Transport bot output
            transcript.assistant(),
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

    @transport.event_handler("on_client_connected")
    async def on_client_connected(transport, client):
        logger.info(f"Client connected")
        # Kick off the conversation.
        await task.queue_frames([context_aggregator.user().get_context_frame()])

    @transport.event_handler("on_client_disconnected")
    async def on_client_disconnected(transport, client):
        logger.info(f"Client disconnected")

    # Register event handler for transcript updates
    @transcript.event_handler("on_transcript_update")
    async def on_transcript_update(processor, frame):
        # for msg in frame.messages:
        #     if isinstance(msg, TranscriptionMessage):
        #         timestamp = f"[{msg.timestamp}] " if msg.timestamp else ""
        #         line = f"{timestamp}{msg.role}: {msg.content}"
        #         logger.info(f"Transcript: {line}")
        await transcript_handler.on_transcript_update(processor, frame)

    runner = PipelineRunner(handle_sigint=True)

    await runner.run(task)


if __name__ == "__main__":
    from run import main

    main()
