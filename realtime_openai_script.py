import argparse
import os
from datetime import datetime
from typing import Optional, List
from dotenv import load_dotenv
from loguru import logger
from typing import Set
import json
from fastapi import WebSocket
from collections import deque
# from sentence_transformers import SentenceTransformer
# import faiss
import numpy as np
import os
from groq import Groq

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
from groq import AsyncGroq

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

kb_path = os.path.join(os.path.dirname(__file__), "knowledge_base.txt")
# kb_text = load_knowledge_base(kb_path)



instruction_path = os.path.join(os.path.dirname(__file__), "prompt.txt")
instruction_text = load_instrcutions(instruction_path)

#To implement RAG


# Load your model once globally
# EMBED_MODEL = SentenceTransformer("all-MiniLM-L6-v2")  # or your multilingual model

# Load FAISS index and corresponding text chunks
# faiss_index = faiss.read_index("kb.index")
# with open("kb_chunks.txt", encoding="utf-8") as f:
#     KB_CHUNKS = [line.strip() for line in f if line.strip()]

# def retrieve_rag_context(question, top_k=3):
#     q_vec = EMBED_MODEL.encode([question])
#     D, I = faiss_index.search(np.array(q_vec).astype(np.float32), top_k)
#     return "\n".join([KB_CHUNKS[i] for i in I[0]])


# client = AsyncGroq(
#     api_key=groq_api_key  # This is the default and can be omitted
# )
# async def llm_generate(prompt):
#     completion = client.chat.completions.create(
#         model="llama-3.1-8b-instant",
#         messages=[
#             {"role": "user", "content": prompt}
#         ],
#         temperature=0.3,
#         max_completion_tokens=512,
#         stream=False,  # set to True if you want streaming in your app
#     )
#     # If not streaming, just return the final response:
#     return completion.choices[0].message.content.strip()


# async def query_knowledge_base(params: FunctionCallParams):
#     question = params.arguments["question"]
#     context = retrieve_rag_context(question, top_k=3)  # implement this as before
#     prompt = f"""उत्तर नीचे दी गई जानकारी के आधार पर दें:

#     {context}

#     प्रश्न: {question}
#     उत्तर सरल हिंदी में दें:"""
#     answer = await llm_generate(prompt)
#     await params.result_callback({"answer": answer})






#This is Rulling based retrieval 
def search_kb(query, kb_path="kb.txt"):
    # Very basic: match if query words appear in a line or section
    with open(kb_path, encoding="utf-8") as f:
        content = f.read().split("\n\n")  # chunked by double newline
    best = ""
    score = 0
    for chunk in content:
        s = sum(1 for w in query.lower().split() if w in chunk.lower())
        if s > score:
            best = chunk.strip()
            score = s
    return best or "माफ़ कीजिए, मुझे इसका उत्तर नहीं मिला।"


knowledge_base_function = FunctionSchema(
    name="query_knowledge_base",
    description="Answer questions about Anganwadi and Poshan Tracker using the official knowledge base.",
    properties={
        "question": {
            "type": "string",
            "description": "The user's question in Hindi or English."
        }
    },
    required=["question"]
)



async def query_knowledge_base(params: FunctionCallParams):
    question = params.arguments["question"]
    logger.info(f"Searching KB for: {question}")
    
    # Option 2: Use simple file-based search (better)
    answer = search_kb(question, kb_path="kb.txt")
    
    await params.result_callback({"answer": answer})



tools = ToolsSchema(standard_tools=[
    knowledge_base_function
])   

# Add this class before your TranscriptHandler
class TranscriptBroadcaster:
    """Manages WebSocket connections for real-time transcript broadcasting"""
    def __init__(self):
        self.active_connections: Set[WebSocket] = set()
        self.transcript_history = deque(maxlen=100)  # Keep last 100 messages
    
    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.add(websocket)
        logger.info(f"WebSocket connected. Total connections: {len(self.active_connections)}")
        
        # Send existing transcript history to new connection
        for message in self.transcript_history:
            try:
                await websocket.send_json(message)
            except:
                pass
    
    def disconnect(self, websocket: WebSocket):
        self.active_connections.discard(websocket)
        logger.info(f"WebSocket disconnected. Total connections: {len(self.active_connections)}")
    
    async def broadcast_message(self, message: dict):
        """Broadcast message to all connected clients"""
        self.transcript_history.append(message)
        
        # Send to all connected clients
        disconnected = set()
        for connection in self.active_connections:
            try:
                await connection.send_json(message)
            except Exception as e:
                logger.error(f"Error sending to websocket: {e}")
                disconnected.add(connection)
        
        # Clean up disconnected clients
        for conn in disconnected:
            self.disconnect(conn)

# Create a global instance (add this after the TranscriptBroadcaster class)
transcript_broadcaster = TranscriptBroadcaster()

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


        # Broadcast via WebSocket
        await transcript_broadcaster.broadcast_message({
            "type": "transcript",
            "role": message.role,
            "content": message.content,
            "timestamp": message.timestamp or datetime.now().isoformat(),
        })


        
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
    


    #Openai session properties

    session_properties = SessionProperties(
        input_audio_transcription=InputAudioTranscription(),
        turn_detection=SemanticTurnDetection(),
        input_audio_noise_reduction=InputAudioNoiseReduction(type="near_field"),
        # tools=tools,
        instructions=f'''
                 
                {instruction_text}''',
    )

    #speech to speech models
    llm = OpenAIRealtimeBetaLLMService(
        api_key="xx-xx-xx-xx",
        session_properties=session_properties,
        start_audio_paused=False,
    )

    llm.register_function("query_knowledge_base", query_knowledge_base)

    #Transcript handling to log the transcript file
    transcript = TranscriptProcessor()
    transcript_handler = TranscriptHandler(output_file="realtime_openai_transcript.txt")

    context = OpenAILLMContext(
        [{"role": "user", "content": "Conversation started"}],
        tools
    )

    context_aggregator = llm.create_context_aggregator(context)

    pipeline = Pipeline(
        [
            transport.input(),  
            context_aggregator.user(),
            llm,  
            transcript.user(),
            transport.output(),  
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
        # Kick off the conversation. to let LLM follow the system instruction
        await task.queue_frames([context_aggregator.user().get_context_frame()])

    @transport.event_handler("on_client_disconnected")
    async def on_client_disconnected(transport, client):
        logger.info(f"Client disconnected")

    @transcript.event_handler("on_transcript_update")
    async def on_transcript_update(processor, frame):
        # for msg in frame.messages:
        #     if isinstance(msg, TranscriptionMessage):
        #         timestamp = f"[{msg.timestamp}] " if msg.timestamp else ""
        #         line = f"{timestamp}{msg.role}: {msg.content}"
        #         logger.info(f"Transcript: {line}")
        await transcript_handler.on_transcript_update(processor, frame)

    await PipelineRunner(handle_sigint=False).run(task)


if __name__ == "__main__":
    from run import main

    main()
