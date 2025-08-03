# import asyncio
# import base64
# import json
# import uuid
# from typing import Optional, AsyncGenerator
# import websockets
# import numpy as np
# from loguru import logger

# from pipecat.frames.frames import (
#     AudioRawFrame, 
#     TranscriptionFrame, 
#     Frame,
#     StartFrame,
#     EndFrame,
#     ErrorFrame
# )
# from pipecat.services.stt_service import STTService


# class UltravoxSTTService(STTService):
#     """Ultravox Speech-to-Text service using WebSocket connection."""
    
#     def __init__(
#         self,
#         *,
#         server_url: str = "ws://localhost:60032",
#         model_name: str = "fixie-ai/ultravox-v0_5-llama-3_1-8b",
#         hf_token: Optional[str] = None,
#         temperature: float = 0.7,
#         max_tokens: int = 400,
#         sample_rate: int = 16000,
#         audio_format: str = "pcm_s16le",
#         **kwargs
#     ):
#         super().__init__(**kwargs)
        
#         self._server_url = server_url
#         self._model_name = model_name
#         self._hf_token = hf_token
#         self._temperature = temperature
#         self._max_tokens = max_tokens
#         self._sample_rate = sample_rate
#         self._audio_format = audio_format
        
#         self._websocket = None
#         self._session_id = str(uuid.uuid4())
#         self._audio_buffer = []
#         self._is_connected = False
#         self._connection_lock = asyncio.Lock()
#         self._pending_audio_chunks = 0
        
#         logger.info(f"Initialized UltravoxSTTService with session_id: {self._session_id}")
    
#     async def start(self, frame: StartFrame):
#         """Start the service and establish WebSocket connection."""
#         await super().start(frame)
#         await self._connect()
    
#     async def stop(self, frame: EndFrame):
#         """Stop the service and close WebSocket connection."""
#         await self._disconnect()
#         await super().stop(frame)
    
#     async def cancel(self, frame: Frame):
#         """Cancel the service."""
#         await self._disconnect()
#         await super().cancel(frame)
    
#     async def _connect(self):
#         """Establish WebSocket connection to Ultravox server."""
#         async with self._connection_lock:
#             if self._is_connected:
#                 return
                
#             try:
#                 ws_url = f"{self._server_url}/ws/transcribe/{self._session_id}"
#                 logger.info(f"Connecting to Ultravox WebSocket: {ws_url}")
                
#                 self._websocket = await websockets.connect(ws_url)
#                 self._is_connected = True
                
#                 # Start listening for responses
#                 asyncio.create_task(self._listen_for_responses())
                
#                 logger.info("Successfully connected to Ultravox WebSocket")
                
#             except Exception as e:
#                 logger.error(f"Failed to connect to Ultravox WebSocket: {e}")
#                 self._is_connected = False
#                 await self.push_error(ErrorFrame(f"WebSocket connection failed: {e}"))
    
#     async def _disconnect(self):
#         """Close WebSocket connection."""
#         async with self._connection_lock:
#             if self._websocket and self._is_connected:
#                 try:
#                     await self._websocket.close()
#                     logger.info("WebSocket connection closed")
#                 except Exception as e:
#                     logger.error(f"Error closing WebSocket: {e}")
#                 finally:
#                     self._websocket = None
#                     self._is_connected = False
    
#     async def _listen_for_responses(self):
#         """Listen for transcription responses from WebSocket."""
#         try:
#             while self._is_connected and self._websocket:
#                 try:
#                     message = await self._websocket.recv()
#                     data = json.loads(message)
#                     await self._handle_websocket_message(data)
                    
#                 except websockets.exceptions.ConnectionClosed:
#                     logger.info("WebSocket connection closed by server")
#                     break
#                 except json.JSONDecodeError as e:
#                     logger.error(f"Failed to decode WebSocket message: {e}")
#                 except Exception as e:
#                     logger.error(f"Error in WebSocket listener: {e}")
                    
#         except Exception as e:
#             logger.error(f"WebSocket listener error: {e}")
#         finally:
#             self._is_connected = False
    
#     async def _handle_websocket_message(self, data: dict):
#         """Handle incoming WebSocket messages."""
#         message_type = data.get("type")
        
#         if message_type == "transcription_chunk":
#             text = data.get("text", "")
#             if text.strip():
#                 # Push transcription frame directly to pipeline
#                 await self.push_frame(TranscriptionFrame(text, "", 0))
                
#         elif message_type == "transcription_complete":
#             logger.debug(f"Transcription complete for session: {self._session_id}")
            
#         elif message_type == "chunk_received":
#             logger.debug("Audio chunk received by server")
            
#         elif message_type == "error":
#             error_msg = data.get("message", "Unknown error")
#             logger.error(f"Server error: {error_msg}")
#             await self.push_error(ErrorFrame(f"Ultravox error: {error_msg}"))
            
#         elif message_type == "pong":
#             logger.debug("Received pong from server")
    
#     async def process_audio_frame(self, frame: AudioRawFrame, direction):
#         """Override the audio processing to handle WebSocket STT directly."""
#         if not self._is_connected:
#             await self._connect()
            
#         if not self._is_connected:
#             logger.error("Not connected to Ultravox server")
#             return
        
#         try:
#             # Convert audio to the expected format
#             audio_data = self._prepare_audio_data(frame.audio)
            
#             if len(audio_data) == 0:
#                 logger.warning(f"Empty audio frame received for STT service: {self.__class__.__name__} {len(frame.audio)}")
#                 return
            
#             # Add to buffer
#             self._audio_buffer.append(audio_data)
#             self._pending_audio_chunks += 1
            
#             # Send audio chunk (not final)
#             await self._send_audio_chunk(audio_data, is_final=False)
            
#             # Check if we should send final chunk and process transcription
#             if self._pending_audio_chunks >= 10:  # Process every 10 chunks (~200ms)
#                 await self._flush_and_transcribe()
                
#         except Exception as e:
#             logger.error(f"Error processing audio frame: {e}")

#     async def run_stt(self, audio: bytes) -> AsyncGenerator[str, None]:
#         """This method is required by the parent class but we handle audio in process_audio_frame."""
#         # This is now a no-op since we handle everything in process_audio_frame
#         return
#         yield  # Make it a generator
    
#     async def _flush_and_transcribe(self):
#         """Send accumulated audio as final chunk to trigger transcription."""
#         if not self._audio_buffer:
#             return
            
#         try:
#             # Concatenate all audio chunks
#             full_audio = np.concatenate(self._audio_buffer)
            
#             # Send final chunk
#             await self._send_audio_chunk(full_audio, is_final=True)
            
#             # Clear buffer and reset counter
#             self._audio_buffer = []
#             self._pending_audio_chunks = 0
            
#         except Exception as e:
#             logger.error(f"Error flushing audio: {e}")

#     async def flush_audio(self):
#         """Flush accumulated audio and get final transcription."""
#         await self._flush_and_transcribe()
    
#     def _prepare_audio_data(self, audio: bytes) -> np.ndarray:
#         """Convert audio bytes to numpy array."""
#         try:
#             # Assume audio is 16-bit PCM
#             audio_int16 = np.frombuffer(audio, dtype=np.int16)
#             # Convert to float32 and normalize
#             audio_float32 = audio_int16.astype(np.float32) / 32768.0
#             return audio_float32
            
#         except Exception as e:
#             logger.error(f"Error preparing audio data: {e}")
#             return np.array([], dtype=np.float32)
    
#     async def _send_audio_chunk(self, audio_data: np.ndarray, is_final: bool = False):
#         """Send audio chunk to WebSocket."""
#         if not self._websocket or not self._is_connected:
#             return
            
#         try:
#             # Convert to int16 for transmission
#             audio_int16 = (audio_data * 32767).astype(np.int16)
#             audio_bytes = audio_int16.tobytes()
            
#             # Encode as base64
#             audio_b64 = base64.b64encode(audio_bytes).decode('utf-8')
            
#             # Prepare message
#             message = {
#                 "type": "audio_chunk",
#                 "audio_data": audio_b64,
#                 "is_final": is_final,
#                 "temperature": self._temperature,
#                 "max_tokens": self._max_tokens
#             }
            
#             # Send to WebSocket
#             await self._websocket.send(json.dumps(message))
            
#             logger.debug(f"Sent audio chunk (final: {is_final}, size: {len(audio_bytes)} bytes)")
            
#         except Exception as e:
#             logger.error(f"Error sending audio chunk: {e}")


# # Alternative implementation that accumulates audio and sends in larger chunks
# class UltravoxSTTServiceBatched(UltravoxSTTService):
#     """Batched version that accumulates audio before sending."""
    
#     def __init__(self, batch_duration_ms: int = 1000, **kwargs):
#         super().__init__(**kwargs)
#         self._batch_duration_ms = batch_duration_ms
#         self._batch_size = int(self._sample_rate * batch_duration_ms / 1000)
#         self._current_batch = []
        
#     async def process_audio_frame(self, frame: AudioRawFrame, direction):
#         """Accumulate audio and send in batches."""
#         if not self._is_connected:
#             await self._connect()
            
#         if not self._is_connected:
#             return
        
#         try:
#             audio_data = self._prepare_audio_data(frame.audio)
            
#             if len(audio_data) == 0:
#                 return
                
#             self._current_batch.append(audio_data)
            
#             # Check if we have enough for a batch
#             total_samples = sum(len(chunk) for chunk in self._current_batch)
            
#             if total_samples >= self._batch_size:
#                 # Send accumulated batch
#                 batch_audio = np.concatenate(self._current_batch)
#                 await self._send_audio_chunk(batch_audio, is_final=True)
                
#                 # Clear batch
#                 self._current_batch = []
                
#         except Exception as e:
#             logger.error(f"Error in batched STT: {e}")
    
#     async def flush_audio(self):
#         """Send any remaining audio in the current batch."""
#         if self._current_batch:
#             try:
#                 batch_audio = np.concatenate(self._current_batch)
#                 await self._send_audio_chunk(batch_audio, is_final=True)
#                 self._current_batch = []
#             except Exception as e:
#                 logger.error(f"Error flushing batched audio: {e}")


# # Usage example
# async def create_ultravox_service():
#     """Example of how to create the service."""
#     return UltravoxSTTService(
#         server_url="ws://103.247.19.245:8000",  # Your Ultravox server URL
#         model_name="fixie-ai/ultravox-v0_5-llama-3_1-8b",
#         temperature=0.7,
#         max_tokens=100,
#         sample_rate=16000
#     )

import asyncio
import base64
import json
import uuid
from typing import Optional, AsyncGenerator
import websockets
import numpy as np
from loguru import logger

# from pipecat.frames.frames import (
#     AudioRawFrame, 
#     TranscriptionFrame, 
#     Frame,
#     StartFrame,
#     EndFrame,
#     ErrorFrame
# )
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
from pipecat.services.stt_service import STTService


class UltravoxSTTService(STTService):
    """Ultravox Speech-to-Text service using WebSocket connection."""
    
    def __init__(
        self,
        *,
        server_url: str = "ws://localhost:60032",
        model_name: str = "fixie-ai/ultravox-v0_5-llama-3_1-8b",
        hf_token: Optional[str] = None,
        temperature: float = 0.7,
        max_tokens: int = 400,
        sample_rate: int = 16000,
        audio_format: str = "pcm_s16le",
        min_audio_length: int = 1600,  # Minimum audio samples before processing (~100ms at 16kHz)
        **kwargs
    ):
        super().__init__(**kwargs)
        
        self._server_url = server_url
        self._model_name = model_name
        self._hf_token = hf_token
        self._temperature = temperature
        self._max_tokens = max_tokens
        self._sample_rate = sample_rate
        self._audio_format = audio_format
        self._min_audio_length = min_audio_length
        
        self._websocket = None
        self._session_id = str(uuid.uuid4())
        self._audio_buffer = []
        self._is_connected = False
        self._connection_lock = asyncio.Lock()
        self._pending_audio_chunks = 0
        self._empty_frame_count = 0
        self._total_frame_count = 0
        
        logger.info(f"Initialized UltravoxSTTService with session_id: {self._session_id}")
    
    async def start(self, frame: StartFrame):
        """Start the service and establish WebSocket connection."""
        await super().start(frame)
        await self._connect()
    
    async def stop(self, frame: EndFrame):
        """Stop the service and close WebSocket connection."""
        await self._disconnect()
        await super().stop(frame)
    
    async def cancel(self, frame: Frame):
        """Cancel the service."""
        await self._disconnect()
        await super().cancel(frame)
    
    async def _connect(self):
        """Establish WebSocket connection to Ultravox server."""
        async with self._connection_lock:
            if self._is_connected:
                return
                
            try:
                ws_url = f"{self._server_url}/ws/transcribe/{self._session_id}"
                logger.info(f"Connecting to Ultravox WebSocket: {ws_url}")
                
                self._websocket = await websockets.connect(ws_url)
                self._is_connected = True
                
                # Start listening for responses
                asyncio.create_task(self._listen_for_responses())
                
                logger.info("Successfully connected to Ultravox WebSocket")
                
            except Exception as e:
                logger.error(f"Failed to connect to Ultravox WebSocket: {e}")
                self._is_connected = False
                await self.push_error(ErrorFrame(f"WebSocket connection failed: {e}"))
    
    async def _disconnect(self):
        """Close WebSocket connection."""
        async with self._connection_lock:
            if self._websocket and self._is_connected:
                try:
                    await self._websocket.close()
                    logger.info("WebSocket connection closed")
                except Exception as e:
                    logger.error(f"Error closing WebSocket: {e}")
                finally:
                    self._websocket = None
                    self._is_connected = False
    
    async def _listen_for_responses(self):
        """Listen for transcription responses from WebSocket."""
        try:
            while self._is_connected and self._websocket:
                try:
                    message = await self._websocket.recv()
                    data = json.loads(message)
                    # yield LLMFullResponseStartFrame() 
                    await self._handle_websocket_message(data)
                    # yield LLMFullResponseEndFrame()
                    
                except websockets.exceptions.ConnectionClosed:
                    logger.info("WebSocket connection closed by server")
                    break
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to decode WebSocket message: {e}")
                except Exception as e:
                    logger.error(f"Error in WebSocket listener: {e}")
                    
        except Exception as e:
            logger.error(f"WebSocket listener error: {e}")
        finally:
            self._is_connected = False
    
    async def _handle_websocket_message(self, data: dict):
        """Handle incoming WebSocket messages."""
        message_type = data.get("type")
        
        if message_type == "transcription_chunk":
            text = data.get("text", "")
            if text.strip():
                # Push transcription frame directly to pipeline
                logger.info(f"📝 Transcription: {text}")
                # await self.push_frame(LLFrame(text, "", 0))
                await self.push_frame(LLMTextFrame(text))
                
        elif message_type == "transcription_complete":
            logger.debug(f"Transcription complete for session: {self._session_id}")
            
        elif message_type == "chunk_received":
            logger.debug("Audio chunk received by server")
            
        elif message_type == "error":
            error_msg = data.get("message", "Unknown error")
            logger.error(f"Server error: {error_msg}")
            await self.push_error(ErrorFrame(f"Ultravox error: {error_msg}"))
            
        elif message_type == "pong":
            logger.debug("Received pong from server")
    
    async def process_audio_frame(self, frame: AudioRawFrame, direction):
        """Override the audio processing to handle WebSocket STT directly."""
        self._total_frame_count += 1
        
        if not self._is_connected:
            await self._connect()
            
        if not self._is_connected:
            logger.error("Not connected to Ultravox server")
            return
        
        try:
            # Check if frame has audio data
            if not frame.audio or len(frame.audio) == 0:
                self._empty_frame_count += 1
                # Log statistics occasionally
                if self._total_frame_count % 100 == 0:
                    empty_percentage = (self._empty_frame_count / self._total_frame_count) * 100
                    logger.debug(f"Audio stats: {self._empty_frame_count}/{self._total_frame_count} empty frames ({empty_percentage:.1f}%)")
                return
            
            # Convert audio to the expected format
            audio_data = self._prepare_audio_data(frame.audio)
            
            if len(audio_data) == 0:
                logger.debug(f"Audio conversion resulted in empty data (input size: {len(frame.audio)})")
                return
            
            # Add to buffer
            self._audio_buffer.append(audio_data)
            self._pending_audio_chunks += 1
            
            # Send audio chunk (not final)
            await self._send_audio_chunk(audio_data, is_final=False)
            
            # Check if we should send final chunk and process transcription
            if self._pending_audio_chunks >= 10:  # Process every 10 chunks (~200ms)
                await self._flush_and_transcribe()
                
        except Exception as e:
            logger.error(f"Error processing audio frame: {e}")

    async def run_stt(self, audio: bytes) -> AsyncGenerator[str, None]:
        """This method is required by the parent class but we handle audio in process_audio_frame."""
        # This is now a no-op since we handle everything in process_audio_frame
        return
        yield  # Make it a generator
    
    async def _flush_and_transcribe(self):
        """Send accumulated audio as final chunk to trigger transcription."""
        if not self._audio_buffer:
            return
            
        try:
            # Concatenate all audio chunks
            full_audio = np.concatenate(self._audio_buffer)
            
            # Only send if we have enough audio data
            if len(full_audio) >= self._min_audio_length:
                # Send final chunk
                await self._send_audio_chunk(full_audio, is_final=True)
                logger.debug(f"Flushed {len(full_audio)} audio samples to server")
            else:
                logger.debug(f"Skipping flush - not enough audio data ({len(full_audio)} < {self._min_audio_length})")
            
            # Clear buffer and reset counter
            self._audio_buffer = []
            self._pending_audio_chunks = 0
            
        except Exception as e:
            logger.error(f"Error flushing audio: {e}")

    async def flush_audio(self):
        """Flush accumulated audio and get final transcription."""
        await self._flush_and_transcribe()
    
    def _prepare_audio_data(self, audio: bytes) -> np.ndarray:
        """Convert audio bytes to numpy array."""
        try:
            if not audio or len(audio) == 0:
                return np.array([], dtype=np.float32)
            
            # Ensure we have an even number of bytes for 16-bit audio
            if len(audio) % 2 != 0:
                logger.debug(f"Odd number of bytes in audio frame: {len(audio)}, padding")
                audio = audio + b'\x00'
            
            # Assume audio is 16-bit PCM
            audio_int16 = np.frombuffer(audio, dtype=np.int16)
            
            # Convert to float32 and normalize
            audio_float32 = audio_int16.astype(np.float32) / 32768.0
            
            return audio_float32
            
        except Exception as e:
            logger.error(f"Error preparing audio data: {e}")
            return np.array([], dtype=np.float32)
    
    async def _send_audio_chunk(self, audio_data: np.ndarray, is_final: bool = False):
        """Send audio chunk to WebSocket."""
        if not self._websocket or not self._is_connected:
            return
            
        try:
            # Convert to int16 for transmission
            audio_int16 = (audio_data * 32767).astype(np.int16)
            audio_bytes = audio_int16.tobytes()
            
            # Encode as base64
            audio_b64 = base64.b64encode(audio_bytes).decode('utf-8')
            
            # Prepare message
            message = {
                "type": "audio_chunk",
                "audio_data": audio_b64,
                "is_final": is_final,
                "temperature": self._temperature,
                "max_tokens": self._max_tokens
            }
            
            # Send to WebSocket
            await self._websocket.send(json.dumps(message))
            
            logger.debug(f"Sent audio chunk (final: {is_final}, size: {len(audio_bytes)} bytes)")
            
        except Exception as e:
            logger.error(f"Error sending audio chunk: {e}")


# Alternative implementation that accumulates audio and sends in larger chunks
class UltravoxSTTServiceBatched(UltravoxSTTService):
    """Batched version that accumulates audio before sending."""
    
    def __init__(self, batch_duration_ms: int = 1000, **kwargs):
        super().__init__(**kwargs)
        self._batch_duration_ms = batch_duration_ms
        self._batch_size = int(self._sample_rate * batch_duration_ms / 1000)
        self._current_batch = []
        
    async def process_audio_frame(self, frame: AudioRawFrame, direction):
        """Accumulate audio and send in batches."""
        self._total_frame_count += 1
        
        if not self._is_connected:
            await self._connect()
            
        if not self._is_connected:
            return
        
        try:
            # Skip empty frames silently
            if not frame.audio or len(frame.audio) == 0:
                self._empty_frame_count += 1
                return
                
            audio_data = self._prepare_audio_data(frame.audio)
            
            if len(audio_data) == 0:
                return
                
            self._current_batch.append(audio_data)
            
            # Check if we have enough for a batch
            total_samples = sum(len(chunk) for chunk in self._current_batch)
            
            if total_samples >= self._batch_size:
                # Send accumulated batch
                batch_audio = np.concatenate(self._current_batch)
                await self._send_audio_chunk(batch_audio, is_final=True)
                logger.info(f"Sent batch of {total_samples} samples ({total_samples/self._sample_rate:.2f}s)")
                
                # Clear batch
                self._current_batch = []
                
        except Exception as e:
            logger.error(f"Error in batched STT: {e}")
    
    async def flush_audio(self):
        """Send any remaining audio in the current batch."""
        if self._current_batch:
            try:
                batch_audio = np.concatenate(self._current_batch)
                await self._send_audio_chunk(batch_audio, is_final=True)
                logger.info(f"Flushed remaining batch of {len(batch_audio)} samples")
                self._current_batch = []
            except Exception as e:
                logger.error(f"Error flushing batched audio: {e}")


# Usage example
async def create_ultravox_service():
    """Example of how to create the service."""
    return UltravoxSTTService(
        server_url="ws://103.247.19.245:60032",  # Your Ultravox server URL
        model_name="fixie-ai/ultravox-v0_5-llama-3_1-8b",
        temperature=0.7,
        max_tokens=100,
        sample_rate=16000,
        min_audio_length=1600  # ~100ms at 16kHz
    )