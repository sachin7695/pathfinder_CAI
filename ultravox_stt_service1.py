#
# SPDX-License-Identifier: BSD 2-Clause License
#

"""This module implements the Ultravox speech-to-text service using API calls."""
import json
import time
import base64
import asyncio
from typing import AsyncGenerator, List, Optional
import aiohttp

import numpy as np
from loguru import logger

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
from pipecat.processors.frame_processor import FrameDirection
from pipecat.services.ai_service import AIService


class AudioBuffer:
    def __init__(self):
        self.frames: List[AudioRawFrame] = []
        self.started_at: Optional[float] = None
        self.is_processing: bool = False


class UltravoxSTTService(AIService):
    def __init__(
        self,
        *,
        api_base_url: str = "http://localhost:8000",
        temperature: float = 0.7,
        max_tokens: int = 250,
        timeout: float = 30.0,
        instruct: str = "You are a helpful assistant.",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self._api_base_url = api_base_url.rstrip('/')
        self._buffer = AudioBuffer()
        self._temperature = temperature
        self._max_tokens = max_tokens
        self._timeout = timeout
        self._connection_active = False
        self._system_prompt = instruct
        self._session = None
        self.conv_history = []
        logger.info(f"Initialized UltravoxSTTService with API base URL: {self._api_base_url}")

    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create HTTP session."""
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=self._timeout)
            self._session = aiohttp.ClientSession(timeout=timeout)
        return self._session

    async def _close_session(self):
        """Close HTTP session."""
        if self._session and not self._session.closed:
            await self._session.close()

    async def _check_health(self) -> bool:
        """Check if the API server is healthy."""
        try:
            session = await self._get_session()
            async with session.get(f"{self._api_base_url}/health") as response:
                if response.status == 200:
                    data = await response.json()
                    is_healthy = data.get("status") == "healthy" and data.get("model_loaded", False)
                    logger.info(f"Health check: {data}")
                    return is_healthy
                else:
                    logger.error(f"Health check failed with status: {response.status}")
                    return False
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return False

    async def _transcribe_audio_api(self, audio_data: np.ndarray) -> str:
        """Transcribe audio using the API."""
        try:
            session = await self._get_session()
            
            # Convert audio to bytes and encode as base64
            if audio_data.dtype == np.float32:
                # Convert float32 to int16
                audio_int16 = (audio_data * 32767).astype(np.int16)
            else:
                audio_int16 = audio_data.astype(np.int16)
            
            audio_bytes = audio_int16.tobytes()
            audio_b64 = base64.b64encode(audio_bytes).decode('utf-8')
            
            # Prepare request
            request_data = {
                "audio_data": audio_b64,
                "audio_format": "pcm_s16le",
                "sample_rate": 16000,
                "temperature": self._temperature,
                "max_tokens": self._max_tokens
            }
            
            # Make API call
            async with session.post(
                f"{self._api_base_url}/transcribe",
                json=request_data
            ) as response:
                if response.status == 200:
                    result = await response.json()
                    return result.get("text", "")
                else:
                    error_text = await response.text()
                    logger.error(f"API transcription failed with status {response.status}: {error_text}")
                    raise Exception(f"API call failed: {response.status}")
                    
        except Exception as e:
            logger.error(f"Error calling transcription API: {e}")
            raise

    def can_generate_metrics(self) -> bool:
        return True

    def json_process(self, text):
        """Process JSON response from the model."""
        try:
            # Try to parse as JSON first
            if text.strip().startswith('{') and text.strip().endswith('}'):
                return json.loads(text)
            
            # If not JSON, try to extract JSON-like content
            import re
            json_match = re.search(r'\{[^}]*"question"[^}]*"answer"[^}]*\}', text, re.DOTALL)
            if json_match:
                json_str = json_match.group(0)
                # Clean up the JSON string
                json_str = re.sub(r'([{,]\s*)([a-zA-Z_][a-zA-Z0-9_]*)\s*:', r'\1"\2":', json_str)
                return json.loads(json_str)
            
            # Fallback: create a basic structure
            return {
                "question": text,
                "answer": "I don't know"
            }
            
        except Exception as e:
            logger.error(f"Error processing JSON: {e}")
            return {
                "question": text,
                "answer": "I don't know"
            }
    
    async def start(self, frame: StartFrame):
        await super().start(frame)
        self._connection_active = True
        
        # Check if API server is healthy
        if not await self._check_health():
            logger.error("API server is not healthy or model is not loaded")
            raise Exception("API server is not available")
        
        logger.info("UltravoxSTTService started with API connection")

    async def stop(self, frame: EndFrame):
        await super().stop(frame)
        self._connection_active = False
        await self._close_session()
        logger.info("UltravoxSTTService stopped")

    async def cancel(self, frame: CancelFrame):
        await super().cancel(frame)
        self._connection_active = False
        self._buffer = AudioBuffer()
        await self._close_session()
        logger.info("UltravoxSTTService cancelled")

    async def process_frame(self, frame: Frame, direction: FrameDirection):
        await super().process_frame(frame, direction)
        
        if isinstance(frame, UserStartedSpeakingFrame):
            logger.info("Speech started")
            self._buffer = AudioBuffer()
            self._buffer.started_at = time.time()
            
        elif isinstance(frame, AudioRawFrame) and self._buffer.started_at is not None:
            self._buffer.frames.append(frame)
            
        elif isinstance(frame, UserStoppedSpeakingFrame):
            if self._buffer.frames and not self._buffer.is_processing:
                logger.info("Speech ended, processing buffer...")
                await self.process_generator(self._process_audio_buffer())
                return
                
        if frame is not None:
            await self.push_frame(frame, direction)

    async def _process_audio_buffer(self) -> AsyncGenerator[Frame, None]:
        try:
            self._buffer.is_processing = True
            if not self._buffer.frames:
                logger.warning("No audio frames to process")
                yield ErrorFrame("No audio frames to process")
                return

            # Process audio frames
            audio_arrays = []
            for f in self._buffer.frames:
                if hasattr(f, "audio") and f.audio:
                    if isinstance(f.audio, bytes):
                        try:
                            arr = np.frombuffer(f.audio, dtype=np.int16)
                            if arr.size > 0:
                                audio_arrays.append(arr)
                        except Exception as e:
                            logger.error(f"Error processing bytes audio frame: {e}")
                    elif isinstance(f.audio, np.ndarray):
                        if f.audio.size > 0:
                            if f.audio.dtype != np.int16:
                                logger.info(f"Converting array from {f.audio.dtype} to int16")
                                audio_arrays.append(f.audio.astype(np.int16))
                            else:
                                audio_arrays.append(f.audio)

            if not audio_arrays:
                logger.warning("No valid audio data found in frames")
                yield ErrorFrame("No valid audio data found in frames")
                return

            # Concatenate audio data
            audio_data = np.concatenate(audio_arrays)
            audio_float32 = audio_data.astype(np.float32) / 32768.0

            try:
                logger.info("Generating text from audio using API...")
                await self.start_ttfb_metrics()
                await self.start_processing_metrics()
                yield LLMFullResponseStartFrame()

                # Call the API for transcription
                complete_text = await self._transcribe_audio_api(audio_float32)
                await self.stop_ttfb_metrics()

                logger.info(f"API response: {complete_text}")

                # Process the response
                try:
                    json_response = self.json_process(complete_text)
                    logger.info(f"Processed JSON response: {json_response}")

                    if isinstance(json_response, str):
                        json_response = json.loads(json_response)

                    answer = json_response.get('answer', complete_text)
                    transcription = json_response.get('question', complete_text)

                    # Update conversation history
                    self.conv_history.append({
                        "role": "user",
                        "content": transcription
                    })
                    self.conv_history.append({
                        "role": "assistant",
                        "content": answer
                    })

                    logger.info("Conversation history:")
                    logger.info(self.conv_history)

                    # Yield the answer as text frames
                    if answer and answer != "I don't know":
                        # Split answer into chunks for streaming effect
                        words = answer.split()
                        for word in words:
                            yield LLMTextFrame(text=word + " ")
                            # Small delay to simulate streaming
                            await asyncio.sleep(0.05)
                    else:
                        yield LLMTextFrame(text=transcription)

                except Exception as e:
                    logger.error(f"Error processing API response: {e}")
                    # Fallback to raw response
                    yield LLMTextFrame(text=complete_text)

                await self.stop_processing_metrics()
                yield LLMFullResponseEndFrame()

            except Exception as e:
                logger.error(f"Error calling API: {e}")
                yield ErrorFrame(f"Error calling API: {str(e)}")

        except Exception as e:
            logger.error(f"Error processing audio buffer: {e}")
            import traceback
            logger.error(traceback.format_exc())
            yield ErrorFrame(f"Error processing audio: {str(e)}")
        finally:
            self._buffer.is_processing = False
            self._buffer.frames = []
            self._buffer.started_at = None