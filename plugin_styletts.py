import asyncio
import json
import websockets
from typing import AsyncGenerator, Optional
from loguru import logger
import io
import wave

from pipecat.frames.frames import (
    ErrorFrame,
    Frame,
    StartFrame,
    TTSAudioRawFrame,
    TTSStartedFrame,
    TTSStoppedFrame,
    StartInterruptionFrame,
    CancelFrame,
    EndFrame,
)
from pipecat.services.tts_service import TTSService
from pipecat.transcriptions.language import Language
from pipecat.utils.tracing.service_decorators import traced_tts
from pipecat.processors.frame_processor import FrameDirection

class StyleTTSWebSocketService(TTSService):
    """WebSocket-based StyleTTS service for pipecat pipelines"""

    def __init__(
        self,
        websocket_url: str = "ws://localhost:4009/ws/tts",
        voice_id: Optional[str] = None,
        language: Language = Language.EN,
        sample_rate: int = 24000,
        alpha: float = 0.5,
        beta: float = 0.9,
        diffusion_steps: int = 5,
        embedding_scale: float = 1.5,
        buffer_threshold_seconds: float = 0.05,
        sentence_fragment_delimiters: str = ".?!।॥",
        chunk_size_ms: int = 100,
        priority: int = 0,
        **kwargs
    ):
        # Initialize with required sample rate
        super().__init__(sample_rate=sample_rate, **kwargs)
        
        self._websocket_url = websocket_url
        self._voice_id = voice_id
        self._language = language
        self._sample_rate = sample_rate
        self._alpha = alpha
        self._beta = beta
        self._diffusion_steps = diffusion_steps
        self._embedding_scale = embedding_scale
        self._buffer_threshold_seconds = buffer_threshold_seconds
        self._sentence_fragment_delimiters = sentence_fragment_delimiters
        self._chunk_size_ms = chunk_size_ms
        self._priority = priority
        
        self._websocket: Optional[websockets.WebSocketClientProtocol] = None
        self._session_id: Optional[str] = None
        self._is_generating = False
        self._connection_lock = asyncio.Lock()
        self._audio_chunks = []
        self._generation_complete = asyncio.Event()
        self._started = False
        self._connect_timeout = 10.0
        self._max_retries = 3
        
        logger.info(f"StyleTTS WebSocket service initialized for {websocket_url}")

    def _is_websocket_closed(self, ws) -> bool:
        """Check if websocket is closed in a version-agnostic way"""
        if ws is None:
            return True
        
        # Try different methods to check if closed
        try:
            # For newer versions of websockets
            if hasattr(ws, 'closed'):
                return ws.closed
            # For older versions, check state
            elif hasattr(ws, 'state'):
                from websockets.client import State
                return ws.state == State.CLOSED
            # Fallback: try to check if open
            elif hasattr(ws, 'open'):
                return not ws.open
            else:
                # If none of the above work, assume it's open
                return False
        except Exception:
            # If any error occurs, assume closed
            return True

    async def start(self, frame: Frame) -> None:
        """Start the service and establish WebSocket connection"""
        await super().start(frame)
        self._started = True
        
        # Try to establish connection with retries
        for attempt in range(self._max_retries):
            try:
                await self._ensure_connection()
                break
            except Exception as e:
                logger.warning(f"Connection attempt {attempt + 1} failed: {e}")
                if attempt == self._max_retries - 1:
                    logger.error("Failed to establish WebSocket connection after all retries")
                    raise
                await asyncio.sleep(1.0)

    async def stop(self, frame: Frame) -> None:
        """Stop the service and close WebSocket connection"""
        self._started = False
        await self._close_connection()
        await super().stop(frame)

    async def cancel(self, frame: CancelFrame) -> None:
        """Cancel any ongoing generation"""
        if self._is_generating and self._websocket:
            try:
                # CRITICAL: Send interrupt to WebSocket server
                interrupt_msg = {
                    "type": "interrupt",
                    "data": {}
                }
                await self._websocket.send(json.dumps(interrupt_msg))
                logger.info("Sent interrupt to WebSocket TTS server")
                self._is_generating = False
            except Exception as e:
                logger.error(f"Error canceling generation: {e}")
        await super().cancel(frame)

    async def _ensure_connection(self):
        """Ensure WebSocket connection is established"""
        if not self._started:
            return
            
        async with self._connection_lock:
            if self._websocket is None or self._is_websocket_closed(self._websocket):
                await self._connect()

    async def _connect(self):
        """Establish WebSocket connection"""
        try:
            logger.info(f"Connecting to WebSocket TTS server at {self._websocket_url}")
            
            self._websocket = await asyncio.wait_for(
                websockets.connect(
                    self._websocket_url,
                    ping_interval=30,
                    ping_timeout=10,
                    close_timeout=5,
                    max_size=16 * 1024 * 1024  # 16MB max message size
                ),
                timeout=self._connect_timeout
            )
            
            # Wait for connection confirmation
            try:
                message = await asyncio.wait_for(self._websocket.recv(), timeout=5.0)
                data = json.loads(message)
                
                if data["type"] == "connected":
                    self._session_id = data["data"]["session_id"]
                    logger.info(f"WebSocket TTS connected with session: {self._session_id}")
                else:
                    raise Exception(f"Unexpected connection response: {data}")
                    
            except asyncio.TimeoutError:
                raise Exception("Timeout waiting for connection confirmation")
                
        except Exception as e:
            logger.error(f"Failed to connect to WebSocket TTS server: {e}")
            if self._websocket:
                try:
                    await self._websocket.close()
                except:
                    pass
            self._websocket = None
            raise

    async def _close_connection(self):
        """Close WebSocket connection"""
        async with self._connection_lock:
            if self._websocket:
                try:
                    # Send a graceful close if still generating
                    if self._is_generating:
                        await self._send_interrupt()
                        await asyncio.sleep(0.1)
                    
                    await self._websocket.close()
                except Exception as e:
                    logger.error(f"Error closing WebSocket connection: {e}")
                finally:
                    self._websocket = None
                    self._session_id = None
                    logger.info("WebSocket TTS connection closed")

    async def _send_interrupt(self):
        """Send interrupt message to stop current generation"""
        if self._is_generating and self._websocket and not self._is_websocket_closed(self._websocket):
            try:
                interrupt_msg = {
                    "type": "interrupt",
                    "data": {}
                }
                await self._websocket.send(json.dumps(interrupt_msg))
                logger.info("Sent interrupt to WebSocket TTS server")
                self._is_generating = False
            except Exception as e:
                logger.error(f"Error sending interrupt: {e}")

    async def _handle_interruption(self, frame: StartInterruptionFrame, direction: FrameDirection):
        """Handle interruption frame"""
        # First, let pipecat handle its internal interruption logic
        await super()._handle_interruption(frame, direction)
        # Then, send interrupt to your WebSocket server
        await self._send_interrupt()

    @traced_tts
    async def run_tts(self, text: str) -> AsyncGenerator[Frame, None]:
        """Generate TTS audio from text via WebSocket"""
        if not self._started:
            yield ErrorFrame("StyleTTS service not started")
            return
            
        try:
            await self._ensure_connection()
            
            if not self._websocket or self._is_websocket_closed(self._websocket):
                # Try to reconnect once
                try:
                    await self._connect()
                except Exception as e:
                    yield ErrorFrame(f"WebSocket TTS connection failed: {str(e)}")
                    return

            logger.info(f"TTS for text: '{text[:50]}{'...' if len(text) > 50 else ''}'")
            
            # Send TTS started frame
            yield TTSStartedFrame()
            
            # Prepare TTS request
            tts_request = {
                "type": "tts_request",
                "data": {
                    "text": text,
                    "voice_id": self._voice_id,
                    # "language": self._language.value if hasattr(self._language, 'value') else str(self._language),
                    "alpha": self._alpha,
                    "beta": self._beta,
                    "diffusion_steps": self._diffusion_steps,
                    "embedding_scale": self._embedding_scale,
                    "buffer_threshold_seconds": self._buffer_threshold_seconds,
                    "sentence_fragment_delimiters": self._sentence_fragment_delimiters,
                    "chunk_size_ms": self._chunk_size_ms,
                    "priority": self._priority
                }
            }
            
            # Reset state
            self._audio_chunks = []
            self._generation_complete.clear()
            self._is_generating = True
            
            # Send request
            await self._websocket.send(json.dumps(tts_request))
            
            # Listen for audio chunks and status messages
            audio_received = False
            timeout_counter = 0
            max_timeout_cycles = 50  # 5 seconds at 0.1s per cycle
            
            async for frame in self._receive_audio_stream():
                if isinstance(frame, TTSAudioRawFrame):
                    audio_received = True
                    timeout_counter = 0  # Reset timeout when we receive audio
                yield frame
                
                # Check for timeout if no audio received
                if not audio_received:
                    timeout_counter += 1
                    if timeout_counter >= max_timeout_cycles:
                        logger.warning("TTS generation timeout - no audio received")
                        yield ErrorFrame("TTS generation timeout")
                        break
                        
        except Exception as e:
            logger.error(f"Error in TTS generation: {e}")
            yield ErrorFrame(f"TTS generation failed: {str(e)}")
        finally:
            self._is_generating = False
            yield TTSStoppedFrame()

    async def _receive_audio_stream(self) -> AsyncGenerator[Frame, None]:
        """Receive and process audio stream from WebSocket"""
        try:
            while self._is_generating and self._websocket and not self._is_websocket_closed(self._websocket):
                try:
                    # Wait for message with timeout
                    message = await asyncio.wait_for(self._websocket.recv(), timeout=0.1)
                    
                    # Handle binary audio data
                    if isinstance(message, bytes):
                        # Convert bytes to audio frame
                        audio_frame = TTSAudioRawFrame(
                            audio=message,
                            sample_rate=self._sample_rate,
                            num_channels=1
                        )
                        yield audio_frame
                        
                    # Handle JSON status messages
                    else:
                        try:
                            data = json.loads(message)
                            await self._handle_status_message(data)
                            
                        except json.JSONDecodeError:
                            logger.warning(f"Received non-JSON text message: {message}")
                            
                except asyncio.TimeoutError:
                    # Check if generation is still active
                    if not self._is_generating:
                        break
                    continue
                    
                except websockets.exceptions.ConnectionClosed:
                    logger.warning("WebSocket connection closed during audio streaming")
                    break
                    
                except Exception as e:
                    logger.error(f"Error receiving message: {e}")
                    break
                    
        except Exception as e:
            logger.error(f"Error receiving audio stream: {e}")
            raise

    async def _handle_status_message(self, data: dict):
        """Handle status messages from WebSocket server"""
        msg_type = data["type"]
        msg_data = data.get("data", {})
        
        if msg_type == "status":
            if msg_data.get("status") == "generating":
                logger.debug(f"TTS generation started for request {msg_data.get('request_id')}")
                
        elif msg_type == "completed":
            logger.info(f"TTS generation completed: {msg_data.get('total_bytes')} bytes, "
                       f"{msg_data.get('synthesis_duration', 0):.3f}s synthesis time")
            self._is_generating = False
            self._generation_complete.set()
            
        elif msg_type == "error":
            logger.error(f"TTS server error: {msg_data.get('error')}")
            self._is_generating = False
            
        elif msg_type == "interrupted":
            logger.info("TTS generation was interrupted")
            self._is_generating = False

    async def say(self, text: str) -> None:
        """Convenience method to generate and play TTS audio"""
        try:
            async for frame in self.run_tts(text):
                if isinstance(frame, TTSAudioRawFrame):
                    # In a real pipeline, this would be handled by the pipeline
                    # For standalone usage, you might want to handle audio playback here
                    pass
                elif isinstance(frame, ErrorFrame):
                    logger.error(f"TTS error: {frame.error}")
                    raise Exception(frame.error)
                    
        except Exception as e:
            logger.error(f"Error in say(): {e}")
            raise

    async def get_connection_status(self) -> dict:
        """Get current connection status"""
        if not self._websocket:
            return {"connected": False, "session_id": None}
            
        if self._is_websocket_closed(self._websocket):
            return {"connected": False, "session_id": self._session_id, "closed": True}
            
        try:
            # Send status request
            status_request = {
                "type": "status",
                "data": {}
            }
            await self._websocket.send(json.dumps(status_request))
            
            # Wait for response
            message = await asyncio.wait_for(self._websocket.recv(), timeout=2.0)
            data = json.loads(message)
            
            if data["type"] == "session_status":
                return {
                    "connected": True,
                    "session_id": self._session_id,
                    **data["data"]
                }
            else:
                return {"connected": True, "session_id": self._session_id}
                
        except Exception as e:
            logger.error(f"Error getting connection status: {e}")
            return {"connected": False, "error": str(e)}

    def __del__(self):
        """Cleanup on destruction"""
        if self._websocket and not self._is_websocket_closed(self._websocket):
            # Note: This won't work in practice since __del__ can't run async code
            # The proper cleanup should happen in stop() or cancel()
            logger.warning("StyleTTSWebSocketService destroyed with open connection")


# Example usage and testing
async def test_websocket_tts():
    """Test function for the WebSocket TTS service"""
    service = StyleTTSWebSocketService(
        websocket_url="ws://localhost:9002/ws/tts",
        alpha=0.3,
        beta=0.8,
        diffusion_steps=2,
        chunk_size_ms=60
    )
    
    try:
        # Create a proper frame with audio attributes
        start_frame = StartFrame()
        
        # Initialize service
        await service.start(start_frame)
        
        # Test connection status
        status = await service.get_connection_status()
        logger.info(f"Connection status: {status}")
        
        # Generate some test audio
        test_text = "Hello, this is a test of the WebSocket StyleTTS service integration with pipecat."
        
        logger.info(f"Generating audio for: {test_text}")
        audio_frames = []
        
        async for frame in service.run_tts(test_text):
            if isinstance(frame, TTSAudioRawFrame):
                audio_frames.append(frame.audio)
                logger.debug(f"Received audio frame: {len(frame.audio)} bytes")
            elif isinstance(frame, TTSStartedFrame):
                logger.info("TTS generation started")
            elif isinstance(frame, TTSStoppedFrame):
                logger.info("TTS generation completed")
            elif isinstance(frame, ErrorFrame):
                logger.error(f"TTS error: {frame.error}")
                
        if audio_frames:
            # Combine all audio data
            combined_audio = b''.join(audio_frames)
            logger.info(f"Total audio generated: {len(combined_audio)} bytes")
            
            # Save to file for testing
            with open("test_websocket_tts.raw", "wb") as f:
                f.write(combined_audio)
            logger.info("Audio saved to test_websocket_tts.raw")
            
    except Exception as e:
        logger.error(f"Test failed: {e}")
        raise
    finally:
        await service.stop(start_frame)


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    asyncio.run(test_websocket_tts())