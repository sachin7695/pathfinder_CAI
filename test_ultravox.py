#!/usr/bin/env python3
"""
Enhanced Test script for Ultravox STT Server with detailed timing measurements

This script focuses on measuring the time taken for text generation from Ultravox.
"""

import asyncio
import base64
import json
import os
import time
import aiohttp
import numpy as np
from loguru import logger
from dataclasses import dataclass
from typing import Optional, Dict, Any

# Test configuration
SERVER_URL = "http://103.247.19.245:60032"
TIMEOUT = 30


@dataclass
class TimingResults:
    """Container for timing measurements."""
    total_request_time: float
    server_processing_time: Optional[float]
    audio_preparation_time: float
    network_latency: Optional[float]
    audio_duration: float
    real_time_factor: Optional[float]  # Processing time / Audio duration


class UltravoxTimer:
    """Enhanced timer for measuring Ultravox performance."""
    
    def __init__(self):
        self.start_time = None
        self.audio_prep_start = None
        self.audio_prep_end = None
        self.request_start = None
        self.request_end = None
    
    def start_audio_preparation(self):
        """Start timing audio preparation."""
        self.audio_prep_start = time.time()
    
    def end_audio_preparation(self):
        """End timing audio preparation."""
        self.audio_prep_end = time.time()
    
    def start_request(self):
        """Start timing the actual request."""
        self.request_start = time.time()
    
    def end_request(self):
        """End timing the request."""
        self.request_end = time.time()
    
    def get_audio_prep_time(self) -> float:
        """Get audio preparation time."""
        if self.audio_prep_start and self.audio_prep_end:
            return self.audio_prep_end - self.audio_prep_start
        return 0.0
    
    def get_total_request_time(self) -> float:
        """Get total request time."""
        if self.request_start and self.request_end:
            return self.request_end - self.request_start
        return 0.0


async def test_health_check():
    """Test server health endpoint."""
    logger.info("🔍 Testing health check...")
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{SERVER_URL}/health", timeout=aiohttp.ClientTimeout(total=10)) as response:
                if response.status == 200:
                    data = await response.json()
                    logger.info(f"✅ Server is healthy: {data}")
                    return data.get("model_loaded", False)
                else:
                    logger.error(f"❌ Health check failed with status {response.status}")
                    return False
    except Exception as e:
        logger.error(f"❌ Health check failed: {e}")
        return False


def calculate_audio_duration(sample_rate: int, audio_data: np.ndarray) -> float:
    """Calculate audio duration in seconds."""
    return len(audio_data) / sample_rate


def log_timing_results(timing: TimingResults, text: str = ""):
    """Log detailed timing results."""
    logger.info("⏱️  TIMING RESULTS:")
    logger.info(f"   📄 Transcribed text: '{text}'")
    logger.info(f"   🎵 Audio duration: {timing.audio_duration:.3f}s")
    logger.info(f"   🔧 Audio preparation: {timing.audio_preparation_time:.3f}s")
    logger.info(f"   🌐 Total request time: {timing.total_request_time:.3f}s")
    
    if timing.server_processing_time:
        logger.info(f"   ⚡ Server processing: {timing.server_processing_time:.3f}s")
        
        if timing.network_latency:
            logger.info(f"   📡 Network latency: {timing.network_latency:.3f}s")
        
        if timing.real_time_factor:
            if timing.real_time_factor < 1.0:
                logger.info(f"   🚀 Real-time factor: {timing.real_time_factor:.2f}x (FASTER than real-time)")
            else:
                logger.info(f"   🐌 Real-time factor: {timing.real_time_factor:.2f}x (slower than real-time)")


async def test_transcription_with_timing():
    """Test transcription with detailed timing measurements."""
    logger.info("🎤 Testing transcription with detailed timing...")
    
    timer = UltravoxTimer()
    
    try:
        # Start audio preparation timing
        timer.start_audio_preparation()
        
        import soundfile as sf
        
        # Load audio from file
        filename = 'test.wav'
        audio_data, sample_rate = sf.read(filename, dtype='int16')
        
        # Calculate audio duration
        audio_duration = calculate_audio_duration(sample_rate, audio_data)
        
        # Convert to bytes and base64 encode
        audio_bytes = audio_data.tobytes()
        audio_b64 = base64.b64encode(audio_bytes).decode('utf-8')
        
        timer.end_audio_preparation()
        
        payload = {
            "audio_data": audio_b64,
            "audio_format": "pcm_s16le",
            "sample_rate": sample_rate,
            "temperature": 0.7,
            "max_tokens": 50
        }
        
        # Start request timing
        timer.start_request()
        
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{SERVER_URL}/transcribe",
                json=payload,
                timeout=aiohttp.ClientTimeout(total=TIMEOUT)
            ) as response:
                timer.end_request()
                
                if response.status == 200:
                    result = await response.json()
                    
                    # Extract timing information
                    server_processing_time = result.get('processing_time')
                    total_request_time = timer.get_total_request_time()
                    audio_prep_time = timer.get_audio_prep_time()
                    
                    # Calculate network latency (if server processing time is available)
                    network_latency = None
                    if server_processing_time:
                        network_latency = total_request_time - server_processing_time
                    
                    # Calculate real-time factor
                    real_time_factor = None
                    if server_processing_time and audio_duration > 0:
                        real_time_factor = server_processing_time / audio_duration
                    
                    # Create timing results
                    timing = TimingResults(
                        total_request_time=total_request_time,
                        server_processing_time=server_processing_time,
                        audio_preparation_time=audio_prep_time,
                        network_latency=network_latency,
                        audio_duration=audio_duration,
                        real_time_factor=real_time_factor
                    )
                    
                    # Log results
                    text = result.get('text', 'No text')
                    logger.info("✅ Transcription successful!")
                    log_timing_results(timing, text)
                    
                    return True, timing
                else:
                    error_text = await response.text()
                    logger.error(f"❌ Transcription failed with status {response.status}: {error_text}")
                    return False, None
                    
    except Exception as e:
        logger.error(f"❌ Transcription test failed: {e}")
        return False, None


async def test_websocket_with_timing():
    """Test WebSocket transcription with timing."""
    logger.info("🔌 Testing WebSocket transcription with timing...")
    
    timer = UltravoxTimer()
    
    try:
        import websockets
        
        # Start audio preparation timing
        timer.start_audio_preparation()
        
        import soundfile as sf
        
        # Load audio from file
        filename = 'test.wav'
        audio_data, sample_rate = sf.read(filename, dtype='int16')
        
        # Calculate audio duration
        audio_duration = calculate_audio_duration(sample_rate, audio_data)
        
        # Convert to bytes and base64 encode
        audio_bytes = audio_data.tobytes()
        audio_b64 = base64.b64encode(audio_bytes).decode('utf-8')
        
        timer.end_audio_preparation()
        
        ws_url = SERVER_URL.replace("http://", "ws://").replace("https://", "wss://")
        ws_url = f"{ws_url}/ws/transcribe/test_session"
        print(ws_url)
        async with websockets.connect(ws_url) as websocket:
            # Send audio chunk with timing
            timer.start_request()
            
            message = {
                "type": "audio_chunk",
                "audio_data": audio_b64,
                "is_final": True,
                "temperature": 0.7,
                "max_tokens": 50
            }
            
            await websocket.send(json.dumps(message))
            logger.info("📤 Sent audio chunk via WebSocket")
            
            # Receive responses
            timeout_count = 0
            received_transcription = False
            transcription_text = ""
            first_response_time = None
            
            while timeout_count < 10:  # Wait up to 10 seconds
                try:
                    response = await asyncio.wait_for(websocket.recv(), timeout=1.0)
                    
                    if first_response_time is None:
                        first_response_time = time.time()
                    
                    data = json.loads(response)
                    
                    msg_type = data.get("type")
                    if msg_type == "transcription_chunk":
                        text = data.get("text", "")
                        transcription_text += text
                        logger.info(f"📥 Received transcription chunk: '{text}'")
                        received_transcription = True
                    elif msg_type == "transcription_complete":
                        timer.end_request()
                        logger.info("✅ WebSocket transcription completed")
                        
                        # Calculate timing
                        total_time = timer.get_total_request_time()
                        audio_prep_time = timer.get_audio_prep_time()
                        
                        # Create timing results
                        timing = TimingResults(
                            total_request_time=total_time,
                            server_processing_time=None,  # Not available in WebSocket
                            audio_preparation_time=audio_prep_time,
                            network_latency=None,
                            audio_duration=audio_duration,
                            real_time_factor=total_time / audio_duration if audio_duration > 0 else None
                        )
                        
                        log_timing_results(timing, transcription_text)
                        return True, timing
                        
                    elif msg_type == "error":
                        logger.error(f"❌ WebSocket error: {data.get('message')}")
                        return False, None
                        
                except asyncio.TimeoutError:
                    timeout_count += 1
                    continue
            
            if received_transcription:
                logger.info("✅ WebSocket test completed (with timeout)")
                return True, None
            else:
                logger.warning("⚠️  WebSocket test completed but no transcription received")
                return False, None
                
    except ImportError:
        logger.warning("⚠️  websockets library not installed, skipping WebSocket test")
        return True, None
    except Exception as e:
        logger.error(f"❌ WebSocket test failed: {e}")
        return False, None


async def run_performance_benchmark(num_tests: int = 5):
    """Run multiple tests to get average performance metrics."""
    logger.info(f"🏁 Running performance benchmark with {num_tests} tests...")
    
    timings = []
    successful_tests = 0
    
    for i in range(num_tests):
        logger.info(f"📊 Running test {i + 1}/{num_tests}")
        success, timing = await test_transcription_with_timing()
        
        if success and timing:
            timings.append(timing)
            successful_tests += 1
        
        # Small delay between tests
        if i < num_tests - 1:
            await asyncio.sleep(1)
    
    if timings:
        # Calculate averages
        avg_total = sum(t.total_request_time for t in timings) / len(timings)
        avg_server = sum(t.server_processing_time for t in timings if t.server_processing_time) / len([t for t in timings if t.server_processing_time])
        avg_audio_duration = sum(t.audio_duration for t in timings) / len(timings)
        avg_rtf = sum(t.real_time_factor for t in timings if t.real_time_factor) / len([t for t in timings if t.real_time_factor])
        
        logger.info("=" * 60)
        logger.info("📈 PERFORMANCE BENCHMARK RESULTS:")
        logger.info(f"   ✅ Successful tests: {successful_tests}/{num_tests}")
        logger.info(f"   ⏱️  Average total request time: {avg_total:.3f}s")
        logger.info(f"   ⚡ Average server processing time: {avg_server:.3f}s")
        logger.info(f"   🎵 Average audio duration: {avg_audio_duration:.3f}s")
        logger.info(f"   🚀 Average real-time factor: {avg_rtf:.2f}x")
        logger.info("=" * 60)


async def run_all_tests():
    """Run all tests with enhanced timing."""
    logger.info(f"🧪 Starting Enhanced Ultravox STT Server Timing Tests")
    logger.info(f"   Server URL: {SERVER_URL}")
    logger.info("=" * 50)
    
    # Test 1: Health check
    health_ok = await test_health_check()
    if not health_ok:
        logger.error("❌ Cannot proceed - server is not healthy")
        return False
    
    print()
    
    # Test 2: Basic transcription with timing
    transcription_ok, timing = await test_transcription_with_timing()
    
    print()
    
    # Test 3: WebSocket transcription with timing
    websocket_ok, ws_timing = await test_websocket_with_timing()
    
    print()
    
    # Test 4: Performance benchmark
    await run_performance_benchmark(3)
    
    print()
    
    # Summary
    logger.info("=" * 50)
    logger.info("📋 Test Summary:")
    logger.info(f"   Health Check: {'✅ PASS' if health_ok else '❌ FAIL'}")
    logger.info(f"   Transcription: {'✅ PASS' if transcription_ok else '❌ FAIL'}")
    logger.info(f"   WebSocket: {'✅ PASS' if websocket_ok else '❌ FAIL'}")
    
    all_passed = health_ok and transcription_ok and websocket_ok
    
    if all_passed:
        logger.info("🎉 All tests passed! Server is working correctly.")
    else:
        logger.error("❌ Some tests failed. Check the logs above.")
    
    return all_passed


async def wait_for_server(max_wait=60):
    """Wait for server to become available."""
    logger.info(f"⏳ Waiting for server to become available (max {max_wait}s)...")
    
    start_time = time.time()
    while time.time() - start_time < max_wait:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(f"{SERVER_URL}/health", timeout=aiohttp.ClientTimeout(total=5)) as response:
                    if response.status == 200:
                        data = await response.json()
                        if data.get("model_loaded", False):
                            logger.info("✅ Server is ready!")
                            return True
                        else:
                            logger.info("⏳ Server is up but model not loaded yet...")
        except:
            pass
        
        await asyncio.sleep(2)
    
    logger.error(f"❌ Server did not become available within {max_wait}s")
    return False


def main():
    """Main test function."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Enhanced Ultravox STT Server Timing Tests")
    parser.add_argument("--wait", action="store_true", help="Wait for server to start")
    parser.add_argument("--max-wait", type=int, default=60, help="Max time to wait for server")
    parser.add_argument("--benchmark", type=int, default=0, help="Run only benchmark with N tests")
    
    args = parser.parse_args()
    
    async def async_main():
        if args.wait:
            if not await wait_for_server(args.max_wait):
                return False
        
        if args.benchmark > 0:
            # Run only benchmark
            health_ok = await test_health_check()
            if health_ok:
                await run_performance_benchmark(args.benchmark)
                return True
            return False
        else:
            return await run_all_tests()
    
    try:
        success = asyncio.run(async_main())
        exit(0 if success else 1)
    except KeyboardInterrupt:
        logger.info("🛑 Tests interrupted by user")
        exit(1)
    except Exception as e:
        logger.error(f"❌ Test execution failed: {e}")
        exit(1)


if __name__ == "__main__":
    main()