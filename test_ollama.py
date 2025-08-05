import argparse
import os
import aiohttp
from dotenv import load_dotenv
from loguru import logger
import sys
import asyncio
from dotenv import load_dotenv
sys.path.append('/home/user/voice/pipcat/pipecat/src')

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
from pipecat.frames.frames import  TTSSpeakFrame,  LLMFullResponseStartFrame, TextFrame, LLMFullResponseEndFrame
            
# Import your WebSocket StyleTTS service
# from styletts_websocket_service import 
# from agentic_planner import agentic_schedule_by_names

from plugin_styletts import StyleTTSWebSocketService
from plugin_chatterbox import ChatterboxWebSocketService
from aiortc import RTCConfiguration, RTCIceServer



# load_dotenv(override=True)
# # Recommended environment variables for Ultravox 8B:
# os.environ.setdefault("VLLM_MAX_MODEL_LEN", "128")
# os.environ.setdefault("VLLM_MAX_NUM_SEQS", "1")
# os.environ.setdefault("VLLM_GPU_MEMORY_UTILIZATION", "0.4")
# os.environ.setdefault("VLLM_DISABLE_CUSTOM_ALL_REDUCE", "true")
# os.environ.setdefault("PYTORCH_CUDA_ALLOC_CONF", "expandable_segments:True,max_split_size_mb:21")


    
async def fetch_weather_from_api(params: FunctionCallParams):
    await params.llm.push_frame(TTSSpeakFrame("Let me check on that."))
    await params.result_callback({"conditions": "nice", "temperature": "75"})

async def run_bot(webrtc_connection: SmallWebRTCConnection, args: argparse.Namespace):
    logger.info(f"Starting simplified bot with Groq LLM and WebSocket TTS")

    # Improved VAD configuration
    vad_analyzer = SileroVADAnalyzer(
        params=VADParams(
            stop_secs=0.25,
            confidence = 0.4,
            start_secs=0.1,
            min_volume=0.7,
        )
    )
    transport = SmallWebRTCTransport(
        webrtc_connection=webrtc_connection,
        params=TransportParams(
            audio_in_enabled=True,
            audio_out_enabled=True,
            audio_in_sample_rate=16000,
            audio_out_sample_rate=24000,
            vad_analyzer=vad_analyzer,
            audio_in_filter = NoisereduceFilter(),
            vad_enabled=True,
        ),
    )


    # Initialize STT service
    stt = WhisperSTTService(
        model=Model.LARGE,
        # model = MLXModel.LARGE_V3_TURBO_Q4,
        device="cuda",
        compute_type="int8",
        no_speech_prob=0.4,
        language=Language.EN,
    )
    
    groq_api_key = "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" #os.getenv("GROQ_API_KEY")

    statement_llm = GroqLLMService(
        api_key=groq_api_key,
        model="llama3-8b-8192",
    )
    statement_messages = [
        {
            "role": "system",
            "content": "Determine if the user's statement is a complete sentence or question, ending in a natural pause or punctuation. Return 'YES' if it is complete and 'NO' if it seems to leave a thought unfinished.",
        },
    ]

    statement_context = OpenAILLMContext(statement_messages)
    statement_context_aggregator = statement_llm.create_context_aggregator(statement_context)


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
    
    # Create simple context with greeting instruction
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

                # "role": "system",
                # "content": """
                # You are SalesPro — a warm, friendly, and proactive AI sales assistant that communicates only in **Hindi** (देवनागरी लिपि).

                # **Your role:**
                # - उपयोगकर्ताओं के साथ उनके सेल्स एक्टिविटीज़ के बारे में सार्थक बातचीत करें।
                # - उनके सेल्स प्रोसेस, टारगेट, लीड्स, चुनौतियाँ, हाल की सफलताएँ और लक्ष्य पूछें।
                # - सेल्स प्रदर्शन बेहतर करने के लिए समर्थन, प्रेरणा और व्यावहारिक सुझाव दें।
                # - सेल्स पाइपलाइन ट्रैक करने, फॉलो-अप पर चर्चा करने और नए अवसरों की खोज करने में मदद करें।

                # **Guidelines for all responses:**
                # - केवल हिंदी (देवनागरी लिपि) में उत्तर दें।
                # - भाषा सरल, प्रेरणादायक और व्यक्तिगत रखें।
                # - हर टर्न में 2–3 वाक्यों से ज़्यादा न लिखें।
                # - यूज़र को हमेशा उनकी सेल्स प्रगति, उपलब्धियाँ या वर्तमान फ़ोकस शेयर करने के लिए प्रॉम्प्ट करें।
                # - अगर यूज़र विषय से भटक जाए तो विनम्रता से बातचीत को सेल्स और प्रोफेशनल ग्रोथ पर वापस लाएं।
                # - जब यूज़र पहली बार आपसे बात करे, तो उन्हें गर्मजोशी से नमस्ते कहें और अपने आप को SalesPro के रूप में परिचित कराएँ।

                # **Examples (in Hindi):**
                # - नमस्ते! मैं SalesPro हूँ, आपका AI सेल्स असिस्टेंट। इस हफ़्ते आपकी सेल्स कैसी चल रही हैं?
                # - आपका वर्तमान सेल्स टारगेट क्या है, और आप उससे कितने करीब हैं?
                # - क्या आप हाल ही में किसी डील क्लोज़ करते समय आई किसी चुनौती के बारे में बता सकते हैं?
                # - बढ़िया काम किया! क्या आप लीड्स को फॉलो-अप करने के लिए कोई टिप्स चाहते हैं?
                # """
            },
        ]
    context = OpenAILLMContext(messages, tools=tools)
    # # Create context aggregator
    # context_aggregator = llm.create_context_aggregator(
    #     context, user_params=LLMUserAggregatorParams(aggregation_timeout=0.5)
    # )
    context_aggregator = llm.create_context_aggregator(context)


    # We have instructed the LLM to return 'YES' if it thinks the user
    # completed a sentence. So, if it's 'YES' we will return true in this
    # predicate which will wake up the notifier.
    async def wake_check_filter(frame):
        return frame.text == "YES"

    # This is a notifier that we use to synchronize the two LLMs.
    notifier = EventNotifier()

    # This a filter that will wake up the notifier if the given predicate
    # (wake_check_filter) returns true.
    completness_check = WakeNotifierFilter(notifier, types=(TextFrame,), filter=wake_check_filter)

    # This processor keeps the last context and will let it through once the
    # notifier is woken up. We start with the gate open because we send an
    # initial context frame to start the conversation.
    gated_context_aggregator = GatedOpenAILLMContextAggregator(notifier=notifier, start_open=True)

    # Notify if the user hasn't said anything.
    async def user_idle_notifier(frame):
        await notifier.notify()

    # Sometimes the LLM will fail detecting if a user has completed a
    # sentence, this will wake up the notifier if that happens.
    user_idle = UserIdleProcessor(callback=user_idle_notifier, timeout=3.0)

    # Initialize WebSocket StyleTTS service
    # websocket_url = os.getenv("TTS_WEBSOCKET_URL", "ws://103.247.19.245:60031/ws/tts")
    # tts = StyleTTSWebSocketService(
    #     websocket_url=websocket_url,
    #     voice_id=None,
    #     language=Language.EN,
    #     sample_rate=24000,
    #     alpha=0.5,
    #     beta=0.5,
    #     diffusion_steps=3,
    #     embedding_scale=1.1,
    #     buffer_threshold_seconds=0.0,
    #     sentence_fragment_delimiters="।॥.!?,;:",
    #     chunk_size_ms=100,
    # )

    tts1 = ChatterboxWebSocketService(
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

    # Set up pipeline
    pipeline = Pipeline([
        transport.input(),
        stt,
        # ParallelPipeline(
        #         [
        #             statement_context_aggregator.user(),
        #             statement_llm,
        #             completness_check,
        #             NullFilter(),
        #         ],
        #         [context_aggregator.user(), gated_context_aggregator, llm],
        # ),
        # user_idle,
        context_aggregator.user(),
        llm,
        tts1,
        transport.output(),
        context_aggregator.assistant()
    ])

    # Create task
    task = PipelineTask(
        pipeline,
        params=PipelineParams(
            allow_interruptions=True,
            enable_metrics=True,
            enable_usage_metrics=True,
            report_only_initial_ttfb=True,
        ),
    )


    # Set up transport event handlers
    @transport.event_handler("on_client_connected")
    async def on_client_connected(transport, client):
        logger.info(f"Client connected")
        
        # Wait for pipeline to be ready and send start frame
        await asyncio.sleep(1.0)
        
        try:
            # Test TTS connection
            # greeting_text =  "नमस्ते मैं SalesPro हूँ, आपका निजी AI सेल्स असिस्टेंट। मैं यहाँ आपकी मदद के लिए हूँ ।"
            # greeting_text = "ahhh, hello i am AI assistant your personal helper!!"
            #TTS frame
            # speak_frame = TTSSpeakFrame(text=greeting_text)


            # await task.queue_frames([
            #     LLMFullResponseStartFrame(),
            #     TextFrame(text=greeting_text),
            #     LLMFullResponseEndFrame()
            # ])

            # # await task.queue_frame(speak_frame)
            # context = OpenAILLMContext()
            # context.add_message({
            #     "role": "assistant",
            #     "content": greeting_text
            # })


            messages.append({"role": "system", "content": "Please introduce yourself to the user."})
            await task.queue_frames([context_aggregator.user().get_context_frame()])

        except Exception as e:
            logger.error(f"Error checking TTS connection: {e}")

    @transport.event_handler("on_client_disconnected")
    async def on_client_disconnected(transport, client):
        logger.info(f"Client disconnected")

    @transport.event_handler("on_client_closed")
    async def on_client_closed(transport, client):
        logger.info(f"Client closed connection")
        try:
            await task.cancel()
        except Exception as e:
            logger.error(f"Error stopping task: {e}")

    # Error handling function (can be used by processors if needed)
    async def handle_pipeline_error(error):
        logger.error(f"Pipeline error: {error}")
        # Add any custom error handling logic here

    # Run the pipeline
    runner = PipelineRunner(handle_sigint=False)
    
    try:
        logger.info("Starting pipeline runner...")
        await runner.run(task)
    except Exception as e:
        logger.error(f"Error running pipeline: {e}")
        logger.exception("Pipeline error traceback:")
        await handle_pipeline_error(e)
        raise

if __name__ == "__main__":
    from run import main
    main()