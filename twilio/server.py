#
# Copyright (c) 2025, Daily
#
# SPDX-License-Identifier: BSD 2-Clause License
#

import argparse
import json

import uvicorn
from bot import run_bot
from fastapi import FastAPI, WebSocket
from fastapi.middleware.cors import CORSMiddleware
from starlette.responses import HTMLResponse

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allow all origins for testing
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.post("/")
async def start_call():
    print("POST TwiML")
    return HTMLResponse(content=open("./templates/streams.xml").read(), media_type="application/xml")


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    try:
        print("WebSocket connection attempt...")
        await websocket.accept()
        print("WebSocket connection accepted")
        
        start_data = websocket.iter_text()
        print("Waiting for start message...")
        await start_data.__anext__()  # First message (usually connection info)
        
        print("Waiting for call data...")
        call_data_raw = await start_data.__anext__()
        print(f"Raw call data: {call_data_raw}")
        
        call_data = json.loads(call_data_raw)
        print(f"Parsed call data: {call_data}")
        
        stream_sid = call_data["start"]["streamSid"]
        call_sid = call_data["start"]["callSid"]
        print(f"Stream SID: {stream_sid}, Call SID: {call_sid}")
        
        print("Starting bot...")
        await run_bot(websocket, stream_sid, call_sid, app.state.testing)
        
    except Exception as e:
        print(f"WebSocket error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Pipecat Twilio Chatbot Server")
    parser.add_argument(
        "-t", "--test", action="store_true", default=False, help="set the server in testing mode"
    )
    args, _ = parser.parse_known_args()

    app.state.testing = args.test

    uvicorn.run(app, host="0.0.0.0", port=6099)