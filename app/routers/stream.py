"""Here it listens on /media-stream for Twilio's connection, opens a second connection to OpenAI, and passes the binary audio buffers back and forth concurrently"""
import json
import asyncio
from fastapi import APIRouter, WebSocket
from app.config import settings
import websockets

router = APIRouter()

# FIX: Updated the model name from the deprecated preview version to the official GA version
OPENAI_WS_URL = "wss://api.openai.com/v1/realtime?model=gpt-realtime"

@router.websocket("/media-stream")
async def handle_media_stream(twilio_ws: WebSocket):
    """
    Handles the live audio stream between Twilio and OpenAI.
    """
    await twilio_ws.accept()
    print("Twilio phone stream connected.")

    # 1. GA MIGRATION: Removed the deprecated "OpenAI-Beta" header
    openai_headers = {
        "Authorization": f"Bearer {settings.OPENAI_API_KEY}"
    }

    async with websockets.connect(OPENAI_WS_URL, additional_headers=openai_headers) as openai_ws:
        print("Connected to OpenAI Realtime API.")
        
        # Track the Twilio stream ID to ensure audio routes back to the correct phone call
        stream_sid = None

        # Initialize the session parameters with OpenAI
        await initialize_openai_session(openai_ws)

        # Define the task to receive audio from Twilio and send it to OpenAI
        async def receive_from_twilio():
            nonlocal stream_sid
            try:
                async for message in twilio_ws.iter_text():
                    data = json.loads(message)
                    
                    if data['event'] == 'start':
                        stream_sid = data['start']['streamSid']
                        print(f"Call started. Stream SID: {stream_sid}")
                        
                    elif data['event'] == 'media':
                        # Extract raw base64 audio payload from Twilio
                        base64_audio = data['media']['payload']
                        
                        # Forward audio chunk directly to OpenAI
                        audio_event = {
                            "type": "input_audio_buffer.append",
                            "audio": base64_audio
                        }
                        await openai_ws.send(json.dumps(audio_event))
                        
                    elif data['event'] == 'stop':
                        print("Twilio call hung up.")
                        break
            except Exception as e:
                print(f"Error reading from Twilio: {e}")

        # Define the task to receive audio from OpenAI and send it back to the phone
        async def send_to_twilio():
            try:
                async for message in openai_ws:
                    response = json.loads(message)
                    
                    # FIX: Changed back to 'response.audio.delta' to catch the AI's voice packets
                    if response.get("type") == "response.audio.delta" and stream_sid:
                        base64_output_audio = response["delta"]
                        
                        # Format the packet exactly how Twilio expects it
                        twilio_message = {
                            "event": "media",
                            "streamSid": stream_sid,
                            "media": {
                                "payload": base64_output_audio
                            }
                        }
                        await twilio_ws.send_text(json.dumps(twilio_message))
            except Exception as e:
                print(f"Error sending to Twilio: {e}")

        # Run both data-pipelines concurrently
        await asyncio.gather(receive_from_twilio(), send_to_twilio())


async def initialize_openai_session(openai_ws):
    """
    Configures how OpenAI acts, its voice profile, and audio formats.
    """
    session_update = {
        "type": "session.update",
        "session": {
            "modalities": ["audio", "text"],
            "instructions": "You are a helpful, witty, and highly concise phone assistant. Keep answers brief since this is a phone call.",
            "voice": "alloy",
            # FIX: Reverted to the flat structure required for Twilio's telephone encoding
            "input_audio_format": "g711_ulaw",
            "output_audio_format": "g711_ulaw",
            "turn_detection": {
                "type": "server_vad"
            }
        }
    }
    await openai_ws.send(json.dumps(session_update))

    # FIX: Force the AI to speak first so you know the connection is live!
    initial_greeting = {
        "type": "response.create",
        "response": {
            "instructions": "Greet the user warmly with 'Hello! I am connected. How can I help you today?'"
        }
    }
    await openai_ws.send(json.dumps(initial_greeting))