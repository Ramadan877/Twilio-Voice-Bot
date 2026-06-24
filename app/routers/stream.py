"""Here it listens on /media-stream for Twilio's connection, opens a second connection to OpenAI, and passes the binary audio buffers back and forth concurrently"""
import json
import asyncio
from fastapi import APIRouter, WebSocket
from app.config import settings
import websockets

router = APIRouter()

OPENAI_WS_URL = "wss://api.openai.com/v1/realtime?model=gpt-realtime"

@router.websocket("/media-stream")
async def handle_media_stream(twilio_ws: WebSocket):
    """
    Handles the live audio stream between Twilio and OpenAI.
    """
    await twilio_ws.accept()
    print("Twilio phone stream connected.")

    openai_headers = {
        "Authorization": f"Bearer {settings.OPENAI_API_KEY}"
    }

    async with websockets.connect(OPENAI_WS_URL, additional_headers=openai_headers) as openai_ws:
        print("Connected to OpenAI Realtime API.")
        
        # 1. THE RACE CONDITION FIX: 
        # Halt everything and wait for Twilio to give us the stream_sid FIRST.
        stream_sid = None
        while stream_sid is None:
            message = await twilio_ws.receive_text()
            data = json.loads(message)
            if data.get('event') == 'start':
                stream_sid = data['start']['streamSid']
                print(f"Call started. Stream SID captured: {stream_sid}")
            # If random media arrives early, ignore it until we have the SID
            elif data.get('event') == 'media':
                pass

        # Now that we safely have the ID, initialize the AI and force it to speak
        await initialize_openai_session(openai_ws)

        # Define the task to receive audio from Twilio and send it to OpenAI
        async def receive_from_twilio():
            try:
                # iter_text() will pick up right where the while loop left off
                async for message in twilio_ws.iter_text():
                    data = json.loads(message)
                    
                    if data['event'] == 'media':
                        # Forward audio chunk directly to OpenAI
                        audio_event = {
                            "type": "input_audio_buffer.append",
                            "audio": data['media']['payload']
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
                    event_type = response.get("type")
                    
                    # DEBUG LOGGING: So we are never blind in the void again!
                    # (We ignore printing 'delta' so it doesn't spam the console 100x a second)
                    if "delta" not in event_type:
                        print(f"OpenAI Event: {event_type}")
                    
                    # 2. THE EVENT FIX: Catching the official GA audio delta packet
                    if event_type == "response.output_audio.delta":
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
            "input_audio_format": "g711_ulaw", 
            "output_audio_format": "g711_ulaw",
            "turn_detection": {
                "type": "server_vad"
            }
        }
    }
    await openai_ws.send(json.dumps(session_update))

    initial_greeting = {
        "type": "response.create",
        "response": {
            "instructions": "Greet the user warmly with 'Hello! I am connected. How can I help you today?'"
        }
    }
    await openai_ws.send(json.dumps(initial_greeting))