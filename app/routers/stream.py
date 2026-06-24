"""Here it listens on /media-stream for Twilio's connection, opens a second connection to OpenAI, and passes the binary audio buffers back and forth concurrently"""
import json
import asyncio
from fastapi import APIRouter, WebSocket, WebSocketDisconnect
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

    try:
        # FIX: Added ping_interval=None to stop the library from dropping the connection during heavy audio streams!
        async with websockets.connect(OPENAI_WS_URL, additional_headers=openai_headers, ping_interval=None) as openai_ws:
            print("Connected to OpenAI Realtime API.")
            
            stream_sid = None

            async def receive_from_twilio():
                nonlocal stream_sid
                try:
                    async for message in twilio_ws.iter_text():
                        data = json.loads(message)
                        
                        if data['event'] == 'start':
                            stream_sid = data['start']['streamSid']
                            print(f"Call started. Stream SID: {stream_sid}")
                            
                        elif data['event'] == 'media':
                            if openai_ws.open:
                                audio_event = {
                                    "type": "input_audio_buffer.append",
                                    "audio": data['media']['payload']
                                }
                                await openai_ws.send(json.dumps(audio_event))
                                
                        elif data['event'] == 'stop':
                            print("Twilio call hung up.")
                            break
                except WebSocketDisconnect:
                    print("Twilio WebSocket disconnected.")
                except Exception as e:
                    print(f"Error reading from Twilio: {e}")

            async def send_to_twilio():
                nonlocal stream_sid
                try:
                    # 1. Send the session configuration immediately
                    session_update = {
                        "type": "session.update",
                        "session": {
                            "modalities": ["audio", "text"],
                            "instructions": "You are a helpful phone assistant. Be highly concise.",
                            "voice": "alloy",
                            "input_audio_format": "g711_ulaw", 
                            "output_audio_format": "g711_ulaw",
                            "turn_detection": {
                                "type": "server_vad"
                            }
                        }
                    }
                    await openai_ws.send(json.dumps(session_update))

                    # 2. Process incoming messages from OpenAI
                    async for message in openai_ws:
                        response = json.loads(message)
                        event_type = response.get("type")
                        
                        # THE PCM16 STATIC FIX: 
                        # Wait until OpenAI confirms the audio format is U-Law BEFORE making it speak
                        if event_type == "session.updated":
                            print("Session configured to U-Law successfully. Triggering AI greeting.")
                            initial_greeting = {
                                "type": "response.create",
                                "response": {
                                    "instructions": "Say: Hello! I am connected. How can I help you today?"
                                }
                            }
                            await openai_ws.send(json.dumps(initial_greeting))

                        # Route the AI's audio chunks back to the phone
                        elif event_type == "response.audio.delta" and stream_sid:
                            twilio_message = {
                                "event": "media",
                                "streamSid": stream_sid,
                                "media": {
                                    "payload": response["delta"]
                                }
                            }
                            await twilio_ws.send_text(json.dumps(twilio_message))
                            
                        # Debug logging (ignoring audio deltas to prevent spam)
                        elif event_type not in ["response.audio.delta", "input_audio_buffer.append"]:
                            print(f"OpenAI Event: {event_type}")

                except Exception as e:
                    print(f"Error in OpenAI loop: {e}")

            # Run both WebSocket streams concurrently
            await asyncio.gather(receive_from_twilio(), send_to_twilio())

    except Exception as e:
        print(f"Failed to connect to OpenAI: {e}")