"""Here it listens on /media-stream for Twilio's connection, opens a second connection to OpenAI, and passes the binary audio buffers back and forth concurrently"""
                        
import json
import asyncio
import logging
from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from app.config import settings
import websockets

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

router = APIRouter()

# Using the exact URL and stable model you referenced
OPENAI_WS_URL = "wss://api.openai.com/v1/realtime?model=gpt-4o-realtime-preview-2024-12-17"

@router.websocket("/media-stream")
async def handle_media_stream(twilio_ws: WebSocket):
    """
    Handles the live audio stream between Twilio and OpenAI.
    """
    await twilio_ws.accept()
    logger.info("Twilio phone stream connected.")

    # Using the standard headers required by OpenAI Realtime
    openai_headers = {
        "Authorization": f"Bearer {settings.OPENAI_API_KEY}",
        "OpenAI-Beta": "realtime=v1"
    }

    try:
        async with websockets.connect(OPENAI_WS_URL, additional_headers=openai_headers, ping_interval=None) as openai_ws:
            logger.info("Connected to OpenAI Realtime API.")
            
            stream_sid = None

            async def receive_from_twilio():
                nonlocal stream_sid
                try:
                    async for message in twilio_ws.iter_text():
                        data = json.loads(message)
                        
                        # When Twilio officially starts the call
                        if data['event'] == 'start':
                            stream_sid = data['start']['streamSid']
                            logger.info(f"Call started. Stream SID: {stream_sid}")
                            
                            # EXACTLY AS OPENAI DOCS SPECIFY: Flat audio formatting
                            session_update = {
                                "type": "session.update",
                                "session": {
                                    "modalities": ["audio", "text"],
                                    "instructions": "You are a helpful phone assistant. Be brief.",
                                    "voice": "alloy",
                                    "input_audio_format": "g711_ulaw", 
                                    "output_audio_format": "g711_ulaw",
                                    "turn_detection": {
                                        "type": "server_vad"
                                    }
                                }
                            }
                            await openai_ws.send(json.dumps(session_update))
                            
                            # Immediately prompt the AI to speak
                            await openai_ws.send(json.dumps({
                                "type": "response.create",
                                "response": {
                                    "instructions": "Say: Hello! How can I help you today?"
                                }
                            }))
                            
                        # Standard audio routing
                        elif data['event'] == 'media':
                            await openai_ws.send(json.dumps({
                                "type": "input_audio_buffer.append",
                                "audio": data['media']['payload']
                            }))
                            
                        elif data['event'] == 'stop':
                            logger.info("Twilio call hung up.")
                            break
                except WebSocketDisconnect:
                    logger.info("Twilio WebSocket disconnected.")
                except Exception as e:
                    logger.error(f"Error reading from Twilio: {e}")

            async def send_to_twilio():
                try:
                    async for message in openai_ws:
                        response = json.loads(message)
                        event_type = response.get("type")
                        
                        # Route audio deltas to Twilio
                        if event_type == "response.audio.delta" and stream_sid:
                            await twilio_ws.send_text(json.dumps({
                                "event": "media",
                                "streamSid": stream_sid,
                                "media": {
                                    "payload": response["delta"]
                                }
                            }))
                            
                        # Explicitly catch and log OpenAI errors
                        elif event_type == "error":
                            logger.error(f"OPENAI ERROR: {response}")
                            
                except Exception as e:
                    logger.error(f"Error in OpenAI loop: {e}")

            # Run both cleanly
            await asyncio.gather(receive_from_twilio(), send_to_twilio())

    except Exception as e:
        logger.error(f"Failed to connect to OpenAI: {e}")