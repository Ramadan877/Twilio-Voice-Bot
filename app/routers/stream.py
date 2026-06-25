"""Here it listens on /media-stream for Twilio's connection, opens a second connection to OpenAI, and passes the binary audio buffers back and forth concurrently"""
import json
import asyncio
import logging
from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from app.config import settings
import websockets

# Instant logging to bypass buffering
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

router = APIRouter()

OPENAI_WS_URL = "wss://api.openai.com/v1/realtime?model=gpt-4o-realtime-preview-2024-12-17"

@router.websocket("/media-stream")
async def handle_media_stream(twilio_ws: WebSocket):
    """
    Handles the live audio stream between Twilio and OpenAI.
    """
    await twilio_ws.accept()
    logger.info("Twilio phone stream connected.")

    openai_headers = {
        "Authorization": f"Bearer {settings.OPENAI_API_KEY}"
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
                        
                        if data['event'] == 'start':
                            stream_sid = data['start']['streamSid']
                            logger.info(f"Call started. Stream SID: {stream_sid}")
                            
                        elif data['event'] == 'media':
                            audio_event = {
                                "type": "input_audio_buffer.append",
                                "audio": data['media']['payload']
                            }
                            await openai_ws.send(json.dumps(audio_event))
                                
                        elif data['event'] == 'stop':
                            logger.info("Twilio call hung up.")
                            break
                except WebSocketDisconnect:
                    logger.info("Twilio WebSocket disconnected.")
                except Exception as e:
                    logger.error(f"Error reading from Twilio: {e}")

            async def send_to_twilio():
                nonlocal stream_sid
                try:
                    # FAIL-SAFE: Prevent an infinite loop if Twilio lags
                    wait_time = 0
                    while stream_sid is None:
                        await asyncio.sleep(0.1)
                        wait_time += 0.1
                        if wait_time > 10:
                            logger.error("Timeout waiting for Twilio stream SID. Cancelling.")
                            return

                    # FIX: The Official General Availability Session Shape!
                    session_update = {
                        "type": "session.update",
                        "session": {
                            "type": "realtime",
                            "voice": "alloy",
                            "instructions": "You are a helpful phone assistant. Be highly concise.",
                            "audio": {
                                "input": {
                                    "format": { "type": "audio/pcmu" }
                                },
                                "output": {
                                    "format": { "type": "audio/pcmu" }
                                }
                            },
                            "turn_detection": {
                                "type": "server_vad"
                            }
                        }
                    }
                    await openai_ws.send(json.dumps(session_update))

                    # Process incoming messages from OpenAI
                    async for message in openai_ws:
                        response = json.loads(message)
                        event_type = response.get("type")
                        
                        if event_type == "error":
                            logger.error(f"OPENAI API FATAL ERROR: {response}")
                            
                        elif event_type == "session.updated":
                            logger.info("Session configured. Triggering AI greeting.")
                            initial_greeting = {
                                "type": "response.create",
                                "response": {
                                    "instructions": "Say: Hello! I am connected. How can I help you today?"
                                }
                            }
                            await openai_ws.send(json.dumps(initial_greeting))

                        # FIX: Catching the GA output audio delta event
                        elif event_type == "response.output_audio.delta" and stream_sid:
                            twilio_message = {
                                "event": "media",
                                "streamSid": stream_sid,
                                "media": {
                                    "payload": response["delta"]
                                }
                            }
                            await twilio_ws.send_text(json.dumps(twilio_message))
                            
                        elif event_type not in ["response.output_audio.delta", "input_audio_buffer.append"]:
                            logger.info(f"OpenAI Event: {event_type}")

                except Exception as e:
                    logger.error(f"Error in OpenAI loop: {e}")

            await asyncio.gather(receive_from_twilio(), send_to_twilio())

    except Exception as e:
        logger.error(f"Failed to connect to OpenAI: {e}")
    finally:
        # FIX: Gracefully close the Twilio WebSocket to prevent the 31921 error
        try:
            await twilio_ws.close()
        except Exception:
            pass