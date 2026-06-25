"""
Twilio <-> OpenAI Realtime bridge

Fixes:
- correct session format
- proper audio buffer handling
- proper streaming stability
"""

import json
import asyncio
import logging

from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from app.config import settings
import websockets

# ------------------------------------------------------------------
# Logging
# ------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)

logger = logging.getLogger(__name__)

router = APIRouter()

# ------------------------------------------------------------------
# OpenAI Realtime endpoint
# ------------------------------------------------------------------

OPENAI_WS_URL = "wss://api.openai.com/v1/realtime?model=gpt-4o-realtime-preview"
# ------------------------------------------------------------------
# helper
# ------------------------------------------------------------------

async def send_to_openai(ws, payload: dict):
    await ws.send(json.dumps(payload))


# ------------------------------------------------------------------
# main
# ------------------------------------------------------------------

@router.websocket("/media-stream")
async def media_stream(twilio_ws: WebSocket):

    await twilio_ws.accept()

    logger.info("TWILIO CONNECTED")

    headers = {
        "Authorization": f"Bearer {settings.OPENAI_API_KEY}",
        "OpenAI-Beta": "realtime=v1"
    }

    try:
        async with websockets.connect(
            OPENAI_WS_URL,
            additional_headers=headers,
            ping_interval=20
        ) as openai_ws:

            logger.info("OPENAI CONNECTED")

            stream_sid = None

            # ----------------------------------------------------------
            # INIT SESSION (CRITICAL FIX)
            # ----------------------------------------------------------

            session_update = {
                "type": "session.update",
                "session": {
                    "modalities": ["audio"],
                    "instructions": "You are a helpful phone assistant. Be brief.",
                    "input_audio_format": "g711_ulaw",
                    "output_audio_format": "g711_ulaw",
                    "voice": "alloy",
                    "turn_detection": {
                        "type": "server_vad"
                    }
                }
            }

            await send_to_openai(openai_ws, session_update)

            # ----------------------------------------------------------
            # TWILIO -> OPENAI
            # ----------------------------------------------------------

            async def twilio_to_openai():

                nonlocal stream_sid

                async for msg in twilio_ws.iter_text():

                    data = json.loads(msg)
                    event = data.get("event")

                    if event == "start":
                        stream_sid = data["start"]["streamSid"]

                        await send_to_openai(openai_ws, {
                            "type": "response.create",
                            "response": {
                                "instructions": "Say: Hello! How can I help you today?"
                            }
                        })

                    elif event == "media":
                        # IMPORTANT: raw base64 ulaw from Twilio
                        audio_payload = data["media"]["payload"]

                        await openai_ws.send(json.dumps({
                            "type": "input_audio_buffer.append",
                            "audio": audio_payload
                        }))

                    elif event == "stop":
                        break

            # ----------------------------------------------------------
            # OPENAI -> TWILIO
            # ----------------------------------------------------------

            async def openai_to_twilio():

                async for msg in openai_ws:

                    event = json.loads(msg)
                    t = event.get("type")

                    if t == "response.output_audio.delta":

                        if not stream_sid:
                            continue

                        audio = event["delta"]

                        # Twilio expects base64 G.711 μ-law
                        await twilio_ws.send_text(json.dumps({
                            "event": "media",
                            "streamSid": stream_sid,
                            "media": {
                                "payload": audio
                            }
                        }))

                    elif t == "error":
                        logger.error(event)

            await asyncio.gather(
                twilio_to_openai(),
                openai_to_twilio()
            )

    except Exception:
        logger.exception("FAILED WEBSOCKET")
