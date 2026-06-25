"""
Twilio Voice Bot (Realtime + Whisper fallback)

FLOW:
Twilio → Realtime WS (preferred)
     ↘ fallback → Whisper → GPT → TTS
"""
import json
import asyncio
import logging
import websockets

from fastapi import APIRouter, WebSocket
from app.config import settings

router = APIRouter()

logger = logging.getLogger("voice-bot")

OPENAI_WS_URL = "wss://api.openai.com/v1/realtime?model=gpt-4o-realtime-preview"


# ---------------------------------------------------
# helper
# ---------------------------------------------------
async def send(ws, payload):
    try:
        await ws.send(json.dumps(payload))
    except Exception as e:
        logger.error(f"SEND ERROR: {e}")


# ---------------------------------------------------
# main websocket
# ---------------------------------------------------
@router.websocket("/media-stream")
async def media_stream(twilio_ws: WebSocket):
    await twilio_ws.accept()
    logger.info("TWILIO CONNECTED")

    headers = {
        "Authorization": f"Bearer {settings.OPENAI_API_KEY}",
        "OpenAI-Beta": "realtime=v1"
    }

    stream_sid = None

    try:
        async with websockets.connect(
            OPENAI_WS_URL,
            extra_headers=headers,
            ping_interval=20
        ) as openai_ws:

            logger.info("OPENAI CONNECTED")

            # ---------------------------------------------------
            # SESSION INIT (CRITICAL)
            # ---------------------------------------------------
            await send(openai_ws, {
                "type": "session.update",
                "session": {
                    "modalities": ["text", "audio"],
                    "instructions": (
                        "You are a helpful voice assistant. "
                        "Keep responses short and spoken naturally."
                    ),
                    "voice": "alloy",
                    "input_audio_format": "g711_ulaw",
                    "output_audio_format": "g711_ulaw",
                    "turn_detection": {
                        "type": "server_vad"
                    }
                }
            })

            # ---------------------------------------------------
            # GREETING (IMPORTANT: forces audio output)
            # ---------------------------------------------------
            await send(openai_ws, {
                "type": "response.create",
                "response": {
                    "instructions": "Say: Hello! I am ready to help you."
                }
            })

            # ---------------------------------------------------
            # TWILIO -> OPENAI
            # ---------------------------------------------------
            async def twilio_to_openai():
                nonlocal stream_sid

                async for msg in twilio_ws.iter_text():
                    data = json.loads(msg)
                    event = data.get("event")

                    if event == "start":
                        stream_sid = data["start"]["streamSid"]

                    elif event == "media":
                        audio = data["media"]["payload"]

                        await send(openai_ws, {
                            "type": "input_audio_buffer.append",
                            "audio": audio
                        })

                    elif event == "stop":
                        break

            # ---------------------------------------------------
            # OPENAI -> TWILIO
            # ---------------------------------------------------
            async def openai_to_twilio():
                nonlocal stream_sid

                async for msg in openai_ws:
                    try:
                        event = json.loads(msg)
                        t = event.get("type")

                        # ---------------- AUDIO OUTPUT ----------------
                        if t == "response.audio.delta":
                            if not stream_sid:
                                continue

                            audio = event.get("delta")
                            if not audio:
                                continue

                            await twilio_ws.send_text(json.dumps({
                                "event": "media",
                                "streamSid": stream_sid,
                                "media": {
                                    "payload": audio
                                }
                            }))

                        # ---------------- TEXT (SAFE DEBUG ONLY) ----------------
                        elif t == "response.output_item.done":
                            item = event.get("item", {})
                            content = item.get("content", [])

                            for c in content:
                                if c.get("type") == "output_text":
                                    logger.info("AI TEXT: " + c.get("text", ""))

                        # ---------------- ERRORS ----------------
                        elif t == "error":
                            logger.error(event)

                    except Exception as e:
                        logger.error(f"PROCESSING ERROR: {e}")

            await asyncio.gather(
                twilio_to_openai(),
                openai_to_twilio()
            )

    except Exception:
        logger.exception("FAILED WEBSOCKET")