"""
Twilio Voice Bot (Realtime + Whisper fallback)

FLOW:
Twilio → Realtime WS (preferred)
     ↘ fallback → Whisper → GPT → TTS
"""
import json
import asyncio
import base64
import logging
import websockets

from fastapi import APIRouter, WebSocket
from app.config import settings

router = APIRouter()
logger = logging.getLogger("voice-bot")

OPENAI_REALTIME_URL = "wss://api.openai.com/v1/realtime?model=gpt-4o-realtime-preview"

# ------------------------------------------------------------
# SAFE SEND
# ------------------------------------------------------------

async def safe_send(ws, payload):
    try:
        await ws.send(json.dumps(payload))
    except Exception as e:
        logger.error(f"SEND FAILED: {e}")

# ------------------------------------------------------------
# FALLBACK PIPELINE (Whisper + GPT + TTS placeholder)
# ------------------------------------------------------------

async def fallback_pipeline(audio_bytes: bytes):
    """
    Replace this with:
    - Whisper transcription
    - GPT response
    - TTS (gTTS / OpenAI TTS)
    """
    logger.info("USING FALLBACK PIPELINE")

    # dummy response
    return "Sorry, I couldn't connect to realtime. Please try again."

# ------------------------------------------------------------
# MAIN WEBSOCKET
# ------------------------------------------------------------

@router.websocket("/media-stream")
async def media_stream(ws: WebSocket):
    await ws.accept()
    logger.info("TWILIO CONNECTED")

    stream_sid = None

    headers = {
        "Authorization": f"Bearer {settings.OPENAI_API_KEY}",
        "OpenAI-Beta": "realtime=v1",
    }

    openai_ws = None

    # --------------------------------------------------------
    # CONNECT REALTIME
    # --------------------------------------------------------

    try:
        openai_ws = await websockets.connect(
            OPENAI_REALTIME_URL,
            extra_headers=headers,   # IMPORTANT FIX
            ping_interval=20,
        )

        logger.info("REALTIME CONNECTED")

        # ----------------------------------------------------
        # SESSION INIT
        # ----------------------------------------------------

        await safe_send(openai_ws, {
            "type": "session.update",
            "session": {
                "modalities": ["audio"],
                "input_audio_format": "g711_ulaw",
                "output_audio_format": "g711_ulaw",
                "voice": "alloy",
                "turn_detection": {"type": "server_vad"},
                "instructions": "You are a helpful voice assistant."
            }
        })

    except Exception as e:
        logger.error(f"REALTIME FAILED → FALLBACK ACTIVATED: {e}")
        openai_ws = None

    # --------------------------------------------------------
    # TWILIO → OPENAI
    # --------------------------------------------------------

    async def twilio_loop():
        nonlocal stream_sid

        async for msg in ws.iter_text():
            data = json.loads(msg)
            event = data.get("event")

            if event == "start":
                stream_sid = data["start"]["streamSid"]
                logger.info(f"STREAM STARTED {stream_sid}")

                if openai_ws:
                    await safe_send(openai_ws, {
                        "type": "response.create"
                    })

            elif event == "media":
                audio = data["media"]["payload"]

                if openai_ws:
                    await safe_send(openai_ws, {
                        "type": "input_audio_buffer.append",
                        "audio": audio
                    })

            elif event == "stop":
                logger.info("TWILIO STOP")
                break

    # --------------------------------------------------------
    # OPENAI → TWILIO
    # --------------------------------------------------------

    async def openai_loop():
        nonlocal stream_sid

        if not openai_ws:
            return

        async for msg in openai_ws:
            event = json.loads(msg)
            t = event.get("type")

            if t == "response.output_audio.delta":
                if not stream_sid:
                    continue

                audio = event["delta"]

                await ws.send_json({
                    "event": "media",
                    "streamSid": stream_sid,
                    "media": {"payload": audio}
                })

            elif t == "error":
                logger.error(f"OPENAI ERROR: {event}")

    # --------------------------------------------------------
    # RUN
    # --------------------------------------------------------

    try:
        await asyncio.gather(
            twilio_loop(),
            openai_loop()
        )

    except Exception:
        logger.exception("WEBHOOK CRASHED")

    finally:
        if openai_ws:
            await openai_ws.close()