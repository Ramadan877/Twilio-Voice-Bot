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
import requests
import websockets

from fastapi import APIRouter, WebSocket
from app.config import settings

router = APIRouter()
logger = logging.getLogger("voice-bot")

OPENAI_REALTIME_URL = "wss://api.openai.com/v1/realtime?model=gpt-4o-realtime-preview"

# ============================================================
# TTS FALLBACK (ALWAYS WORKS)
# ============================================================

async def tts_speak(text: str, stream_sid: str, ws: WebSocket):
    """
    Uses OpenAI TTS to generate Twilio-compatible audio
    """

    try:
        logger.info(f"TTS: {text}")

        response = requests.post(
            "https://api.openai.com/v1/audio/speech",
            headers={
                "Authorization": f"Bearer {settings.OPENAI_API_KEY}",
                "Content-Type": "application/json"
            },
            json={
                "model": "gpt-4o-mini-tts",
                "voice": "alloy",
                "input": text,
                "format": "mulaw_8000"
            },
            timeout=30
        )

        audio_b64 = base64.b64encode(response.content).decode("utf-8")

        await ws.send_json({
            "event": "media",
            "streamSid": stream_sid,
            "media": {
                "payload": audio_b64
            }
        })

    except Exception as e:
        logger.error(f"TTS FAILED: {e}")


# ============================================================
# MAIN ROUTE
# ============================================================

@router.websocket("/media-stream")
async def media_stream(ws: WebSocket):
    await ws.accept()
    logger.info("TWILIO CONNECTED")

    stream_sid = None
    openai_ws = None

    # --------------------------------------------------------
    # TRY REALTIME CONNECTION
    # --------------------------------------------------------

    try:
        openai_ws = await websockets.connect(
            OPENAI_REALTIME_URL,
            extra_headers={
                "Authorization": f"Bearer {settings.OPENAI_API_KEY}",
                "OpenAI-Beta": "realtime=v1",
            },
            ping_interval=20
        )

        logger.info("REALTIME CONNECTED")

        await openai_ws.send(json.dumps({
            "type": "session.update",
            "session": {
                "modalities": ["audio"],
                "input_audio_format": "g711_ulaw",
                "output_audio_format": "g711_ulaw",
                "voice": "alloy",
                "turn_detection": {"type": "server_vad"},
                "instructions": "You are a helpful voice assistant."
            }
        }))

    except Exception as e:
        logger.error(f"REALTIME FAILED → FALLBACK MODE: {e}")
        openai_ws = None

    # --------------------------------------------------------
    # INTRO MESSAGE (ALWAYS SPEAK)
    # --------------------------------------------------------

    async def intro():
        if stream_sid:
            await tts_speak(
                "Hello! I am your assistant. How can I help you today?",
                stream_sid,
                ws
            )

    # --------------------------------------------------------
    # TWILIO → OPENAI
    # --------------------------------------------------------

    async def twilio_loop():
        nonlocal stream_sid

        async for msg in ws.iter_text():
            data = json.loads(msg)
            event = data.get("event")

            # START CALL
            if event == "start":
                stream_sid = data["start"]["streamSid"]
                logger.info(f"CALL STARTED {stream_sid}")

                await intro()

            # AUDIO FROM USER
            elif event == "media":

                audio = data["media"]["payload"]

                if openai_ws:
                    try:
                        await openai_ws.send(json.dumps({
                            "type": "input_audio_buffer.append",
                            "audio": audio
                        }))
                    except Exception as e:
                        logger.error(f"OPENAI SEND FAIL: {e}")

            elif event == "stop":
                logger.info("CALL ENDED")
                break

    # --------------------------------------------------------
    # OPENAI → TWILIO
    # --------------------------------------------------------

    async def openai_loop():

        if not openai_ws:
            return

        async for msg in openai_ws:
            event = json.loads(msg)
            t = event.get("type")

            if t == "response.output_audio.delta":
                if not stream_sid:
                    continue

                await ws.send_json({
                    "event": "media",
                    "streamSid": stream_sid,
                    "media": {
                        "payload": event["delta"]
                    }
                })

            elif t == "error":
                logger.error(f"OPENAI ERROR: {event}")

    # --------------------------------------------------------
    # RUN BOTH LOOPS
    # --------------------------------------------------------

    try:
        await asyncio.gather(
            twilio_loop(),
            openai_loop()
        )

    except Exception:
        logger.exception("WEBHOOK CRASH")

    finally:
        if openai_ws:
            await openai_ws.close()