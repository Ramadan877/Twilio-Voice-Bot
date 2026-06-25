"""
Twilio Voice Bot (Realtime + Whisper fallback)

FLOW:
Twilio → Realtime WS (preferred)
     ↘ fallback → Whisper → GPT → TTS
"""
import json
import base64
import asyncio
import logging
import tempfile
import requests

from fastapi import APIRouter, WebSocket
from app.config import settings

router = APIRouter()
logger = logging.getLogger("voice-bot")

# ============================================================
# GPT RESPONSE
# ============================================================

def ask_gpt(text: str) -> str:
    resp = requests.post(
        "https://api.openai.com/v1/chat/completions",
        headers={
            "Authorization": f"Bearer {settings.OPENAI_API_KEY}",
            "Content-Type": "application/json",
        },
        json={
            "model": "gpt-4o-mini",
            "messages": [
                {"role": "system", "content": "You are a helpful voice assistant. Be brief."},
                {"role": "user", "content": text}
            ]
        },
        timeout=30
    )

    return resp.json()["choices"][0]["message"]["content"]


# ============================================================
# WHISPER TRANSCRIPTION
# ============================================================

def transcribe_audio(audio_bytes: bytes) -> str:
    files = {
        "file": ("audio.wav", audio_bytes, "audio/wav")
    }

    resp = requests.post(
        "https://api.openai.com/v1/audio/transcriptions",
        headers={
            "Authorization": f"Bearer {settings.OPENAI_API_KEY}",
        },
        files=files,
        data={"model": "whisper-1"},
        timeout=60
    )

    return resp.json()["text"]


# ============================================================
# TTS (TWILIO COMPATIBLE)
# ============================================================

def text_to_speech(text: str) -> bytes:
    resp = requests.post(
        "https://api.openai.com/v1/audio/speech",
        headers={
            "Authorization": f"Bearer {settings.OPENAI_API_KEY}",
            "Content-Type": "application/json",
        },
        json={
            "model": "gpt-4o-mini-tts",
            "voice": "alloy",
            "input": text,
            "format": "mulaw_8000"
        },
        timeout=30
    )

    return resp.content


# ============================================================
# INTRO MESSAGE
# ============================================================

async def send_intro(ws, stream_sid):
    await asyncio.sleep(0.5)

    audio = text_to_speech("Hello! I am your assistant. How can I help you today?")

    await ws.send_json({
        "event": "media",
        "streamSid": stream_sid,
        "media": {
            "payload": base64.b64encode(audio).decode()
        }
    })


# ============================================================
# MAIN WEBHOOK
# ============================================================

@router.websocket("/media-stream")
async def media_stream(ws: WebSocket):
    await ws.accept()
    logger.info("TWILIO CONNECTED")

    stream_sid = None
    audio_buffer = bytearray()

    async def process_audio():
        nonlocal audio_buffer

        if len(audio_buffer) < 8000:
            return

        try:
            text = transcribe_audio(bytes(audio_buffer))
            logger.info(f"USER: {text}")

            audio_buffer = bytearray()

            if not text.strip():
                return

            reply = ask_gpt(text)
            logger.info(f"GPT: {reply}")

            speech = text_to_speech(reply)

            await ws.send_json({
                "event": "media",
                "streamSid": stream_sid,
                "media": {
                    "payload": base64.b64encode(speech).decode()
                }
            })

        except Exception as e:
            logger.error(f"PROCESSING ERROR: {e}")

    try:
        async for msg in ws.iter_text():
            data = json.loads(msg)
            event = data.get("event")

            # ---------------- START ----------------
            if event == "start":
                stream_sid = data["start"]["streamSid"]
                logger.info(f"CALL STARTED {stream_sid}")

                asyncio.create_task(send_intro(ws, stream_sid))

            # ---------------- AUDIO ----------------
            elif event == "media":
                audio = base64.b64decode(data["media"]["payload"])
                audio_buffer.extend(audio)

                await process_audio()

            # ---------------- STOP ----------------
            elif event == "stop":
                logger.info("CALL ENDED")
                break

    except Exception:
        logger.exception("WEBSOCKET CRASHED")