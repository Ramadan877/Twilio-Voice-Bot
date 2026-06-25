"""
Twilio Voice Bot (Realtime + Whisper fallback)

FLOW:
Twilio → Realtime WS (preferred)
     ↘ fallback → Whisper → GPT → TTS
"""

import json
import asyncio
import logging
import base64
import httpx
import websockets

from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from app.config import settings

router = APIRouter()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("voice-bot")

OPENAI_WS_URL = "wss://api.openai.com/v1/realtime?model=gpt-realtime"
OPENAI_HTTP = "https://api.openai.com/v1"

# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------

async def gpt_text(client, text: str):
    r = await client.post(
        f"{OPENAI_HTTP}/responses",
        headers={
            "Authorization": f"Bearer {settings.OPENAI_API_KEY}",
        },
        json={
            "model": "gpt-4o-mini",
            "input": text
        }
    )
    return r.json()["output"][0]["content"][0]["text"]

async def tts(client, text: str):
    r = await client.post(
        f"{OPENAI_HTTP}/audio/speech",
        headers={
            "Authorization": f"Bearer {settings.OPENAI_API_KEY}",
        },
        json={
            "model": "gpt-4o-mini-tts",
            "voice": "alloy",
            "input": text,
            "format": "pcm"
        }
    )
    return r.content  # raw audio

async def whisper(client, audio_bytes: bytes):
    files = {
        "file": ("audio.wav", audio_bytes, "audio/wav"),
        "model": (None, "whisper-1")
    }

    r = await client.post(
        f"{OPENAI_HTTP}/audio/transcriptions",
        headers={
            "Authorization": f"Bearer {settings.OPENAI_API_KEY}",
        },
        files=files
    )

    return r.json()["text"]

# ---------------------------------------------------------------------
# MAIN ROUTE
# ---------------------------------------------------------------------

@router.websocket("/media-stream")
async def media_stream(twilio_ws: WebSocket):

    await twilio_ws.accept()
    logger.info("TWILIO CONNECTED")

    stream_sid = None
    audio_buffer = bytearray()

    async with httpx.AsyncClient(timeout=None) as http:

        # =========================================================
        # TRY REALTIME FIRST
        # =========================================================

        try:
            headers = {
                "Authorization": f"Bearer {settings.OPENAI_API_KEY}",
                "OpenAI-Beta": "realtime=v1"
            }

            async with websockets.connect(
                OPENAI_WS_URL,
                extra_headers=headers,
                ping_interval=20
            ) as openai_ws:

                logger.info("OPENAI REALTIME CONNECTED")

                session = {
                    "type": "session.update",
                    "session": {
                        "modalities": ["audio"],
                        "instructions": "You are a helpful phone assistant.",
                        "input_audio_format": "g711_ulaw",
                        "output_audio_format": "g711_ulaw",
                        "turn_detection": {"type": "server_vad"}
                    }
                }

                await openai_ws.send(json.dumps(session))

                async def twilio_to_openai():
                    nonlocal stream_sid

                    async for msg in twilio_ws.iter_text():
                        data = json.loads(msg)
                        event = data.get("event")

                        if event == "start":
                            stream_sid = data["start"]["streamSid"]

                            await openai_ws.send(json.dumps({
                                "type": "response.create",
                                "response": {
                                    "instructions": "Say hello briefly"
                                }
                            }))

                        elif event == "media":
                            await openai_ws.send(json.dumps({
                                "type": "input_audio_buffer.append",
                                "audio": data["media"]["payload"]
                            }))

                        elif event == "stop":
                            return

                async def openai_to_twilio():
                    async for msg in openai_ws:
                        event = json.loads(msg)

                        if event.get("type") == "response.output_audio.delta":
                            if not stream_sid:
                                continue

                            await twilio_ws.send_text(json.dumps({
                                "event": "media",
                                "streamSid": stream_sid,
                                "media": {
                                    "payload": event["delta"]
                                }
                            }))

                await asyncio.gather(
                    twilio_to_openai(),
                    openai_to_twilio()
                )

                return  # ✅ SUCCESS → DO NOT FALLBACK

        except Exception as e:
            logger.error(f"REALTIME FAILED → FALLBACK ACTIVATED: {e}")

        # =========================================================
        # FALLBACK PIPELINE
        # =========================================================

        logger.info("USING WHISPER + GPT + TTS PIPELINE")

        async def fallback():
            nonlocal stream_sid

            async for msg in twilio_ws.iter_text():
                data = json.loads(msg)
                event = data.get("event")

                if event == "start":
                    stream_sid = data["start"]["streamSid"]

                elif event == "media":

                    # decode audio chunk
                    audio = base64.b64decode(data["media"]["payload"])
                    audio_buffer.extend(audio)

                elif event == "stop":
                    break

            # 1. Whisper
            user_text = await whisper(http, bytes(audio_buffer))
            logger.info(f"USER: {user_text}")

            # 2. GPT
            reply = await gpt_text(http, user_text)
            logger.info(f"GPT: {reply}")

            # 3. TTS
            audio = await tts(http, reply)

            # 4. send to Twilio
            await twilio_ws.send_text(json.dumps({
                "event": "media",
                "streamSid": stream_sid,
                "media": {
                    "payload": base64.b64encode(audio).decode()
                }
            }))

        await fallback()