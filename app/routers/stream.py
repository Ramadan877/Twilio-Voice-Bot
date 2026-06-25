"""
Twilio <-> OpenAI Realtime bridge

Listens on /media-stream for Twilio Media Streams,
opens a second websocket to OpenAI Realtime,
and forwards audio in both directions.
"""

import json
import asyncio
import logging

from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from app.config import settings
import websockets

# -----------------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)

logger = logging.getLogger(__name__)

# -----------------------------------------------------------------------------
# FastAPI Router
# -----------------------------------------------------------------------------

router = APIRouter()

# -----------------------------------------------------------------------------
# OpenAI Realtime
# -----------------------------------------------------------------------------

OPENAI_WS_URL = (
    "wss://api.openai.com/v1/realtime"
    "?model=gpt-realtime"
)

# -----------------------------------------------------------------------------
# Helper
# -----------------------------------------------------------------------------

async def send_to_openai(openai_ws, payload: dict):
    logger.info(
        "OPENAI >>> %s",
        json.dumps(payload, indent=2)
    )

    await openai_ws.send(json.dumps(payload))


# -----------------------------------------------------------------------------
# Main Websocket Handler
# -----------------------------------------------------------------------------

@router.websocket("/media-stream")
async def handle_media_stream(twilio_ws: WebSocket):
    """
    Handles live audio streaming between Twilio and OpenAI.
    """

    await twilio_ws.accept()

    logger.info("========================================")
    logger.info("TWILIO PHONE STREAM CONNECTED")
    logger.info("========================================")

    openai_headers = {
        "Authorization": f"Bearer {settings.OPENAI_API_KEY}"
    }

    try:
        async with websockets.connect(
            OPENAI_WS_URL,
            additional_headers=openai_headers,
            ping_interval=None
        ) as openai_ws:

            logger.info("========================================")
            logger.info("CONNECTED TO OPENAI REALTIME")
            logger.info("URL: %s", OPENAI_WS_URL)
            logger.info("========================================")

            stream_sid = None
            audio_chunks_from_twilio = 0
            audio_chunks_from_openai = 0

            # -----------------------------------------------------------------
            # Twilio -> OpenAI
            # -----------------------------------------------------------------

            async def receive_from_twilio():
                nonlocal stream_sid
                nonlocal audio_chunks_from_twilio

                try:
                    async for message in twilio_ws.iter_text():

                        data = json.loads(message)
                        event_type = data.get("event")

                        logger.info(
                            "TWILIO EVENT <<< %s",
                            event_type
                        )

                        # -----------------------------------------------------
                        # START
                        # -----------------------------------------------------

                        if event_type == "start":

                            stream_sid = data["start"]["streamSid"]

                            logger.info(
                                "CALL STARTED | Stream SID=%s",
                                stream_sid
                            )

                            session_update = {
                                "type": "session.update",
                                "session": {
                                    "type": "realtime",
                                    "instructions": (
                                        "You are a helpful phone assistant. "
                                        "Be brief and conversational."
                                    ),
                                    "voice": "alloy",

                                    "audio": {
                                        "input": {
                                            "format": {
                                                "type": "audio/pcmu"
                                            }
                                        },
                                        "output": {
                                            "format": {
                                                "type": "audio/pcmu"
                                            }
                                        }
                                    }
                                }
                            }

                            await send_to_openai(
                                openai_ws,
                                session_update
                            )

                            initial_response = {
                                "type": "response.create",
                                "response": {
                                    "instructions": (
                                        "Say: Hello! How can I help you today?"
                                    )
                                }
                            }

                            await send_to_openai(
                                openai_ws,
                                initial_response
                            )

                        # -----------------------------------------------------
                        # AUDIO
                        # -----------------------------------------------------

                        elif event_type == "media":

                            audio_chunks_from_twilio += 1

                            if audio_chunks_from_twilio % 100 == 0:
                                logger.info(
                                    "TWILIO AUDIO CHUNKS RECEIVED=%s",
                                    audio_chunks_from_twilio
                                )

                            await openai_ws.send(
                                json.dumps(
                                    {
                                        "type": "input_audio_buffer.append",
                                        "audio": data["media"]["payload"]
                                    }
                                )
                            )

                        # -----------------------------------------------------
                        # STOP
                        # -----------------------------------------------------

                        elif event_type == "stop":

                            logger.info(
                                "TWILIO CALL ENDED"
                            )

                            break

                except WebSocketDisconnect:

                    logger.info(
                        "TWILIO WEBSOCKET DISCONNECTED"
                    )

                except Exception:

                    logger.exception(
                        "ERROR READING FROM TWILIO"
                    )

            # -----------------------------------------------------------------
            # OpenAI -> Twilio
            # -----------------------------------------------------------------

            async def send_to_twilio():

                nonlocal audio_chunks_from_openai

                try:
                    async for message in openai_ws:

                        logger.info(
                            "OPENAI RAW <<< %s",
                            message[:1000]
                        )

                        response = json.loads(message)
                        event_type = response.get("type")

                        logger.info(
                            "OPENAI EVENT <<< %s",
                            event_type
                        )

                        # -----------------------------------------------------
                        # AUDIO DELTAS
                        # -----------------------------------------------------

                        if (
                            event_type == "response.output_audio.delta"
                            and stream_sid
                        ):

                            audio_chunks_from_openai += 1

                            logger.info(
                                "FORWARDING AUDIO TO TWILIO"
                            )

                            if audio_chunks_from_openai % 100 == 0:
                                logger.info(
                                    "OPENAI AUDIO CHUNKS SENT=%s",
                                    audio_chunks_from_openai
                                )

                            await twilio_ws.send_text(
                                json.dumps(
                                    {
                                        "event": "media",
                                        "streamSid": stream_sid,
                                        "media": {
                                            "payload": response["delta"]
                                        }
                                    }
                                )
                            )

                            logger.info(
                                "AUDIO SENT TO TWILIO"
                            )

                        # -----------------------------------------------------
                        # RESPONSE COMPLETE
                        # -----------------------------------------------------

                        elif event_type == "response.done":

                            logger.info(
                                "OPENAI RESPONSE COMPLETE"
                            )

                        # -----------------------------------------------------
                        # ERRORS
                        # -----------------------------------------------------

                        elif event_type == "session.created":

                            logger.info(
                                "SESSION CREATED:\n%s",
                                json.dumps(response, indent=2)
                            )

                        elif event_type == "session.updated":

                            logger.info(
                                "SESSION UPDATED:\n%s",
                                json.dumps(response, indent=2)
                            )

                        elif event_type == "error":

                            logger.error(
                                "OPENAI ERROR:\n%s",
                                json.dumps(
                                    response,
                                    indent=2
                                )
                            )
                            
                except websockets.ConnectionClosed as e:

                    logger.error(
                        "OPENAI WS CLOSED | code=%s reason=%s",
                        e.code,
                        e.reason
                    )

                except Exception:

                    logger.exception(
                        "UNEXPECTED ERROR IN OPENAI LOOP"
                    )

            # -----------------------------------------------------------------
            # Run both directions
            # -----------------------------------------------------------------

            await asyncio.gather(
                receive_from_twilio(),
                send_to_twilio()
            )

    except Exception:

        logger.exception(
            "FAILED TO CONNECT TO OPENAI"
        )