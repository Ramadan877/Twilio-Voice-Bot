""" When someone calls the twilio number, Twilio hits this HTTP endpoint, so our job here is to return TwiML instructions telling Twilio to open a WebSocket connection back to the Deployment server"""

from fastapi import APIRouter, Response
from twilio.twiml.voice_response import VoiceResponse, Connect 
from app.config import settings

router = APIRouter()

@router.post("/twilio/webhook")
async def handle_incoming_call():
    response = VoiceResponse()
    response.say("Connecting you to your AI assistant. Please wait.")

    # Instruct Twilio to open a bidirectional media stream WebSocket
    connect = Connect()

    # changing the render url from https:// to wss://
    ws_url = f"{settings.RENDER_EXTERNAL_URL.replace('https://', 'wss://')}/media-stream"
    connect.stream(url=ws_url)
    response.append(connect)

    return Response(content=str(response), media_type="application/xml")
