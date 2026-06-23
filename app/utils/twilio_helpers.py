from twilio.twiml.voice_response import VoiceResponse

def generate_reject_twiml() -> str:
    """
    Generates TwiML instructions to reject a call (useful for security/whitelisting numbers).
    """
    response = VoiceResponse()
    response.reject(reason="busy")
    return str(response)