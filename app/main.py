import uvicorn
from fastapi import FastAPI
from app.routers import twilio_voice, stream

app = FastAPI(title="Twilio OpenAI Realtime Voice Bot")

app.include_router(twilio_voice.router)
app.include_router(stream.router)

@app.get("/health")
def health_check():
    return {"status": "healthy"}

if __name__ == "__main__":
    # Render sets a PORT environment variable dynamically
    import os
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port, log_level="info")


