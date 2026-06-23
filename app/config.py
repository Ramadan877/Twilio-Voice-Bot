from pydantic_settings import BaseSettings

# Here we will be centralizing the environment variables so the app fails early if something is missing 
class Settings(BaseSettings):
    OPENAI_API_KEY: str
    TWILIO_ACCOUNT_SID: str
    TWILIO_AUTH_TOKEN: str
    RENDER_EXTERNAL_URL: str 

    class Config:
        env_file =".env"

settings = Settings()