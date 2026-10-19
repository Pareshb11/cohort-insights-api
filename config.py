from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    mongo_uri: str = "mongodb://mongo:27017"
    mongo_db: str = "cohort_insights"
    redis_url: str = "redis://redis:6379/0"
    active_limit: int = 3
    cache_ttl_seconds: int = 86400
    worker_poll_seconds: float = 0.5
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

settings = Settings()
