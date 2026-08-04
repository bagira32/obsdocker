from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    database_url: str
    redis_url: str
    cache_ttl_seconds: int = 30
    log_level: str = "info"


settings = Settings()
