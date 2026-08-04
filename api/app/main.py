from contextlib import asynccontextmanager

from fastapi import FastAPI
from prometheus_fastapi_instrumentator import Instrumentator
from redis.asyncio import from_url as redis_from_url

from app import db
from app.config import settings
from app.logging_conf import RequestLoggingMiddleware, configure_logging
from app.routes.products import router as products_router

configure_logging(settings.log_level)


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.db_pool = await db.create_pool(settings.database_url)
    app.state.redis = redis_from_url(settings.redis_url)
    yield
    await app.state.db_pool.close()
    await app.state.redis.aclose()


app = FastAPI(title="Products API", lifespan=lifespan)
app.add_middleware(RequestLoggingMiddleware)
app.include_router(products_router)

Instrumentator().instrument(app).expose(app, endpoint="/metrics", include_in_schema=False)


@app.get("/health", include_in_schema=False)
async def health():
    return {"status": "ok"}
