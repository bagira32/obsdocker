import json
from decimal import Decimal

from redis.asyncio import Redis

from app.models import Product


def _key(product_id: int) -> str:
    return f"product:{product_id}"


async def get_product(redis: Redis, product_id: int) -> Product | None:
    raw = await redis.get(_key(product_id))
    if raw is None:
        return None
    return Product(**json.loads(raw))


async def set_product(redis: Redis, product: Product, ttl_seconds: int) -> None:
    payload = product.model_dump()
    payload["price"] = str(payload["price"])
    await redis.set(_key(product.id), json.dumps(payload), ex=ttl_seconds)


async def invalidate_product(redis: Redis, product_id: int) -> None:
    await redis.delete(_key(product_id))
