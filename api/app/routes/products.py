import asyncpg
from fastapi import APIRouter, Depends, HTTPException, Request, Response
from redis.asyncio import Redis

from app import cache, db
from app.config import settings
from app.metrics import CACHE_HITS, CACHE_MISSES
from app.models import Product, ProductCreate, ProductUpdate

router = APIRouter(prefix="/products", tags=["products"])


def get_db_pool(request: Request) -> asyncpg.Pool:
    return request.app.state.db_pool


def get_redis(request: Request) -> Redis:
    return request.app.state.redis


def _record_to_product(record: asyncpg.Record) -> Product:
    return Product(**dict(record))


@router.get("", response_model=list[Product])
async def list_products(
    limit: int = 100,
    offset: int = 0,
    pool: asyncpg.Pool = Depends(get_db_pool),
):
    rows = await db.list_products(pool, limit, offset)
    return [_record_to_product(r) for r in rows]


@router.get("/{product_id}", response_model=Product)
async def get_product(
    product_id: int,
    request: Request,
    pool: asyncpg.Pool = Depends(get_db_pool),
    redis: Redis = Depends(get_redis),
):
    cached = await cache.get_product(redis, product_id)
    if cached is not None:
        CACHE_HITS.inc()
        request.state.cache_hit = True
        return cached

    CACHE_MISSES.inc()
    request.state.cache_hit = False
    row = await db.get_product(pool, product_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Product not found")

    product = _record_to_product(row)
    await cache.set_product(redis, product, settings.cache_ttl_seconds)
    return product


@router.post("", response_model=Product, status_code=201)
async def create_product(
    body: ProductCreate,
    pool: asyncpg.Pool = Depends(get_db_pool),
):
    row = await db.insert_product(pool, body.name, body.price, body.stock)
    return _record_to_product(row)


@router.put("/{product_id}", response_model=Product)
async def update_product(
    product_id: int,
    body: ProductUpdate,
    pool: asyncpg.Pool = Depends(get_db_pool),
    redis: Redis = Depends(get_redis),
):
    row = await db.update_product(pool, product_id, body.name, body.price, body.stock)
    if row is None:
        raise HTTPException(status_code=404, detail="Product not found")

    await cache.invalidate_product(redis, product_id)
    return _record_to_product(row)


@router.delete("/{product_id}", status_code=204)
async def delete_product(
    product_id: int,
    pool: asyncpg.Pool = Depends(get_db_pool),
    redis: Redis = Depends(get_redis),
):
    deleted = await db.delete_product(pool, product_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Product not found")

    await cache.invalidate_product(redis, product_id)
    return Response(status_code=204)
