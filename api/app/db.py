import asyncpg


async def create_pool(dsn: str) -> asyncpg.Pool:
    return await asyncpg.create_pool(dsn, min_size=1, max_size=10)


async def list_products(pool: asyncpg.Pool, limit: int, offset: int) -> list[asyncpg.Record]:
    return await pool.fetch(
        "SELECT id, name, price, stock FROM products ORDER BY id LIMIT $1 OFFSET $2",
        limit,
        offset,
    )


async def get_product(pool: asyncpg.Pool, product_id: int) -> asyncpg.Record | None:
    return await pool.fetchrow(
        "SELECT id, name, price, stock FROM products WHERE id = $1", product_id
    )


async def insert_product(pool: asyncpg.Pool, name: str, price, stock: int) -> asyncpg.Record:
    return await pool.fetchrow(
        """
        INSERT INTO products (name, price, stock)
        VALUES ($1, $2, $3)
        RETURNING id, name, price, stock
        """,
        name,
        price,
        stock,
    )


async def update_product(
    pool: asyncpg.Pool, product_id: int, name: str, price, stock: int
) -> asyncpg.Record | None:
    return await pool.fetchrow(
        """
        UPDATE products
        SET name = $2, price = $3, stock = $4, updated_at = now()
        WHERE id = $1
        RETURNING id, name, price, stock
        """,
        product_id,
        name,
        price,
        stock,
    )


async def delete_product(pool: asyncpg.Pool, product_id: int) -> bool:
    result = await pool.execute("DELETE FROM products WHERE id = $1", product_id)
    return result == "DELETE 1"
