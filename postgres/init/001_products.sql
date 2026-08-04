CREATE TABLE IF NOT EXISTS products (
    id         SERIAL PRIMARY KEY,
    name       TEXT NOT NULL,
    price      NUMERIC(10,2) NOT NULL,
    stock      INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO products (name, price, stock) VALUES
    ('Widget',      9.99,  100),
    ('Gadget',     19.99,   50),
    ('Gizmo',       4.50,  200),
    ('Doohickey',  14.25,   75),
    ('Thingamajig', 29.00,  30)
ON CONFLICT DO NOTHING;
