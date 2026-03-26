-- Таблица склада
create TABLE IF NOT EXISTS warehouse_products (
    id UUID PRIMARY KEY,
    product_id UUID NOT NULL UNIQUE,
    quantity BIGINT NOT NULL DEFAULT 0,
    fragile BOOLEAN,
    width DOUBLE PRECISION,
    height DOUBLE PRECISION,
    depth DOUBLE PRECISION,
    weight DOUBLE PRECISION
);