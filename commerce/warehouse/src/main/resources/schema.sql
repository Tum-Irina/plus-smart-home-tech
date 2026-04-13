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

-- Таблица бронирования заказов
create TABLE IF NOT EXISTS order_bookings (
    booking_id UUID PRIMARY KEY,
    order_id UUID NOT NULL UNIQUE,
    delivery_id UUID,
    status VARCHAR(20) NOT NULL,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
);