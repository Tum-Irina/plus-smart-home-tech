-- Таблица заказов
create TABLE IF NOT EXISTS orders (
    order_id UUID PRIMARY KEY,
    shopping_cart_id UUID NOT NULL,
    delivery_id UUID,
    payment_id UUID,
    state VARCHAR(20) NOT NULL,
    delivery_volume DOUBLE PRECISION,
    delivery_weight DOUBLE PRECISION,
    fragile BOOLEAN,
    total_price DECIMAL(10,2) NOT NULL,
    products_price DECIMAL(10,2) NOT NULL,
    delivery_price DECIMAL(10,2),
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
);

-- Таблица позиций заказа
create TABLE IF NOT EXISTS order_items (
    order_item_id UUID PRIMARY KEY,
    order_id UUID NOT NULL REFERENCES orders(order_id) ON delete CASCADE,
    product_id UUID NOT NULL,
    quantity BIGINT NOT NULL
);