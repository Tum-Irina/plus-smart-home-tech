-- Таблица корзин
create TABLE IF NOT EXISTS carts (
    shopping_cart_id UUID PRIMARY KEY,
    username VARCHAR(255) NOT NULL UNIQUE,
    active BOOLEAN NOT NULL DEFAULT TRUE
);

-- Таблица позиций корзины
create TABLE IF NOT EXISTS cart_items (
    cart_item_id UUID PRIMARY KEY,
    cart_id UUID NOT NULL REFERENCES carts(shopping_cart_id) ON delete CASCADE,
    product_id UUID NOT NULL,
    quantity BIGINT NOT NULL
);