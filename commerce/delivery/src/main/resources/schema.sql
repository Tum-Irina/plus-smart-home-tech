create TABLE IF NOT EXISTS deliveries (
    delivery_id UUID PRIMARY KEY,
    order_id UUID NOT NULL UNIQUE,
    from_address_country VARCHAR(100) NOT NULL,
    from_address_city VARCHAR(100) NOT NULL,
    from_address_street VARCHAR(100) NOT NULL,
    from_address_house VARCHAR(20) NOT NULL,
    from_address_flat VARCHAR(20),
    to_address_country VARCHAR(100) NOT NULL,
    to_address_city VARCHAR(100) NOT NULL,
    to_address_street VARCHAR(100) NOT NULL,
    to_address_house VARCHAR(20) NOT NULL,
    to_address_flat VARCHAR(20),
    delivery_state VARCHAR(20) NOT NULL,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
);