create TABLE IF NOT EXISTS payments (
    payment_id UUID PRIMARY KEY,
    order_id UUID NOT NULL UNIQUE,
    total_payment DECIMAL(10,2) NOT NULL,
    delivery_total DECIMAL(10,2) NOT NULL,
    fee_total DECIMAL(10,2) NOT NULL,
    payment_state VARCHAR(20) NOT NULL,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
);