package ru.practicum.warehouse.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import ru.practicum.warehouse.model.OrderBooking;

import java.util.Optional;
import java.util.UUID;

public interface OrderBookingRepository extends JpaRepository<OrderBooking, UUID> {
    Optional<OrderBooking> findByOrderId(UUID orderId);
}