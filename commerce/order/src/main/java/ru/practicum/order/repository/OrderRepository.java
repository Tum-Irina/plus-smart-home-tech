package ru.practicum.order.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import ru.practicum.order.model.Order;

import java.util.Optional;
import java.util.UUID;

public interface OrderRepository extends JpaRepository<Order, UUID> {

    @Query("SELECT DISTINCT o FROM Order o LEFT JOIN FETCH o.items WHERE o.orderId = :orderId")
    Optional<Order> findByIdWithItems(@Param("orderId") UUID orderId);

    @Query("SELECT o FROM Order o LEFT JOIN FETCH o.items WHERE o.shoppingCartId = :shoppingCartId")
    Optional<Order> findByShoppingCartIdWithItems(@Param("shoppingCartId") UUID shoppingCartId);
}