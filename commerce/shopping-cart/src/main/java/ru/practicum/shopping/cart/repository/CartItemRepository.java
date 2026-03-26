package ru.practicum.shopping.cart.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import ru.practicum.shopping.cart.model.CartItem;

import java.util.Optional;
import java.util.UUID;

public interface CartItemRepository extends JpaRepository<CartItem, UUID> {
    Optional<CartItem> findByCart_ShoppingCartIdAndProductId(UUID cartId, UUID productId);

    void deleteByCart_ShoppingCartIdAndProductId(UUID cartId, UUID productId);
}