package ru.practicum.shopping.cart.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import ru.practicum.shopping.cart.model.Cart;

import java.util.Optional;
import java.util.UUID;

public interface CartRepository extends JpaRepository<Cart, UUID> {
    @Query("SELECT c FROM Cart c LEFT JOIN FETCH c.items WHERE c.username = :username")
    Optional<Cart> findByUsernameWithItems(@Param("username") String username);
}