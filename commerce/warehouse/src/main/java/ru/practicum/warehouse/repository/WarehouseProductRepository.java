package ru.practicum.warehouse.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import ru.practicum.warehouse.model.WarehouseProduct;

import java.util.Optional;
import java.util.UUID;

public interface WarehouseProductRepository extends JpaRepository<WarehouseProduct, UUID> {
    Optional<WarehouseProduct> findByProductId(UUID productId);
}