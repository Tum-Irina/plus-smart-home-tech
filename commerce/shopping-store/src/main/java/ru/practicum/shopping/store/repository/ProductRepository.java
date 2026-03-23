package ru.practicum.shopping.store.repository;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import ru.practicum.shopping.store.dto.ProductCategory;
import ru.practicum.shopping.store.dto.ProductState;
import ru.practicum.shopping.store.model.Product;

import java.util.UUID;

public interface ProductRepository extends JpaRepository<Product, UUID> {

    Page<Product> findByProductCategoryAndProductState(
            ProductCategory category,
            ProductState state,
            Pageable pageable);
}