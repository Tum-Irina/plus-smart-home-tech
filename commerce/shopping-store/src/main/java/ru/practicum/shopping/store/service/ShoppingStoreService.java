package ru.practicum.shopping.store.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.practicum.interaction.dto.SortObject;
import ru.practicum.shopping.store.dto.*;
import ru.practicum.shopping.store.exception.ProductNotFoundException;
import ru.practicum.shopping.store.mapper.ProductMapper;
import ru.practicum.shopping.store.model.Product;
import ru.practicum.shopping.store.repository.ProductRepository;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class ShoppingStoreService {

    private final ProductRepository productRepository;
    private final ProductMapper productMapper;

    @Transactional(readOnly = true)
    public PageProductDto getProducts(
            ProductCategory category,
            int page,
            int size,
            String[] sort) {

        Sort sortOrder = Sort.by(Sort.Direction.ASC, "productName");

        if (sort != null && sort.length > 0) {
            if (sort.length >= 2) {
                String property = sort[0];
                String directionStr = sort[1];
                Sort.Direction direction = Sort.Direction.fromString(directionStr);
                sortOrder = Sort.by(direction, property);
            } else {
                String[] sortParams = sort[0].split(",");
                String property = sortParams[0];
                String directionStr = sortParams.length > 1 ? sortParams[1] : "asc";
                Sort.Direction direction = Sort.Direction.fromString(directionStr);
                sortOrder = Sort.by(direction, property);
            }
            log.info("Итоговая сортировка: {}", sortOrder);
        }

        Pageable pageable = PageRequest.of(page, size, sortOrder);
        Page<Product> productPage = productRepository.findByProductCategoryAndProductState(
                category,
                ProductState.ACTIVE,
                pageable
        );

        List<SortObject> sortList = sortOrder.get().map(order ->
                SortObject.builder()
                        .direction(order.getDirection().name())
                        .property(order.getProperty())
                        .ascending(order.getDirection().isAscending())
                        .ignoreCase(false)
                        .nullHandling("NATIVE")
                        .build()
        ).collect(Collectors.toList());

        return PageProductDto.builder()
                .content(productPage.getContent().stream()
                        .map(productMapper::toDto)
                        .toList())
                .totalPages(productPage.getTotalPages())
                .totalElements(productPage.getTotalElements())
                .size(productPage.getSize())
                .number(productPage.getNumber())
                .first(productPage.isFirst())
                .last(productPage.isLast())
                .empty(productPage.isEmpty())
                .sort(sortList)
                .build();
    }

    @Transactional(readOnly = true)
    public ProductDto getProduct(UUID productId) {
        Product product = productRepository.findById(productId)
                .orElseThrow(() -> new ProductNotFoundException("Товар с ID " + productId + " не найден"));
        return productMapper.toDto(product);
    }

    @Transactional
    public ProductDto createNewProduct(ProductDto productDto) {
        Product product = productMapper.toEntity(productDto);
        product.setProductState(ProductState.ACTIVE);
        product = productRepository.save(product);
        log.info("Создан новый товар: {}", product.getProductName());
        return productMapper.toDto(product);
    }

    @Transactional
    public ProductDto updateProduct(ProductDto productDto) {
        Product existingProduct = productRepository.findById(productDto.getProductId())
                .orElseThrow(() -> new ProductNotFoundException("Товар с ID " + productDto.getProductId() + " не найден"));

        existingProduct.setProductName(productDto.getProductName());
        existingProduct.setDescription(productDto.getDescription());
        existingProduct.setImageSrc(productDto.getImageSrc());
        existingProduct.setQuantityState(productDto.getQuantityState());
        existingProduct.setProductCategory(productDto.getProductCategory());
        existingProduct.setPrice(productDto.getPrice());

        Product updated = productRepository.save(existingProduct);
        log.info("Обновлён товар: {}", updated.getProductName());
        return productMapper.toDto(updated);
    }

    @Transactional
    public boolean removeProductFromStore(UUID productId) {
        Product product = productRepository.findById(productId)
                .orElseThrow(() -> new ProductNotFoundException("Товар с ID " + productId + " не найден"));

        product.setProductState(ProductState.DEACTIVATE);
        productRepository.save(product);
        log.info("Товар {} деактивирован", productId);
        return true;
    }

    @Transactional
    public boolean setProductQuantityState(SetProductQuantityStateRequest request) {
        Product product = productRepository.findById(request.getProductId())
                .orElseThrow(() -> new ProductNotFoundException("Товар с ID " + request.getProductId() + " не найден"));

        product.setQuantityState(request.getQuantityState());
        productRepository.save(product);
        log.info("Для товара {} установлен статус количества: {}", request.getProductId(), request.getQuantityState());
        return true;
    }
}