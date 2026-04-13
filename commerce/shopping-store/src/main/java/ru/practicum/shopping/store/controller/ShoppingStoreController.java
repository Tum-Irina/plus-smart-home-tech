package ru.practicum.shopping.store.controller;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import ru.practicum.interaction.dto.ProductCategory;
import ru.practicum.interaction.dto.ProductDto;
import ru.practicum.interaction.dto.QuantityState;
import ru.practicum.shopping.store.dto.*;
import ru.practicum.shopping.store.service.ShoppingStoreService;

import java.util.UUID;

@Slf4j
@RestController
@RequiredArgsConstructor
@RequestMapping("/api/v1/shopping-store")
public class ShoppingStoreController {

    private final ShoppingStoreService shoppingStoreService;

    @GetMapping
    public PageProductDto getProducts(
            @RequestParam ProductCategory category,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size,
            @RequestParam(defaultValue = "productName,asc") String[] sort) {
        log.info("GET /api/v1/shopping-store - category: {}, page: {}, size: {}", category, page, size);
        return shoppingStoreService.getProducts(category, page, size, sort);
    }

    @GetMapping("/{productId}")
    public ProductDto getProduct(@PathVariable UUID productId) {
        log.info("GET /api/v1/shopping-store/{}", productId);
        return shoppingStoreService.getProduct(productId);
    }

    @PutMapping
    @ResponseStatus(HttpStatus.OK)
    public ProductDto createNewProduct(@Valid @RequestBody ProductDto productDto) {
        log.info("PUT /api/v1/shopping-store - создание товара: {}", productDto.getProductName());
        return shoppingStoreService.createNewProduct(productDto);
    }

    @PostMapping
    @ResponseStatus(HttpStatus.OK)
    public ProductDto updateProduct(@Valid @RequestBody ProductDto productDto) {
        log.info("POST /api/v1/shopping-store - обновление товара: {}", productDto.getProductId());
        return shoppingStoreService.updateProduct(productDto);
    }

    @PostMapping("/removeProductFromStore")
    public boolean removeProductFromStore(@RequestBody UUID productId) {
        log.info("POST /api/v1/shopping-store/removeProductFromStore - удаление товара: {}", productId);
        return shoppingStoreService.removeProductFromStore(productId);
    }

    @PostMapping("/quantityState")
    public boolean setProductQuantityState(
            @RequestParam UUID productId,
            @RequestParam QuantityState quantityState) {

        log.info("POST /api/v1/shopping-store/quantityState - productId: {}, quantityState: {}", productId, quantityState);

        SetProductQuantityStateRequest request = new SetProductQuantityStateRequest();
        request.setProductId(productId);
        request.setQuantityState(quantityState);

        return shoppingStoreService.setProductQuantityState(request);
    }
}