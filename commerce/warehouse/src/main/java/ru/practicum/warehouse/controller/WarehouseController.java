package ru.practicum.warehouse.controller;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import ru.practicum.interaction.dto.*;
import ru.practicum.warehouse.dto.AddProductToWarehouseRequest;
import ru.practicum.warehouse.dto.NewProductInWarehouseRequest;
import ru.practicum.warehouse.service.WarehouseService;

@Slf4j
@RestController
@RequiredArgsConstructor
@RequestMapping("/api/v1/warehouse")
public class WarehouseController {

    private final WarehouseService warehouseService;

    @PutMapping
    @ResponseStatus(HttpStatus.OK)
    public void addNewProduct(@Valid @RequestBody NewProductInWarehouseRequest request) {
        log.info("PUT /api/v1/warehouse - добавление нового товара: {}", request.getProductId());
        warehouseService.addNewProduct(request);
    }

    @PostMapping("/add")
    @ResponseStatus(HttpStatus.OK)
    public void addProductToWarehouse(@Valid @RequestBody AddProductToWarehouseRequest request) {
        log.info("POST /api/v1/warehouse/add - приёмка товара: {} количество: {}",
                request.getProductId(), request.getQuantity());
        warehouseService.addProductToWarehouse(request);
    }

    @PostMapping("/check")
    public BookedProductsDto checkProductsAvailability(@Valid @RequestBody ShoppingCartDto shoppingCart) {
        log.info("POST /api/v1/warehouse/check - проверка корзины: {} товаров",
                shoppingCart.getProducts().size());
        return warehouseService.checkProductsAvailability(shoppingCart);
    }

    @GetMapping("/address")
    public AddressDto getWarehouseAddress() {
        log.info("GET /api/v1/warehouse/address - запрос адреса склада");
        return warehouseService.getWarehouseAddress();
    }
}