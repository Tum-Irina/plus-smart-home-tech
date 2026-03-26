package ru.practicum.warehouse.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.practicum.interaction.dto.*;
import ru.practicum.interaction.exception.*;
import ru.practicum.warehouse.dto.AddProductToWarehouseRequest;
import ru.practicum.warehouse.dto.NewProductInWarehouseRequest;
import ru.practicum.warehouse.exception.NoSpecifiedProductInWarehouseException;
import ru.practicum.warehouse.exception.SpecifiedProductAlreadyInWarehouseException;
import ru.practicum.warehouse.model.Dimension;
import ru.practicum.warehouse.model.WarehouseProduct;
import ru.practicum.warehouse.repository.WarehouseProductRepository;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class WarehouseService {

    private final WarehouseProductRepository repository;

    @Transactional
    public void addNewProduct(NewProductInWarehouseRequest request) {
        UUID productId = request.getProductId();

        if (repository.findByProductId(productId).isPresent()) {
            throw new SpecifiedProductAlreadyInWarehouseException(
                    "Товар с ID " + productId + " уже зарегистрирован на складе"
            );
        }

        WarehouseProduct product = WarehouseProduct.builder()
                .productId(productId)
                .quantity(0L)
                .fragile(request.getFragile())
                .dimension(ru.practicum.warehouse.model.Dimension.builder()
                        .width(request.getDimension().getWidth())
                        .height(request.getDimension().getHeight())
                        .depth(request.getDimension().getDepth())
                        .build())
                .weight(request.getWeight())
                .build();

        repository.save(product);
        log.info("Новый товар {} добавлен на склад", productId);
    }

    @Transactional
    public void addProductToWarehouse(AddProductToWarehouseRequest request) {
        UUID productId = request.getProductId();
        Long quantity = request.getQuantity();

        WarehouseProduct product = repository.findByProductId(productId)
                .orElseThrow(() -> new NoSpecifiedProductInWarehouseException(
                        "Товар с ID " + productId + " не найден на складе"
                ));

        product.setQuantity(product.getQuantity() + quantity);
        repository.save(product);
        log.info("На склад добавлено {} единиц товара {}. Новое количество: {}",
                quantity, productId, product.getQuantity());
    }

    @Transactional(readOnly = true)
    public BookedProductsDto checkProductsAvailability(ShoppingCartDto shoppingCart) {
        Map<UUID, Long> insufficientProducts = new HashMap<>();
        double totalWeight = 0.0;
        double totalVolume = 0.0;
        boolean hasFragile = false;

        for (Map.Entry<UUID, Long> entry : shoppingCart.getProducts().entrySet()) {
            UUID productId = entry.getKey();
            Long requestedQuantity = entry.getValue();

            WarehouseProduct product = repository.findByProductId(productId)
                    .orElseThrow(() -> new NoSpecifiedProductInWarehouseException(
                            "Товар с ID " + productId + " не найден на складе"
                    ));

            if (product.getQuantity() < requestedQuantity) {
                insufficientProducts.put(productId, product.getQuantity());
            }

            totalWeight += product.getWeight() * requestedQuantity;

            Dimension dim = product.getDimension();
            double volume = dim.getWidth() * dim.getHeight() * dim.getDepth();
            totalVolume += volume * requestedQuantity;

            if (Boolean.TRUE.equals(product.getFragile())) {
                hasFragile = true;
            }
        }

        if (!insufficientProducts.isEmpty()) {
            throw new ProductInShoppingCartLowQuantityInWarehouse(
                    "Недостаточно товаров на складе",
                    insufficientProducts
            );
        }

        return BookedProductsDto.builder()
                .deliveryWeight(totalWeight)
                .deliveryVolume(totalVolume)
                .fragile(hasFragile)
                .build();
    }

    public AddressDto getWarehouseAddress() {
        String[] addresses = {"ADDRESS_1", "ADDRESS_2"};
        String selectedAddress = addresses[new java.util.Random().nextInt(addresses.length)];

        return AddressDto.builder()
                .country(selectedAddress)
                .city(selectedAddress)
                .street(selectedAddress)
                .house(selectedAddress)
                .flat(selectedAddress)
                .build();
    }
}