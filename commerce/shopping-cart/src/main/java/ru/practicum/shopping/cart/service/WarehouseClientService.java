package ru.practicum.shopping.cart.service;

import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import ru.practicum.interaction.dto.BookedProductsDto;
import ru.practicum.interaction.dto.ShoppingCartDto;

@FeignClient(name = "warehouse")
public interface WarehouseClientService {

    @PostMapping("/api/v1/warehouse/check")
    BookedProductsDto checkProducts(@RequestBody ShoppingCartDto shoppingCartDto);
}