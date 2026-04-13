package ru.practicum.order.service.client;

import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.*;
import ru.practicum.interaction.dto.AddressDto;
import ru.practicum.interaction.dto.BookedProductsDto;
import ru.practicum.interaction.dto.ShoppingCartDto;
import ru.practicum.interaction.request.AssemblyRequest;

import java.util.Map;
import java.util.UUID;

@FeignClient(name = "warehouse")
public interface WarehouseClient {

    @PostMapping("/api/v1/warehouse/check")
    BookedProductsDto checkProducts(@RequestBody ShoppingCartDto shoppingCartDto);

    @PostMapping("/api/v1/warehouse/assembly")
    void assemblyProductForOrder(@RequestBody AssemblyRequest request);

    @GetMapping("/api/v1/warehouse/address")
    AddressDto getWarehouseAddress();

    @PostMapping("/api/v1/warehouse/return")
    void acceptReturn(@RequestBody Map<UUID, Long> products);
}