package ru.practicum.shopping.cart.controller;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import ru.practicum.interaction.dto.ShoppingCartDto;
import ru.practicum.shopping.cart.dto.ChangeProductQuantityRequest;
import ru.practicum.shopping.cart.service.ShoppingCartService;

import java.util.List;
import java.util.Map;
import java.util.UUID;

@Slf4j
@RestController
@RequiredArgsConstructor
@RequestMapping("/api/v1/shopping-cart")
public class ShoppingCartController {

    private final ShoppingCartService shoppingCartService;

    @GetMapping
    public ShoppingCartDto getShoppingCart(@RequestParam String username) {
        log.info("GET /api/v1/shopping-cart - username: {}", username);
        return shoppingCartService.getShoppingCart(username);
    }

    @PutMapping
    public ShoppingCartDto addProductToShoppingCart(
            @RequestParam String username,
            @RequestBody Map<UUID, Long> products) {
        log.info("PUT /api/v1/shopping-cart - username: {}, товаров: {}", username, products.size());
        return shoppingCartService.addProductsToCart(username, products);
    }

    @PostMapping("/change-quantity")
    public ShoppingCartDto changeProductQuantity(
            @RequestParam String username,
            @Valid @RequestBody ChangeProductQuantityRequest request) {
        log.info("POST /api/v1/shopping-cart/change-quantity - username: {}, productId: {}, newQuantity: {}",
                username, request.getProductId(), request.getNewQuantity());
        return shoppingCartService.changeProductQuantity(username, request);
    }

    @PostMapping("/remove")
    public ShoppingCartDto removeFromShoppingCart(
            @RequestParam String username,
            @RequestBody List<UUID> productIds) {
        log.info("POST /api/v1/shopping-cart/remove - username: {}, товаров: {}", username, productIds.size());
        return shoppingCartService.removeProductsFromCart(username, productIds);
    }

    @DeleteMapping
    @ResponseStatus(HttpStatus.OK)
    public void deactivateCurrentShoppingCart(@RequestParam String username) {
        log.info("DELETE /api/v1/shopping-cart - username: {}", username);
        shoppingCartService.deactivateCart(username);
    }
}