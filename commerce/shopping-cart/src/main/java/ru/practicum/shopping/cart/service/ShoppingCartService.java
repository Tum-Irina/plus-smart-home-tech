package ru.practicum.shopping.cart.service;

import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.practicum.interaction.dto.BookedProductsDto;
import ru.practicum.interaction.dto.ShoppingCartDto;
import ru.practicum.shopping.cart.dto.ChangeProductQuantityRequest;
import ru.practicum.shopping.cart.exception.NoProductsInShoppingCartException;
import ru.practicum.shopping.cart.exception.NotAuthorizedUserException;
import ru.practicum.shopping.cart.model.Cart;
import ru.practicum.shopping.cart.model.CartItem;
import ru.practicum.shopping.cart.repository.CartItemRepository;
import ru.practicum.shopping.cart.repository.CartRepository;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class ShoppingCartService {

    private final CartRepository cartRepository;
    private final CartItemRepository cartItemRepository;
    private final WarehouseClientService warehouseClient;

    @Transactional(readOnly = true)
    public ShoppingCartDto getShoppingCart(String username) {
        if (username == null || username.isBlank()) {
            throw new NotAuthorizedUserException("Имя пользователя не должно быть пустым");
        }

        Cart cart = cartRepository.findByUsernameWithItems(username)
                .orElseGet(() -> createNewCart(username));

        return mapToDto(cart);
    }

    @Transactional
    public ShoppingCartDto addProductsToCart(String username, Map<UUID, Long> products) {
        if (username == null || username.isBlank()) {
            throw new NotAuthorizedUserException("Имя пользователя не должно быть пустым");
        }

        if (products == null || products.isEmpty()) {
            throw new NoProductsInShoppingCartException("Нет товаров для добавления");
        }

        Cart cart = cartRepository.findByUsernameWithItems(username)
                .orElseGet(() -> createNewCart(username));

        if (!cart.getActive()) {
            throw new IllegalStateException("Корзина деактивирована");
        }

        ShoppingCartDto cartDto = ShoppingCartDto.builder()
                .shoppingCartId(cart.getShoppingCartId())
                .products(new HashMap<>(products))
                .build();

        BookedProductsDto booked = checkWarehouse(cartDto);
        log.info("Проверка склада: вес={}, объём={}, хрупкие={}",
                booked.getDeliveryWeight(), booked.getDeliveryVolume(), booked.getFragile());

        for (Map.Entry<UUID, Long> entry : products.entrySet()) {
            UUID productId = entry.getKey();
            Long newQuantity = entry.getValue();

            CartItem existingItem = cartItemRepository
                    .findByCart_ShoppingCartIdAndProductId(cart.getShoppingCartId(), productId)
                    .orElse(null);

            if (existingItem != null) {
                existingItem.setQuantity(existingItem.getQuantity() + newQuantity);
                cartItemRepository.save(existingItem);
            } else {
                CartItem newItem = CartItem.builder()
                        .cart(cart)
                        .productId(productId)
                        .quantity(newQuantity)
                        .build();
                cartItemRepository.save(newItem);
                cart.getItems().add(newItem);
            }
        }

        cartRepository.save(cart);
        log.info("Товары добавлены в корзину пользователя {}", username);

        return mapToDto(cart);
    }

    @CircuitBreaker(name = "warehouseService", fallbackMethod = "warehouseFallback")
    private BookedProductsDto checkWarehouse(ShoppingCartDto cartDto) {
        return warehouseClient.checkProducts(cartDto);
    }

    private BookedProductsDto warehouseFallback(ShoppingCartDto cartDto, Exception e) {
        log.error("Сервис склада недоступен. Ошибка: {}", e.getMessage());
        throw new RuntimeException("Сервис склада временно недоступен. Попробуйте позже.");
    }

    @Transactional
    public ShoppingCartDto changeProductQuantity(String username, ChangeProductQuantityRequest request) {
        if (username == null || username.isBlank()) {
            throw new NotAuthorizedUserException("Имя пользователя не должно быть пустым");
        }

        Cart cart = cartRepository.findByUsernameWithItems(username)
                .orElseThrow(() -> new NoProductsInShoppingCartException("Корзина не найдена"));

        if (!cart.getActive()) {
            throw new IllegalStateException("Корзина деактивирована");
        }

        CartItem item = cartItemRepository
                .findByCart_ShoppingCartIdAndProductId(cart.getShoppingCartId(), request.getProductId())
                .orElseThrow(() -> new NoProductsInShoppingCartException("Товар не найден в корзине"));

        if (request.getNewQuantity() <= 0) {
            cartItemRepository.delete(item);
            cart.getItems().remove(item);
            log.info("Товар {} удалён из корзины", request.getProductId());
        } else {
            item.setQuantity(request.getNewQuantity());
            cartItemRepository.save(item);
            log.info("Количество товара {} изменено на {}", request.getProductId(), request.getNewQuantity());
        }

        cartRepository.save(cart);
        return mapToDto(cart);
    }

    @Transactional
    public ShoppingCartDto removeProductsFromCart(String username, List<UUID> productIds) {
        if (username == null || username.isBlank()) {
            throw new NotAuthorizedUserException("Имя пользователя не должно быть пустым");
        }

        Cart cart = cartRepository.findByUsernameWithItems(username)
                .orElseThrow(() -> new NoProductsInShoppingCartException("Корзина не найдена"));

        for (UUID productId : productIds) {
            cartItemRepository.deleteByCart_ShoppingCartIdAndProductId(cart.getShoppingCartId(), productId);
        }

        cart.getItems().removeIf(item -> productIds.contains(item.getProductId()));
        cartRepository.save(cart);

        log.info("Товары удалены из корзины пользователя {}", username);
        return mapToDto(cart);
    }

    @Transactional
    public void deactivateCart(String username) {
        if (username == null || username.isBlank()) {
            throw new NotAuthorizedUserException("Имя пользователя не должно быть пустым");
        }

        Cart cart = cartRepository.findByUsernameWithItems(username)
                .orElseThrow(() -> new NoProductsInShoppingCartException("Корзина не найдена"));

        cart.setActive(false);
        cartRepository.save(cart);
        log.info("Корзина пользователя {} деактивирована", username);
    }

    private Cart createNewCart(String username) {
        Cart cart = Cart.builder()
                .username(username)
                .active(true)
                .build();
        return cartRepository.save(cart);
    }

    private ShoppingCartDto mapToDto(Cart cart) {
        Map<UUID, Long> products = cart.getItems().stream()
                .collect(Collectors.toMap(
                        CartItem::getProductId,
                        CartItem::getQuantity,
                        (a, b) -> a
                ));

        return ShoppingCartDto.builder()
                .shoppingCartId(cart.getShoppingCartId())
                .products(products)
                .build();
    }
}