package ru.practicum.order.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import ru.practicum.interaction.dto.*;
import ru.practicum.order.service.OrderService;

import java.util.List;
import java.util.UUID;

@Slf4j
@RestController
@RequiredArgsConstructor
@RequestMapping("/api/v1/order")
public class OrderController {

    private final OrderService orderService;

    @PutMapping
    @ResponseStatus(HttpStatus.OK)
    public OrderDto createOrder(@RequestBody CreateNewOrderRequest request) {
        log.info("PUT /api/v1/order - создание заказа для корзины: {}", request.getShoppingCart().getShoppingCartId());
        return orderService.createOrder(request);
    }

    @GetMapping
    public List<OrderDto> getClientOrders(@RequestParam String username) {
        log.info("GET /api/v1/order - получение заказов для пользователя: {}", username);
        return orderService.getClientOrders(username);
    }

    @PostMapping("/assembly")
    @ResponseStatus(HttpStatus.OK)
    public OrderDto assembly(@RequestBody UUID orderId) {
        log.info("POST /api/v1/order/assembly - сборка заказа: {}", orderId);
        return orderService.assembly(orderId);
    }

    @PostMapping("/assembly/failed")
    @ResponseStatus(HttpStatus.OK)
    public OrderDto assemblyFailed(@RequestBody UUID orderId) {
        log.info("POST /api/v1/order/assembly/failed - ошибка сборки заказа: {}", orderId);
        return orderService.assemblyFailed(orderId);
    }

    @PostMapping("/payment")
    @ResponseStatus(HttpStatus.OK)
    public OrderDto payment(@RequestBody UUID orderId) {
        log.info("POST /api/v1/order/payment - оплата заказа: {}", orderId);
        return orderService.payment(orderId);
    }

    @PostMapping("/payment/failed")
    @ResponseStatus(HttpStatus.OK)
    public OrderDto paymentFailed(@RequestBody UUID orderId) {
        log.info("POST /api/v1/order/payment/failed - ошибка оплаты заказа: {}", orderId);
        return orderService.paymentFailed(orderId);
    }

    @PostMapping("/delivery")
    @ResponseStatus(HttpStatus.OK)
    public OrderDto delivery(@RequestBody UUID orderId) {
        log.info("POST /api/v1/order/delivery - доставка заказа: {}", orderId);
        return orderService.delivery(orderId);
    }

    @PostMapping("/delivery/failed")
    @ResponseStatus(HttpStatus.OK)
    public OrderDto deliveryFailed(@RequestBody UUID orderId) {
        log.info("POST /api/v1/order/delivery/failed - ошибка доставки заказа: {}", orderId);
        return orderService.deliveryFailed(orderId);
    }

    @PostMapping("/completed")
    @ResponseStatus(HttpStatus.OK)
    public OrderDto complete(@RequestBody UUID orderId) {
        log.info("POST /api/v1/order/completed - завершение заказа: {}", orderId);
        return orderService.complete(orderId);
    }

    @PostMapping("/calculate/total")
    @ResponseStatus(HttpStatus.OK)
    public OrderDto calculateTotalCost(@RequestBody UUID orderId) {
        log.info("POST /api/v1/order/calculate/total - расчёт общей стоимости заказа: {}", orderId);
        return orderService.calculateTotalCost(orderId);
    }

    @PostMapping("/calculate/delivery")
    @ResponseStatus(HttpStatus.OK)
    public OrderDto calculateDeliveryCost(@RequestBody UUID orderId) {
        log.info("POST /api/v1/order/calculate/delivery - расчёт стоимости доставки заказа: {}", orderId);
        return orderService.calculateDeliveryCost(orderId);
    }

    @PostMapping("/return")
    @ResponseStatus(HttpStatus.OK)
    public OrderDto productReturn(@RequestBody ProductReturnRequest request) {
        log.info("POST /api/v1/order/return - возврат заказа: {}", request.getOrderId());
        return orderService.productReturn(request);
    }
}