package ru.practicum.delivery.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.*;
import ru.practicum.interaction.dto.DeliveryDto;
import ru.practicum.interaction.dto.OrderDto;
import ru.practicum.delivery.service.DeliveryService;

import java.math.BigDecimal;
import java.util.UUID;

@Slf4j
@RestController
@RequiredArgsConstructor
@RequestMapping("/api/v1/delivery")
public class DeliveryController {

    private final DeliveryService deliveryService;

    @PutMapping
    public DeliveryDto planDelivery(@RequestBody DeliveryDto deliveryDto) {
        log.info("PUT /api/v1/delivery - планирование доставки для заказа: {}", deliveryDto.getOrderId());
        return deliveryService.planDelivery(deliveryDto);
    }

    @PostMapping("/cost")
    public BigDecimal deliveryCost(@RequestBody OrderDto order) {
        log.info("POST /api/v1/delivery/cost - расчёт стоимости доставки для заказа: {}", order.getOrderId());
        return deliveryService.deliveryCost(order);
    }

    @PostMapping("/picked")
    public void deliveryPicked(@RequestBody UUID orderId) {
        log.info("POST /api/v1/delivery/picked - передача товаров в доставку для заказа: {}", orderId);
        deliveryService.deliveryPicked(orderId);
    }

    @PostMapping("/successful")
    public void deliverySuccessful(@RequestBody UUID orderId) {
        log.info("POST /api/v1/delivery/successful - успешная доставка для заказа: {}", orderId);
        deliveryService.deliverySuccessful(orderId);
    }

    @PostMapping("/failed")
    public void deliveryFailed(@RequestBody UUID orderId) {
        log.info("POST /api/v1/delivery/failed - ошибка доставки для заказа: {}", orderId);
        deliveryService.deliveryFailed(orderId);
    }
}