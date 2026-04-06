package ru.practicum.interaction.client;

import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.*;
import ru.practicum.interaction.dto.OrderDto;

import java.util.UUID;

@FeignClient(name = "order")
public interface OrderClient {

    @GetMapping("/api/v1/order/{orderId}")
    OrderDto getOrder(@PathVariable("orderId") UUID orderId);

    @PostMapping("/api/v1/order/{orderId}/payment-success")
    void paymentSuccess(@PathVariable("orderId") UUID orderId);

    @PostMapping("/api/v1/order/{orderId}/payment-failed")
    void paymentFailed(@PathVariable("orderId") UUID orderId);

    @PostMapping("/api/v1/order/{orderId}/delivery-success")
    void deliverySuccess(@PathVariable("orderId") UUID orderId);

    @PostMapping("/api/v1/order/{orderId}/delivery-failed")
    void deliveryFailed(@PathVariable("orderId") UUID orderId);
}