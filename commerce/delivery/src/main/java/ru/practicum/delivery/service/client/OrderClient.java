package ru.practicum.delivery.service.client;

import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;

import java.util.UUID;

@FeignClient(name = "order")
public interface OrderClient {

    @PostMapping("/api/v1/order/{orderId}/delivery-success")
    void deliverySuccess(@PathVariable("orderId") UUID orderId);

    @PostMapping("/api/v1/order/{orderId}/delivery-failed")
    void deliveryFailed(@PathVariable("orderId") UUID orderId);
}