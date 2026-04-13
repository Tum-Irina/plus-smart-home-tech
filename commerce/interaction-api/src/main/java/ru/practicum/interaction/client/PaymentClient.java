package ru.practicum.interaction.client;

import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.*;
import ru.practicum.interaction.dto.PaymentDto;
import ru.practicum.interaction.request.PaymentRequest;
import ru.practicum.interaction.request.TotalCostRequest;

import java.math.BigDecimal;
import java.util.UUID;

@FeignClient(name = "payment")
public interface PaymentClient {

    @PostMapping("/api/v1/payment/product-cost")
    BigDecimal productCost(@RequestBody UUID orderId);

    @PostMapping("/api/v1/payment/total-cost")
    BigDecimal totalCost(@RequestBody TotalCostRequest request);

    @PostMapping("/api/v1/payment/payment")
    PaymentDto payment(@RequestBody PaymentRequest request);

    @PostMapping("/api/v1/payment/{paymentId}/success")
    void paymentSuccess(@PathVariable("paymentId") UUID paymentId);

    @PostMapping("/api/v1/payment/{paymentId}/failed")
    void paymentFailed(@PathVariable("paymentId") UUID paymentId);
}