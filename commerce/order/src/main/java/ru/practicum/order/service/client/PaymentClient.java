package ru.practicum.order.service.client;

import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.*;
import ru.practicum.interaction.dto.PaymentDto;
import ru.practicum.interaction.dto.OrderDto;

import java.math.BigDecimal;
import java.util.UUID;

@FeignClient(name = "payment")
public interface PaymentClient {

    @PostMapping("/api/v1/payment/product-cost")
    BigDecimal productCost(@RequestBody UUID orderId);

    @PostMapping("/api/v1/payment/total-cost")
    BigDecimal calculateTotalCost(@RequestParam("orderId") UUID orderId,
                                  @RequestParam("deliveryCost") BigDecimal deliveryCost);

    @PostMapping("/api/v1/payment")
    PaymentDto payment(@RequestBody OrderDto order);
}