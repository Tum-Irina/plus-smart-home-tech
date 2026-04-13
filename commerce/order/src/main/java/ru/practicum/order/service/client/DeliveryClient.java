package ru.practicum.order.service.client;

import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.*;
import ru.practicum.interaction.dto.DeliveryDto;
import ru.practicum.interaction.request.PlanDeliveryRequest;

import java.math.BigDecimal;
import java.util.UUID;

@FeignClient(name = "delivery")
public interface DeliveryClient {

    @PostMapping("/api/v1/delivery/plan")
    DeliveryDto planDelivery(@RequestBody PlanDeliveryRequest request);

    @PostMapping("/api/v1/delivery/cost/{orderId}")
    BigDecimal calculateDeliveryCost(@PathVariable("orderId") UUID orderId);
}