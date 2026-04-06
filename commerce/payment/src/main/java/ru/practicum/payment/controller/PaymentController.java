package ru.practicum.payment.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.*;
import ru.practicum.interaction.dto.OrderDto;
import ru.practicum.interaction.dto.PaymentDto;
import ru.practicum.payment.service.PaymentService;

import java.math.BigDecimal;
import java.util.UUID;

@Slf4j
@RestController
@RequiredArgsConstructor
@RequestMapping("/api/v1/payment")
public class PaymentController {

    private final PaymentService paymentService;

    @PostMapping("/productCost")
    public BigDecimal productCost(@RequestBody OrderDto order) {
        log.info("POST /api/v1/payment/productCost - расчёт стоимости товаров для заказа: {}", order.getOrderId());
        return paymentService.productCost(order);
    }

    @PostMapping("/totalCost")
    public BigDecimal getTotalCost(@RequestBody OrderDto order) {
        log.info("POST /api/v1/payment/totalCost - расчёт полной стоимости для заказа: {}", order.getOrderId());
        return paymentService.getTotalCost(order);
    }

    @PostMapping
    public PaymentDto payment(@RequestBody OrderDto order) {
        log.info("POST /api/v1/payment - формирование оплаты для заказа: {}", order.getOrderId());
        return paymentService.payment(order);
    }

    @PostMapping("/refund")
    public void paymentSuccess(@RequestBody UUID paymentId) {
        log.info("POST /api/v1/payment/refund - успешная оплата: {}", paymentId);
        paymentService.paymentSuccess(paymentId);
    }

    @PostMapping("/failed")
    public void paymentFailed(@RequestBody UUID paymentId) {
        log.info("POST /api/v1/payment/failed - ошибка оплаты: {}", paymentId);
        paymentService.paymentFailed(paymentId);
    }
}