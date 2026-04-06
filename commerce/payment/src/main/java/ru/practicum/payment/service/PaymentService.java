package ru.practicum.payment.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.practicum.interaction.dto.*;
import ru.practicum.interaction.exception.NoOrderFoundException;
import ru.practicum.interaction.exception.NotEnoughInfoInOrderToCalculateException;
import ru.practicum.payment.mapper.PaymentMapper;
import ru.practicum.payment.model.Payment;
import ru.practicum.payment.repository.PaymentRepository;
import ru.practicum.payment.service.client.OrderClient;
import ru.practicum.payment.service.client.ShoppingStoreClient;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Map;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class PaymentService {

    private final PaymentRepository paymentRepository;
    private final PaymentMapper paymentMapper;
    private final OrderClient orderClient;
    private final ShoppingStoreClient shoppingStoreClient;

    @Transactional(readOnly = true)
    public BigDecimal productCost(OrderDto order) {
        if (order == null || order.getProducts() == null || order.getProducts().isEmpty()) {
            throw new NotEnoughInfoInOrderToCalculateException("Недостаточно информации в заказе для расчёта стоимости товаров");
        }

        BigDecimal total = BigDecimal.ZERO;

        for (Map.Entry<UUID, Long> entry : order.getProducts().entrySet()) {
            UUID productId = entry.getKey();
            Long quantity = entry.getValue();

            try {
                ProductDto product = shoppingStoreClient.getProduct(productId);
                BigDecimal productPrice = product.getPrice();
                total = total.add(productPrice.multiply(BigDecimal.valueOf(quantity)));
            } catch (Exception e) {
                log.error("Ошибка получения цены товара {}: {}", productId, e.getMessage());
                throw new NotEnoughInfoInOrderToCalculateException("Не удалось получить цену товара: " + productId);
            }
        }

        return total.setScale(2, RoundingMode.HALF_UP);
    }

    @Transactional(readOnly = true)
    public BigDecimal getTotalCost(OrderDto order) {
        if (order == null || order.getDeliveryPrice() == null || order.getProductPrice() == null) {
            throw new NotEnoughInfoInOrderToCalculateException("Недостаточно информации в заказе для расчёта полной стоимости");
        }

        BigDecimal productPrice = order.getProductPrice();
        BigDecimal deliveryPrice = order.getDeliveryPrice();
        BigDecimal fee = productPrice.multiply(BigDecimal.valueOf(0.1));

        return productPrice.add(deliveryPrice).add(fee).setScale(2, RoundingMode.HALF_UP);
    }

    @Transactional
    public PaymentDto payment(OrderDto order) {
        if (order == null || order.getOrderId() == null) {
            throw new NotEnoughInfoInOrderToCalculateException("Недостаточно информации в заказе для формирования оплаты");
        }

        UUID orderId = order.getOrderId();

        if (paymentRepository.findByOrderId(orderId).isPresent()) {
            throw new IllegalStateException("Оплата для заказа " + orderId + " уже существует");
        }

        BigDecimal productPrice = order.getProductPrice();
        if (productPrice == null) {
            productPrice = productCost(order);
        }

        BigDecimal deliveryPrice = order.getDeliveryPrice() != null ? order.getDeliveryPrice() : BigDecimal.ZERO;
        BigDecimal fee = productPrice.multiply(BigDecimal.valueOf(0.1));
        BigDecimal total = productPrice.add(deliveryPrice).add(fee);

        Payment payment = Payment.builder()
                .orderId(orderId)
                .totalPayment(total)
                .deliveryTotal(deliveryPrice)
                .feeTotal(fee)
                .paymentState(PaymentState.PENDING)
                .build();

        payment = paymentRepository.save(payment);

        log.info("Создана оплата для заказа: {}, сумма: {}", orderId, total);
        return paymentMapper.toDto(payment);
    }

    @Transactional
    public void paymentSuccess(UUID paymentId) {
        Payment payment = paymentRepository.findById(paymentId)
                .orElseThrow(() -> new NoOrderFoundException("Оплата не найдена: " + paymentId));

        payment.setPaymentState(PaymentState.SUCCESS);
        paymentRepository.save(payment);

        orderClient.paymentSuccess(payment.getOrderId());
        log.info("Оплата {} успешно завершена для заказа {}", paymentId, payment.getOrderId());
    }

    @Transactional
    public void paymentFailed(UUID paymentId) {
        Payment payment = paymentRepository.findById(paymentId)
                .orElseThrow(() -> new NoOrderFoundException("Оплата не найдена: " + paymentId));

        payment.setPaymentState(PaymentState.FAILED);
        paymentRepository.save(payment);

        orderClient.paymentFailed(payment.getOrderId());
        log.info("Оплата {} завершена с ошибкой для заказа {}", paymentId, payment.getOrderId());
    }
}