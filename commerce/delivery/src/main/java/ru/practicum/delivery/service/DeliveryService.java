package ru.practicum.delivery.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.practicum.interaction.dto.*;
import ru.practicum.interaction.exception.NoDeliveryFoundException;
import ru.practicum.delivery.mapper.DeliveryMapper;
import ru.practicum.delivery.model.Delivery;
import ru.practicum.delivery.repository.DeliveryRepository;
import ru.practicum.delivery.service.client.OrderClient;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class DeliveryService {

    private final DeliveryRepository deliveryRepository;
    private final DeliveryMapper deliveryMapper;
    private final OrderClient orderClient;

    private static final BigDecimal BASE_COST = BigDecimal.valueOf(5.0);
    private static final double FRAGILE_MULTIPLIER = 0.2;
    private static final double WEIGHT_MULTIPLIER = 0.3;
    private static final double VOLUME_MULTIPLIER = 0.2;
    private static final double ADDRESS_MISMATCH_MULTIPLIER = 0.2;

    @Transactional
    public DeliveryDto planDelivery(DeliveryDto deliveryDto) {
        log.info("Планирование доставки для заказа: {}", deliveryDto.getOrderId());

        Delivery delivery = Delivery.builder()
                .orderId(deliveryDto.getOrderId())
                .fromAddress(deliveryDto.getFromAddress())
                .toAddress(deliveryDto.getToAddress())
                .deliveryState(DeliveryState.CREATED)
                .build();

        delivery = deliveryRepository.save(delivery);
        log.info("Доставка создана с ID: {}", delivery.getDeliveryId());

        return deliveryMapper.toDto(delivery);
    }

    @Transactional(readOnly = true)
    public BigDecimal deliveryCost(OrderDto order) {
        log.info("Расчёт стоимости доставки для заказа: {}", order.getOrderId());

        if (order.getFromAddress() == null || order.getToAddress() == null) {
            throw new IllegalArgumentException("Адреса доставки не указаны");
        }

        BigDecimal cost = BASE_COST;

        String warehouseAddress = order.getFromAddress().getStreet();
        if (warehouseAddress != null && warehouseAddress.contains("ADDRESS_2")) {
            cost = cost.multiply(BigDecimal.valueOf(2));
        } else {
            cost = cost.multiply(BigDecimal.valueOf(1));
        }
        cost = cost.add(BASE_COST);

        if (Boolean.TRUE.equals(order.getFragile())) {
            cost = cost.add(cost.multiply(BigDecimal.valueOf(FRAGILE_MULTIPLIER)));
        }

        if (order.getDeliveryWeight() != null) {
            cost = cost.add(BigDecimal.valueOf(order.getDeliveryWeight() * WEIGHT_MULTIPLIER));
        }

        if (order.getDeliveryVolume() != null) {
            cost = cost.add(BigDecimal.valueOf(order.getDeliveryVolume() * VOLUME_MULTIPLIER));
        }

        String deliveryStreet = order.getToAddress().getStreet();
        if (deliveryStreet != null && !deliveryStreet.equals(warehouseAddress)) {
            cost = cost.add(cost.multiply(BigDecimal.valueOf(ADDRESS_MISMATCH_MULTIPLIER)));
        }

        return cost.setScale(2, RoundingMode.HALF_UP);
    }

    @Transactional
    public void deliveryPicked(UUID orderId) {
        Delivery delivery = deliveryRepository.findByOrderId(orderId)
                .orElseThrow(() -> new NoDeliveryFoundException("Доставка для заказа " + orderId + " не найдена"));

        delivery.setDeliveryState(DeliveryState.IN_PROGRESS);
        deliveryRepository.save(delivery);

        log.info("Товары для заказа {} переданы в доставку", orderId);
    }

    @Transactional
    public void deliverySuccessful(UUID orderId) {
        Delivery delivery = deliveryRepository.findByOrderId(orderId)
                .orElseThrow(() -> new NoDeliveryFoundException("Доставка для заказа " + orderId + " не найдена"));

        delivery.setDeliveryState(DeliveryState.DELIVERED);
        deliveryRepository.save(delivery);

        orderClient.deliverySuccess(orderId);
        log.info("Доставка для заказа {} успешно завершена", orderId);
    }

    @Transactional
    public void deliveryFailed(UUID orderId) {
        Delivery delivery = deliveryRepository.findByOrderId(orderId)
                .orElseThrow(() -> new NoDeliveryFoundException("Доставка для заказа " + orderId + " не найдена"));

        delivery.setDeliveryState(DeliveryState.FAILED);
        deliveryRepository.save(delivery);

        orderClient.deliveryFailed(orderId);
        log.info("Доставка для заказа {} завершилась с ошибкой", orderId);
    }
}