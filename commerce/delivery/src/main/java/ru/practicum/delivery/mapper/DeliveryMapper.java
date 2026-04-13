package ru.practicum.delivery.mapper;

import org.springframework.stereotype.Component;
import ru.practicum.interaction.dto.DeliveryDto;
import ru.practicum.delivery.model.Delivery;

@Component
public class DeliveryMapper {

    public DeliveryDto toDto(Delivery delivery) {
        return DeliveryDto.builder()
                .deliveryId(delivery.getDeliveryId())
                .fromAddress(delivery.getFromAddress())
                .toAddress(delivery.getToAddress())
                .orderId(delivery.getOrderId())
                .deliveryState(delivery.getDeliveryState())
                .build();
    }
}