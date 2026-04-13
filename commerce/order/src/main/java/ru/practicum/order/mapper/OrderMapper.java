package ru.practicum.order.mapper;

import org.springframework.stereotype.Component;
import ru.practicum.interaction.dto.OrderDto;
import ru.practicum.order.model.Order;
import ru.practicum.order.model.OrderItem;

import java.util.stream.Collectors;

@Component
public class OrderMapper {

    public OrderDto toDto(Order order) {
        return OrderDto.builder()
                .orderId(order.getOrderId())
                .shoppingCartId(order.getShoppingCartId())
                .deliveryId(order.getDeliveryId())
                .paymentId(order.getPaymentId())
                .state(order.getState())
                .products(order.getItems().stream()
                        .collect(Collectors.toMap(
                                OrderItem::getProductId,
                                OrderItem::getQuantity
                        )))
                .deliveryVolume(order.getDeliveryVolume())
                .deliveryWeight(order.getDeliveryWeight())
                .fragile(order.getFragile())
                .totalPrice(order.getTotalPrice())
                .productPrice(order.getProductPrice())
                .deliveryPrice(order.getDeliveryPrice())
                .build();
    }
}