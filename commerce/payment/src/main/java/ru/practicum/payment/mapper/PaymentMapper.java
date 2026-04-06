package ru.practicum.payment.mapper;

import org.springframework.stereotype.Component;
import ru.practicum.interaction.dto.PaymentDto;
import ru.practicum.payment.model.Payment;

@Component
public class PaymentMapper {

    public PaymentDto toDto(Payment payment) {
        return PaymentDto.builder()
                .paymentId(payment.getPaymentId())
                .totalPayment(payment.getTotalPayment())
                .deliveryTotal(payment.getDeliveryTotal())
                .feeTotal(payment.getFeeTotal())
                .build();
    }
}