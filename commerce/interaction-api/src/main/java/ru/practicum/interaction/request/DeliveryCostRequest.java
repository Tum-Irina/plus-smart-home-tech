package ru.practicum.interaction.request;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import ru.practicum.interaction.dto.AddressDto;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DeliveryCostRequest {
    private AddressDto fromAddress;
    private AddressDto toAddress;
    private Double volume;
    private Double weight;
    private Boolean fragile;
}