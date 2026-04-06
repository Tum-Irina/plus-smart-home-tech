package ru.practicum.delivery.model;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import ru.practicum.interaction.dto.AddressDto;
import ru.practicum.interaction.dto.DeliveryState;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "deliveries")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Delivery {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID deliveryId;

    @Column(name = "order_id", nullable = false, unique = true)
    private UUID orderId;

    @Embedded
    @AttributeOverrides({
            @AttributeOverride(name = "country", column = @Column(name = "from_address_country")),
            @AttributeOverride(name = "city", column = @Column(name = "from_address_city")),
            @AttributeOverride(name = "street", column = @Column(name = "from_address_street")),
            @AttributeOverride(name = "house", column = @Column(name = "from_address_house")),
            @AttributeOverride(name = "flat", column = @Column(name = "from_address_flat"))
    })
    private AddressDto fromAddress;

    @Embedded
    @AttributeOverrides({
            @AttributeOverride(name = "country", column = @Column(name = "to_address_country")),
            @AttributeOverride(name = "city", column = @Column(name = "to_address_city")),
            @AttributeOverride(name = "street", column = @Column(name = "to_address_street")),
            @AttributeOverride(name = "house", column = @Column(name = "to_address_house")),
            @AttributeOverride(name = "flat", column = @Column(name = "to_address_flat"))
    })
    private AddressDto toAddress;

    @Enumerated(EnumType.STRING)
    @Column(name = "delivery_state", nullable = false)
    private DeliveryState deliveryState;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    @PrePersist
    protected void onCreate() {
        createdAt = Instant.now();
        updatedAt = Instant.now();
    }

    @PreUpdate
    protected void onUpdate() {
        updatedAt = Instant.now();
    }
}