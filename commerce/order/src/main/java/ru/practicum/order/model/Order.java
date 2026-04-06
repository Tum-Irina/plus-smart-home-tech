package ru.practicum.order.model;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import ru.practicum.interaction.dto.AddressDto;
import ru.practicum.interaction.dto.OrderState;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Entity
@Table(name = "orders")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Order {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID orderId;

    @Column(name = "shopping_cart_id", nullable = false)
    private UUID shoppingCartId;

    @Column(name = "delivery_id")
    private UUID deliveryId;

    @Column(name = "payment_id")
    private UUID paymentId;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private OrderState state;

    @Column(name = "delivery_volume")
    private Double deliveryVolume;

    @Column(name = "delivery_weight")
    private Double deliveryWeight;

    private Boolean fragile;

    @Column(name = "total_price", nullable = false)
    private BigDecimal totalPrice;

    @Column(name = "products_price", nullable = false)
    private BigDecimal productPrice;

    @Column(name = "delivery_price")
    private BigDecimal deliveryPrice;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    @OneToMany(mappedBy = "order", cascade = CascadeType.ALL, orphanRemoval = true, fetch = FetchType.LAZY)
    @Builder.Default
    private List<OrderItem> items = new ArrayList<>();

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