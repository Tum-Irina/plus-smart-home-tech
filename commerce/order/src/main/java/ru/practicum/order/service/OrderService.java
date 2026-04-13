package ru.practicum.order.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.practicum.interaction.dto.*;
import ru.practicum.interaction.exception.NoOrderFoundException;
import ru.practicum.interaction.exception.NotAuthorizedUserException;
import ru.practicum.interaction.request.AssemblyRequest;
import ru.practicum.interaction.request.PlanDeliveryRequest;
import ru.practicum.order.mapper.OrderMapper;
import ru.practicum.order.model.Order;
import ru.practicum.order.model.OrderItem;
import ru.practicum.order.repository.OrderItemRepository;
import ru.practicum.order.repository.OrderRepository;
import ru.practicum.order.service.client.DeliveryClient;
import ru.practicum.order.service.client.PaymentClient;
import ru.practicum.order.service.client.ShoppingCartClient;
import ru.practicum.order.service.client.WarehouseClient;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class OrderService {

    private final OrderRepository orderRepository;
    private final OrderItemRepository orderItemRepository;
    private final OrderMapper orderMapper;
    private final PaymentClient paymentClient;
    private final DeliveryClient deliveryClient;
    private final WarehouseClient warehouseClient;
    private final ShoppingCartClient shoppingCartClient;

    @Transactional
    public OrderDto createOrder(CreateNewOrderRequest request) {
        ShoppingCartDto shoppingCart = request.getShoppingCart();
        AddressDto deliveryAddress = request.getDeliveryAddress();

        log.info("Создание заказа для корзины: {}", shoppingCart.getShoppingCartId());

        orderRepository.findByShoppingCartIdWithItems(shoppingCart.getShoppingCartId())
                .ifPresent(order -> {
                    throw new IllegalStateException("Заказ для корзины " + shoppingCart.getShoppingCartId() + " уже существует");
                });

        Order newOrder = Order.builder()
                .shoppingCartId(shoppingCart.getShoppingCartId())
                .state(OrderState.NEW)
                .productPrice(BigDecimal.ZERO)
                .totalPrice(BigDecimal.ZERO)
                .toAddress(deliveryAddress)
                .build();

        Order savedOrder = orderRepository.save(newOrder);

        List<OrderItem> items = shoppingCart.getProducts().entrySet().stream()
                .map(entry -> OrderItem.builder()
                        .order(savedOrder)
                        .productId(entry.getKey())
                        .quantity(entry.getValue())
                        .build())
                .collect(Collectors.toList());

        orderItemRepository.saveAll(items);
        savedOrder.setItems(items);

        BookedProductsDto booked = warehouseClient.checkProducts(shoppingCart);
        savedOrder.setDeliveryVolume(booked.getDeliveryVolume());
        savedOrder.setDeliveryWeight(booked.getDeliveryWeight());
        savedOrder.setFragile(booked.getFragile());

        AddressDto warehouseAddress = warehouseClient.getWarehouseAddress();
        savedOrder.setFromAddress(warehouseAddress);

        BigDecimal productsPrice = paymentClient.productCost(savedOrder.getOrderId());
        savedOrder.setProductPrice(productsPrice);

        BigDecimal deliveryPrice = deliveryClient.calculateDeliveryCost(savedOrder.getOrderId());
        savedOrder.setDeliveryPrice(deliveryPrice);

        BigDecimal totalPrice = paymentClient.calculateTotalCost(savedOrder.getOrderId(), deliveryPrice);
        savedOrder.setTotalPrice(totalPrice);

        Order finalOrder = orderRepository.save(savedOrder);

        log.info("Заказ создан: {}", finalOrder.getOrderId());
        return orderMapper.toDto(finalOrder);
    }

    @Transactional(readOnly = true)
    public List<OrderDto> getClientOrders(String username) {
        if (username == null || username.isBlank()) {
            throw new NotAuthorizedUserException("Имя пользователя не должно быть пустым");
        }

        ShoppingCartDto cart = shoppingCartClient.getShoppingCart(username);

        if (cart == null) {
            log.warn("Корзина для пользователя {} не найдена", username);
            return List.of();
        }

        List<Order> orders = orderRepository.findByShoppingCartIdWithItems(cart.getShoppingCartId())
                .map(List::of)
                .orElse(List.of());

        return orders.stream()
                .map(orderMapper::toDto)
                .collect(Collectors.toList());
    }

    @Transactional
    public OrderDto assembly(UUID orderId) {
        Order order = orderRepository.findByIdWithItems(orderId)
                .orElseThrow(() -> new NoOrderFoundException("Заказ не найден: " + orderId));

        if (order.getState() != OrderState.NEW) {
            throw new IllegalStateException("Заказ не в состоянии NEW: " + order.getState());
        }

        AssemblyRequest assemblyRequest = AssemblyRequest.builder()
                .orderId(orderId)
                .products(order.getItems().stream()
                        .collect(Collectors.toMap(OrderItem::getProductId, OrderItem::getQuantity)))
                .build();

        warehouseClient.assemblyProductForOrder(assemblyRequest);

        order.setState(OrderState.ASSEMBLED);
        order = orderRepository.save(order);

        log.info("Сборка заказа {} начата", orderId);
        return orderMapper.toDto(order);
    }

    @Transactional
    public OrderDto assemblyFailed(UUID orderId) {
        Order order = orderRepository.findById(orderId)
                .orElseThrow(() -> new NoOrderFoundException("Заказ не найден: " + orderId));

        order.setState(OrderState.ASSEMBLY_FAILED);
        order = orderRepository.save(order);

        log.info("Сборка заказа {} провалена", orderId);
        return orderMapper.toDto(order);
    }

    @Transactional
    public OrderDto payment(UUID orderId) {
        Order order = orderRepository.findById(orderId)
                .orElseThrow(() -> new NoOrderFoundException("Заказ не найден: " + orderId));

        if (order.getState() != OrderState.ASSEMBLED) {
            throw new IllegalStateException("Заказ не в состоянии ASSEMBLED: " + order.getState());
        }

        PaymentDto payment = paymentClient.payment(orderMapper.toDto(order));
        order.setPaymentId(payment.getPaymentId());
        order.setState(OrderState.ON_PAYMENT);
        order = orderRepository.save(order);

        log.info("Оплата заказа {} инициирована", orderId);
        return orderMapper.toDto(order);
    }

    @Transactional
    public OrderDto paymentFailed(UUID orderId) {
        Order order = orderRepository.findById(orderId)
                .orElseThrow(() -> new NoOrderFoundException("Заказ не найден: " + orderId));

        order.setState(OrderState.PAYMENT_FAILED);
        order = orderRepository.save(order);

        log.info("Оплата заказа {} провалена", orderId);
        return orderMapper.toDto(order);
    }

    @Transactional
    public OrderDto delivery(UUID orderId) {
        Order order = orderRepository.findById(orderId)
                .orElseThrow(() -> new NoOrderFoundException("Заказ не найден: " + orderId));

        if (order.getState() != OrderState.PAID) {
            throw new IllegalStateException("Заказ не в состоянии PAID: " + order.getState());
        }

        PlanDeliveryRequest planRequest = PlanDeliveryRequest.builder()
                .orderId(orderId)
                .toAddress(order.getToAddress())
                .build();

        DeliveryDto delivery = deliveryClient.planDelivery(planRequest);
        order.setDeliveryId(delivery.getDeliveryId());
        order.setState(OrderState.ON_DELIVERY);
        order = orderRepository.save(order);

        log.info("Доставка заказа {} инициирована", orderId);
        return orderMapper.toDto(order);
    }

    @Transactional
    public OrderDto deliveryFailed(UUID orderId) {
        Order order = orderRepository.findById(orderId)
                .orElseThrow(() -> new NoOrderFoundException("Заказ не найден: " + orderId));

        order.setState(OrderState.DELIVERY_FAILED);
        order = orderRepository.save(order);

        log.info("Доставка заказа {} провалена", orderId);
        return orderMapper.toDto(order);
    }

    @Transactional
    public OrderDto complete(UUID orderId) {
        Order order = orderRepository.findById(orderId)
                .orElseThrow(() -> new NoOrderFoundException("Заказ не найден: " + orderId));

        if (order.getState() != OrderState.DELIVERED) {
            throw new IllegalStateException("Заказ не в состоянии DELIVERED: " + order.getState());
        }

        order.setState(OrderState.COMPLETED);
        order = orderRepository.save(order);

        log.info("Заказ {} завершён", orderId);
        return orderMapper.toDto(order);
    }

    @Transactional
    public OrderDto calculateTotalCost(UUID orderId) {
        Order order = orderRepository.findByIdWithItems(orderId)
                .orElseThrow(() -> new NoOrderFoundException("Заказ не найден: " + orderId));

        BigDecimal totalCost = paymentClient.calculateTotalCost(orderId, order.getDeliveryPrice());
        order.setTotalPrice(totalCost);
        order = orderRepository.save(order);

        log.info("Расчёт общей стоимости заказа {}: {}", orderId, totalCost);
        return orderMapper.toDto(order);
    }

    @Transactional
    public OrderDto calculateDeliveryCost(UUID orderId) {
        Order order = orderRepository.findByIdWithItems(orderId)
                .orElseThrow(() -> new NoOrderFoundException("Заказ не найден: " + orderId));

        BigDecimal deliveryCost = deliveryClient.calculateDeliveryCost(orderId);
        order.setDeliveryPrice(deliveryCost);
        order = orderRepository.save(order);

        log.info("Расчёт стоимости доставки заказа {}: {}", orderId, deliveryCost);
        return orderMapper.toDto(order);
    }

    @Transactional
    public OrderDto productReturn(ProductReturnRequest request) {
        Order order = orderRepository.findById(request.getOrderId())
                .orElseThrow(() -> new NoOrderFoundException("Заказ не найден: " + request.getOrderId()));

        warehouseClient.acceptReturn(request.getProducts());

        order.setState(OrderState.PRODUCT_RETURNED);
        order = orderRepository.save(order);

        log.info("Возврат заказа {}", request.getOrderId());
        return orderMapper.toDto(order);
    }
}