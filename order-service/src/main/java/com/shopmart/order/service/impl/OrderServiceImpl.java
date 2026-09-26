package com.shopmart.order.service.impl;

import com.shopmart.order.dto.OrderRequest;
import com.shopmart.order.dto.OrderResponse;
import com.shopmart.order.dto.ProductDto;
import com.shopmart.order.entity.Order;
import com.shopmart.order.entity.OrderStatus;
import com.shopmart.order.event.KafkaTopics;
import com.shopmart.order.event.OrderEvent;
import com.shopmart.order.event.SagaEventType;
import com.shopmart.order.exception.ResourceNotFoundException;
import com.shopmart.order.repository.OrderRepository;
import com.shopmart.order.service.InventoryGateway;
import com.shopmart.order.service.OrderService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class OrderServiceImpl implements OrderService {

    private final OrderRepository orderRepository;
    private final InventoryGateway inventoryGateway;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Override
    @Transactional
    public OrderResponse createOrder(OrderRequest request) {
        // Câu 2: gọi inventory-service qua Feign (có Circuit Breaker + fallback) để lấy giá sản phẩm.
        ProductDto product = inventoryGateway.getProduct(request.getProductId());

        if (product == null) {
            // inventory-service không khả dụng -> không thể định giá, huỷ đơn để tránh dữ liệu sai lệch.
            Order rejected = Order.builder()
                    .customerId(request.getCustomerId())
                    .productId(request.getProductId())
                    .quantity(request.getQuantity())
                    .status(OrderStatus.CANCELLED)
                    .failureReason("Không lấy được thông tin sản phẩm từ inventory-service")
                    .build();
            log.error("Order rejected: inventory-service unavailable for productId={}", request.getProductId());
            return OrderResponse.from(orderRepository.save(rejected));
        }

        BigDecimal totalAmount = product.getPrice().multiply(BigDecimal.valueOf(request.getQuantity()));

        Order order = Order.builder()
                .customerId(request.getCustomerId())
                .productId(request.getProductId())
                .quantity(request.getQuantity())
                .totalAmount(totalAmount)
                .status(OrderStatus.PENDING)
                .build();
        Order saved = orderRepository.save(order);
        log.info("Created order id={} productId={} qty={} totalAmount={} status=PENDING",
                saved.getId(), saved.getProductId(), saved.getQuantity(), saved.getTotalAmount());

        // Câu 3: phát sự kiện khởi động Choreography Saga.
        publish(saved, SagaEventType.ORDER_CREATED, "Đơn hàng mới được tạo");

        return OrderResponse.from(saved);
    }

    @Override
    public OrderResponse getOrderById(Long id) {
        return OrderResponse.from(findOrder(id));
    }

    @Override
    public List<OrderResponse> getAllOrders() {
        return orderRepository.findAll().stream()
                .map(OrderResponse::from)
                .toList();
    }

    @Override
    @Transactional
    public OrderResponse completeOrder(Long orderId) {
        Order order = findOrder(orderId);
        order.setStatus(OrderStatus.COMPLETED);
        order.setFailureReason(null);
        log.info("[SAGA] Order id={} COMPLETED (tồn kho đã trừ, thanh toán thành công)", orderId);
        return OrderResponse.from(orderRepository.save(order));
    }

    @Override
    @Transactional
    public OrderResponse cancelOrder(Long orderId, String reason) {
        Order order = findOrder(orderId);
        order.setStatus(OrderStatus.CANCELLED);
        order.setFailureReason(reason);
        log.error("[SAGA] Order id={} CANCELLED: {}", orderId, reason);
        return OrderResponse.from(orderRepository.save(order));
    }

    @Override
    public ProductDto getProduct(Long productId) {
        return inventoryGateway.getProduct(productId);
    }

    @Override
    public ProductDto decreaseStock(Long productId, int quantity) {
        return inventoryGateway.decreaseStock(productId, quantity);
    }

    private void publish(Order order, SagaEventType type, String message) {
        OrderEvent event = OrderEvent.builder()
                .orderId(order.getId())
                .productId(order.getProductId())
                .quantity(order.getQuantity())
                .amount(order.getTotalAmount())
                .type(type)
                .message(message)
                .build();
        kafkaTemplate.send(KafkaTopics.ORDER, String.valueOf(order.getId()), event);
        log.info("[SAGA] Published {} for orderId={} to topic '{}'", type, order.getId(), KafkaTopics.ORDER);
    }

    private Order findOrder(Long id) {
        return orderRepository.findById(id)
                .orElseThrow(() -> new ResourceNotFoundException("Không tìm thấy đơn hàng id=" + id));
    }
}
