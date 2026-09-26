package com.shopmart.inventory.messaging;

import com.shopmart.inventory.event.KafkaTopics;
import com.shopmart.inventory.event.OrderEvent;
import com.shopmart.inventory.event.SagaEventType;
import com.shopmart.inventory.service.ProductService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * Câu 3: consumer phía inventory-service trong Choreography Saga.
 * - ORDER_CREATED -> trừ tồn kho; thành công phát INVENTORY_RESERVED, thất bại phát INVENTORY_FAILED
 * - PAYMENT_FAILED -> COMPENSATING: hoàn tồn kho rồi phát INVENTORY_RELEASED
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class OrderEventConsumer {

    private final ProductService productService;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    @KafkaListener(topics = KafkaTopics.ORDER, groupId = "inventory-service-group")
    public void onOrderEvent(OrderEvent event) {
        log.info("[SAGA][inventory-service] Nhận sự kiện: {}", event);

        if (event.getType() == null) {
            return;
        }

        switch (event.getType()) {
            case ORDER_CREATED -> reserveStock(event);
            case PAYMENT_FAILED -> releaseStock(event);
            default -> log.debug("[SAGA] Bỏ qua sự kiện {} của đơn {}", event.getType(), event.getOrderId());
        }
    }

    /** Bước 1 của Saga: trừ tồn kho. */
    private void reserveStock(OrderEvent event) {
        try {
            productService.decreaseStock(event.getProductId(), event.getQuantity());
            log.info("[SAGA] INVENTORY_RESERVED cho đơn {} (product={}, qty={})",
                    event.getOrderId(), event.getProductId(), event.getQuantity());
            publish(event, SagaEventType.INVENTORY_RESERVED, "Đã trừ tồn kho");
        } catch (Exception ex) {
            log.error("[SAGA] INVENTORY_FAILED cho đơn {}: {}", event.getOrderId(), ex.getMessage());
            publish(event, SagaEventType.INVENTORY_FAILED, ex.getMessage());
        }
    }

    /** Compensating transaction: hoàn tồn kho khi thanh toán thất bại. */
    private void releaseStock(OrderEvent event) {
        try {
            productService.increaseStock(event.getProductId(), event.getQuantity());
            log.warn("[SAGA][COMPENSATE] Đã hoàn tồn kho cho đơn {} (product={}, qty={})",
                    event.getOrderId(), event.getProductId(), event.getQuantity());
            publish(event, SagaEventType.INVENTORY_RELEASED, "Đã hoàn tồn kho");
        } catch (Exception ex) {
            log.error("[SAGA][COMPENSATE] Hoàn tồn kho thất bại cho đơn {}: {}",
                    event.getOrderId(), ex.getMessage());
        }
    }

    private void publish(OrderEvent source, SagaEventType type, String message) {
        OrderEvent out = OrderEvent.builder()
                .orderId(source.getOrderId())
                .productId(source.getProductId())
                .quantity(source.getQuantity())
                .amount(source.getAmount())
                .type(type)
                .message(message)
                .build();
        kafkaTemplate.send(KafkaTopics.ORDER, String.valueOf(out.getOrderId()), out);
    }
}
