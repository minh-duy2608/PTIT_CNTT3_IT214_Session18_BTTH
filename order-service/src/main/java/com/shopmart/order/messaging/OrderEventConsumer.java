package com.shopmart.order.messaging;

import com.shopmart.order.event.KafkaTopics;
import com.shopmart.order.event.OrderEvent;
import com.shopmart.order.service.OrderService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Câu 3: consumer phía order-service trong Choreography Saga.
 * - PAYMENT_COMPLETED -> đánh dấu đơn COMPLETED
 * - INVENTORY_FAILED  -> đơn CANCELLED (không cần compensate vì chưa trừ được kho)
 * - PAYMENT_FAILED    -> đơn CANCELLED (inventory-service sẽ tự hoàn kho -> INVENTORY_RELEASED)
 * - INVENTORY_RELEASED-> log kết thúc hoàn tác
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class OrderEventConsumer {

    private final OrderService orderService;

    @KafkaListener(topics = KafkaTopics.ORDER, groupId = "order-service-group")
    public void onOrderEvent(OrderEvent event) {
        log.info("[SAGA][order-service] Nhận sự kiện: {}", event);

        if (event.getType() == null) {
            return;
        }

        switch (event.getType()) {
            case PAYMENT_COMPLETED -> orderService.completeOrder(event.getOrderId());
            case INVENTORY_FAILED -> orderService.cancelOrder(event.getOrderId(),
                    "Trừ tồn kho thất bại: " + event.getMessage());
            case PAYMENT_FAILED -> orderService.cancelOrder(event.getOrderId(),
                    "Thanh toán thất bại: " + event.getMessage());
            case INVENTORY_RELEASED -> log.warn(
                    "[SAGA] Đã hoàn tồn kho cho đơn {} -> đơn ở trạng thái CANCELLED", event.getOrderId());
            default -> log.debug("[SAGA] Bỏ qua sự kiện {} của đơn {}", event.getType(), event.getOrderId());
        }
    }
}
