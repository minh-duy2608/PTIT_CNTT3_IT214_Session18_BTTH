package com.shopmart.payment.messaging;

import com.shopmart.payment.dto.PaymentRequest;
import com.shopmart.payment.dto.PaymentResponse;
import com.shopmart.payment.entity.PaymentStatus;
import com.shopmart.payment.event.KafkaTopics;
import com.shopmart.payment.event.OrderEvent;
import com.shopmart.payment.event.SagaEventType;
import com.shopmart.payment.service.PaymentService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * Câu 3: consumer phía payment-service trong Choreography Saga.
 * Chỉ xử lý khi nhận INVENTORY_RESERVED (kho đã trừ xong) -> thanh toán.
 * Thành công phát PAYMENT_COMPLETED, thất bại phát PAYMENT_FAILED
 * (inventory-service nghe PAYMENT_FAILED sẽ hoàn kho).
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class OrderEventConsumer {

    private final PaymentService paymentService;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    @KafkaListener(topics = KafkaTopics.ORDER, groupId = "payment-service-group")
    public void onOrderEvent(OrderEvent event) {
        log.info("[SAGA][payment-service] Nhận sự kiện: {}", event);

        if (event.getType() != SagaEventType.INVENTORY_RESERVED) {
            log.debug("[SAGA] Bỏ qua sự kiện {} của đơn {}", event.getType(), event.getOrderId());
            return;
        }

        PaymentResponse response = paymentService.processPayment(
                new PaymentRequest(event.getOrderId(), event.getAmount()));

        if (response.getStatus() == PaymentStatus.SUCCESS) {
            log.info("[SAGA] PAYMENT_COMPLETED cho đơn {}", event.getOrderId());
            publish(event, SagaEventType.PAYMENT_COMPLETED, "Thanh toán thành công");
        } else {
            log.error("[SAGA] PAYMENT_FAILED cho đơn {}: {}", event.getOrderId(), response.getMessage());
            publish(event, SagaEventType.PAYMENT_FAILED, response.getMessage());
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
