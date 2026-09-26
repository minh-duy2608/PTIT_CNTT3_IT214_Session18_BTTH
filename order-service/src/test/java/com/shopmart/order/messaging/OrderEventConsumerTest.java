package com.shopmart.order.messaging;

import com.shopmart.order.event.OrderEvent;
import com.shopmart.order.event.SagaEventType;
import com.shopmart.order.service.OrderService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/**
 * Câu 5: test kịch bản ROLLBACK của Saga.
 * Khi nhận PAYMENT_FAILED -> order-service phải huỷ đơn (CANCELLED), không được COMPLETED.
 */
@ExtendWith(MockitoExtension.class)
class OrderEventConsumerTest {

    @Mock
    private OrderService orderService;

    @InjectMocks
    private OrderEventConsumer consumer;

    @Test
    void onPaymentFailed_shouldCancelOrder() {
        OrderEvent event = OrderEvent.builder()
                .orderId(1L)
                .productId(3L)
                .quantity(3)
                .type(SagaEventType.PAYMENT_FAILED)
                .message("Số tiền vượt hạn mức")
                .build();

        consumer.onOrderEvent(event);

        verify(orderService).cancelOrder(eq(1L), contains("Thanh toán thất bại"));
        verify(orderService, never()).completeOrder(1L);
    }

    @Test
    void onInventoryFailed_shouldCancelOrder() {
        OrderEvent event = OrderEvent.builder()
                .orderId(2L)
                .productId(5L)
                .quantity(99)
                .type(SagaEventType.INVENTORY_FAILED)
                .message("không đủ tồn kho")
                .build();

        consumer.onOrderEvent(event);

        verify(orderService).cancelOrder(eq(2L), contains("Trừ tồn kho thất bại"));
    }

    @Test
    void onPaymentCompleted_shouldCompleteOrder() {
        OrderEvent event = OrderEvent.builder()
                .orderId(3L)
                .type(SagaEventType.PAYMENT_COMPLETED)
                .build();

        consumer.onOrderEvent(event);

        verify(orderService).completeOrder(3L);
        verify(orderService, never()).cancelOrder(eq(3L), org.mockito.ArgumentMatchers.anyString());
    }
}
