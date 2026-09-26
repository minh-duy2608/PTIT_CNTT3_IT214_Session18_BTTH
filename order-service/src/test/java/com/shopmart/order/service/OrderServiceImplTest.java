package com.shopmart.order.service;

import com.shopmart.order.dto.OrderRequest;
import com.shopmart.order.dto.OrderResponse;
import com.shopmart.order.dto.ProductDto;
import com.shopmart.order.entity.Order;
import com.shopmart.order.entity.OrderStatus;
import com.shopmart.order.event.KafkaTopics;
import com.shopmart.order.event.OrderEvent;
import com.shopmart.order.repository.OrderRepository;
import com.shopmart.order.service.impl.OrderServiceImpl;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;

import java.math.BigDecimal;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OrderServiceImplTest {

    @Mock
    private OrderRepository orderRepository;

    @Mock
    private InventoryGateway inventoryGateway;

    @Mock
    private KafkaTemplate<String, Object> kafkaTemplate;

    @InjectMocks
    private OrderServiceImpl orderService;

    @Test
    void createOrder_shouldSaveWithPendingStatusAndComputeAmount() {
        when(inventoryGateway.getProduct(1L)).thenReturn(
                ProductDto.builder().id(1L).name("iPhone 15 Pro")
                        .price(new BigDecimal("25000000")).stock(50).build());
        when(orderRepository.save(any(Order.class))).thenAnswer(inv -> {
            Order order = inv.getArgument(0);
            order.setId(1L);
            return order;
        });

        OrderResponse result = orderService.createOrder(new OrderRequest("C001", 1L, 2));

        assertThat(result.getId()).isEqualTo(1L);
        assertThat(result.getStatus()).isEqualTo(OrderStatus.PENDING);
        assertThat(result.getTotalAmount()).isEqualByComparingTo("50000000");
    }

    @Test
    void createOrder_shouldPublishOrderCreatedEvent() {
        when(inventoryGateway.getProduct(1L)).thenReturn(
                ProductDto.builder().id(1L).price(new BigDecimal("25000000")).stock(50).build());
        when(orderRepository.save(any(Order.class))).thenAnswer(inv -> {
            Order order = inv.getArgument(0);
            order.setId(7L);
            return order;
        });

        orderService.createOrder(new OrderRequest("C001", 1L, 1));

        // Câu 3: sau khi tạo đơn phải phát sự kiện ORDER_CREATED để khởi động Saga.
        verify(kafkaTemplate).send(eq(KafkaTopics.ORDER), eq("7"), any(OrderEvent.class));
    }

    @Test
    void createOrder_shouldCancelOrder_whenInventoryUnavailable() {
        // Circuit Breaker fallback trả về null -> đơn bị huỷ, không phát Saga.
        when(inventoryGateway.getProduct(1L)).thenReturn(null);
        when(orderRepository.save(any(Order.class))).thenAnswer(inv -> inv.getArgument(0));

        OrderResponse result = orderService.createOrder(new OrderRequest("C001", 1L, 2));

        assertThat(result.getStatus()).isEqualTo(OrderStatus.CANCELLED);
    }

    @Test
    void cancelOrder_shouldSetCancelledWithReason() {
        Order order = Order.builder().id(1L).status(OrderStatus.PENDING).build();
        when(orderRepository.findById(1L)).thenReturn(Optional.of(order));
        when(orderRepository.save(any(Order.class))).thenAnswer(inv -> inv.getArgument(0));

        OrderResponse result = orderService.cancelOrder(1L, "Payment failed");

        assertThat(result.getStatus()).isEqualTo(OrderStatus.CANCELLED);
        assertThat(result.getFailureReason()).isEqualTo("Payment failed");
    }
}
