package com.shopmart.order.service;

import com.shopmart.order.dto.OrderRequest;
import com.shopmart.order.dto.OrderResponse;
import com.shopmart.order.dto.ProductDto;

import java.util.List;

public interface OrderService {

    /** Tạo đơn hàng ở trạng thái PENDING và khởi động Saga. */
    OrderResponse createOrder(OrderRequest request);

    OrderResponse getOrderById(Long id);

    List<OrderResponse> getAllOrders();

    /** Saga kết thúc thành công -> COMPLETED. */
    OrderResponse completeOrder(Long orderId);

    /** Saga thất bại (đã compensate) -> CANCELLED kèm lý do. */
    OrderResponse cancelOrder(Long orderId, String reason);

    /** Câu 2: gọi thử inventory-service qua Feign + Circuit Breaker (minh chứng Load Balancing). */
    ProductDto getProduct(Long productId);

    /** Câu 2: gọi thử trừ tồn kho qua Feign + Circuit Breaker. */
    ProductDto decreaseStock(Long productId, int quantity);
}
