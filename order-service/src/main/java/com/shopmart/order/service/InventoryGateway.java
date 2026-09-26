package com.shopmart.order.service;

import com.shopmart.order.client.InventoryClient;
import com.shopmart.order.dto.ProductDto;
import com.shopmart.order.dto.StockRequest;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

/**
 * Câu 2: lớp bọc quanh FeignClient, thêm Circuit Breaker (Resilience4j) + fallback
 * để order-service không bị "gãy" khi inventory-service chết.
 *
 * Vì sao tách riêng khỏi OrderServiceImpl? @CircuitBreaker hoạt động qua proxy AOP,
 * nếu để cùng class rồi tự gọi nội bộ (self-invocation) thì annotation sẽ bị bỏ qua.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class InventoryGateway {

    private final InventoryClient inventoryClient;

    @CircuitBreaker(name = "inventoryService", fallbackMethod = "getProductFallback")
    public ProductDto getProduct(Long productId) {
        log.info("[Feign] GET inventory-service /products/{}", productId);
        return inventoryClient.getProduct(productId);
    }

    @CircuitBreaker(name = "inventoryService", fallbackMethod = "decreaseStockFallback")
    public ProductDto decreaseStock(Long productId, int quantity) {
        log.info("[Feign] PUT inventory-service /products/{}/decrease qty={}", productId, quantity);
        return inventoryClient.decreaseStock(productId, new StockRequest(quantity));
    }

    /**
     * Fallback khi không gọi được inventory-service (mạch hở / timeout / lỗi mạng).
     * Trả về null để phía gọi biết bước này thất bại thay vì ném exception ra ngoài.
     */
    private ProductDto getProductFallback(Long productId, Throwable t) {
        log.error("[CircuitBreaker] inventory-service KHÔNG khả dụng khi lấy product id={}: {}",
                productId, t.getMessage());
        return null;
    }

    private ProductDto decreaseStockFallback(Long productId, int quantity, Throwable t) {
        log.error("[CircuitBreaker] inventory-service KHÔNG khả dụng khi trừ kho id={}: {}",
                productId, t.getMessage());
        return null;
    }
}
