package com.shopmart.order.controller;

import com.shopmart.order.dto.OrderRequest;
import com.shopmart.order.dto.OrderResponse;
import com.shopmart.order.dto.ProductDto;
import com.shopmart.order.dto.StockRequest;
import com.shopmart.order.service.OrderService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/order")
@RequiredArgsConstructor
public class OrderController {

    private final OrderService orderService;

    @PostMapping
    public ResponseEntity<OrderResponse> create(@Valid @RequestBody OrderRequest request) {
        return ResponseEntity.status(HttpStatus.CREATED).body(orderService.createOrder(request));
    }

    @GetMapping("/{id}")
    public OrderResponse getById(@PathVariable Long id) {
        return orderService.getOrderById(id);
    }

    @GetMapping
    public List<OrderResponse> getAll() {
        return orderService.getAllOrders();
    }

    /**
     * Câu 2: endpoint gọi thử inventory-service qua FeignClient.
     * Gọi nhiều lần để thấy LoadBalancer luân phiên instance (xem log /api/inventory/instance).
     * Dừng inventory-service để thấy Circuit Breaker + fallback hoạt động (trả về 204).
     */
    @GetMapping("/products/{id}")
    public ResponseEntity<ProductDto> getProduct(@PathVariable Long id) {
        ProductDto product = orderService.getProduct(id);
        return product == null ? ResponseEntity.noContent().build() : ResponseEntity.ok(product);
    }

    /** Câu 2: gọi thử trừ tồn kho qua FeignClient (minh chứng phương thức ghi của Feign). */
    @PutMapping("/products/{id}/decrease")
    public ResponseEntity<ProductDto> decreaseStock(@PathVariable Long id, @Valid @RequestBody StockRequest request) {
        ProductDto product = orderService.decreaseStock(id, request.getQuantity());
        return product == null ? ResponseEntity.noContent().build() : ResponseEntity.ok(product);
    }
}
