# ShopMart – Base Project (Java Microservice – Session 14)

Base project cho bài kiểm tra **"Nâng cấp phân hệ đặt hàng thành giao dịch phân tán (Saga Pattern)"**.

> Sau khi clone: **xoá thư mục `.git`**, sau đó `git init` và đẩy lên repository của bạn theo cú pháp
> `[Tên lớp]_[Họ Tên]` (ví dụ: `HN-K24-CNTT1_NguyenVanA`).

## 1. Công nghệ

| Thành phần | Phiên bản |
|---|---|
| Java | 17+ |
| Spring Boot | 3.3.5 |
| Spring Cloud | 2023.0.3 (BOM đã khai báo sẵn trong `pom.xml` gốc – thêm dependency **không cần ghi version**) |
| MySQL | 8.x (Docker Compose) |
| Kafka / Redis | sinh viên tự bổ sung vào `docker-compose.yml` |

## 2. Cấu trúc project

```
Base-Project
├── pom.xml                  # Parent POM (multi-module)
├── docker-compose.yml       # MySQL (đã có) + TODO Kafka/Zookeeper/Redis
├── config-repo/             # Nơi lưu file cấu hình cho Config Server (native)
├── config-server/     :8888 # [SKELETON] Câu 1
├── eureka-server/     :8761 # [SKELETON] Câu 1
├── api-gateway/       :8080 # [SKELETON] Câu 1
├── order-service/     :8081 # [ĐÃ CHẠY ĐƯỢC] quản lý đơn hàng
├── inventory-service/ :8082 # [ĐÃ CHẠY ĐƯỢC] quản lý tồn kho (có dữ liệu mẫu)
└── payment-service/   :8083 # [ĐÃ CHẠY ĐƯỢC] xử lý thanh toán (có giả lập lỗi)
```

Mỗi business service có cấu trúc package chuẩn:

```
com.shopmart.<service>
├── controller    # REST API
├── service       # interface + impl (nghiệp vụ, log SLF4J)
├── repository    # Spring Data JPA
├── entity        # JPA entity
├── dto           # request/response
├── event         # OrderEvent, SagaEventType, KafkaTopics (đã có sẵn cho Câu 3)
└── exception     # GlobalExceptionHandler
```

## 3. Những gì đã có sẵn

### inventory-service (`/api/inventory`)
| Method | Endpoint | Mô tả |
|---|---|---|
| GET | `/api/inventory/instance` | Trả về port của instance (minh chứng Load Balancing) |
| GET | `/api/inventory/products` | Danh sách sản phẩm |
| GET | `/api/inventory/products/{id}` | Chi tiết sản phẩm (có log `Querying DB...` để kiểm tra cache) |
| POST | `/api/inventory/products` | Tạo sản phẩm |
| PUT | `/api/inventory/products/{id}` | Cập nhật sản phẩm |
| DELETE | `/api/inventory/products/{id}` | Xoá sản phẩm |
| PUT | `/api/inventory/products/{id}/decrease` | Trừ tồn kho – body `{"quantity": 2}` |
| PUT | `/api/inventory/products/{id}/increase` | Hoàn tồn kho (compensate) – body `{"quantity": 2}` |

Dữ liệu mẫu (`data.sql`): 5 sản phẩm, id 1 → 5 (sản phẩm id=3 MacBook giá 28.000.000).

### payment-service (`/api/payment`)
| Method | Endpoint | Mô tả |
|---|---|---|
| POST | `/api/payment` | Thanh toán – body `{"orderId": 1, "amount": 50000000}` → `SUCCESS` (201) hoặc `FAILED` (402) |
| POST | `/api/payment/{orderId}/refund` | Hoàn tiền (compensate) |
| GET | `/api/payment/{orderId}` | Tra cứu thanh toán theo đơn |
| GET | `/api/payment` | Danh sách thanh toán |

**Giả lập lỗi thanh toán** (dùng để chứng minh rollback ở Câu 3):
- `payment.simulate-failure=true` → mọi giao dịch đều `FAILED`
- Số tiền > `payment.max-amount` (mặc định 80.000.000) → `FAILED`
  (ví dụ: đặt 3 chiếc MacBook id=3 = 84.000.000)

### order-service (`/api/order`)
| Method | Endpoint | Mô tả |
|---|---|---|
| POST | `/api/order` | Tạo đơn – body `{"customerId": "C001", "productId": 1, "quantity": 2}` |
| GET | `/api/order/{id}` | Chi tiết đơn |
| GET | `/api/order` | Danh sách đơn |

Hiện tại `createOrder` **chỉ lưu đơn ở trạng thái `PENDING`** (chưa gọi service khác).
`OrderService` đã có sẵn `completeOrder(...)` và `cancelOrder(...)` để dùng khi Saga kết thúc.

Trạng thái đơn: `PENDING` → `COMPLETED` | `CANCELLED`.

### Sự kiện Saga (package `event`, giống nhau ở cả 3 service)
- `KafkaTopics.ORDER = "order"`
- `OrderEvent { orderId, productId, quantity, amount, type, message }`
- `SagaEventType`: `ORDER_CREATED`, `INVENTORY_RESERVED`, `INVENTORY_FAILED`, `PAYMENT_COMPLETED`, `PAYMENT_FAILED`, `INVENTORY_RELEASED`

Sinh viên được phép thay đổi/bổ sung các class này nếu thiết kế Saga theo cách khác.

## 4. Chạy thử base project

```bash
# 1. Khởi động MySQL
docker compose up -d

# 2. Build toàn bộ
mvn clean install

# 3. Chạy từng service (hoặc Run trong IntelliJ)
mvn -pl inventory-service spring-boot:run
mvn -pl payment-service spring-boot:run
mvn -pl order-service spring-boot:run
```

Postman collection mẫu: `postman/ShopMart.postman_collection.json`.

## 5. Nhiệm vụ của sinh viên

Tìm các comment `TODO Câu x` trong project (IntelliJ: **View → Tool Windows → TODO**).

| Câu | Việc cần làm | Vị trí gợi ý |
|---|---|---|
| 1 | Config Server, Eureka Server, API Gateway; các service nạp cấu hình từ Config Server và đăng ký Eureka | `config-server`, `eureka-server`, `api-gateway`, `config-repo`, `*/application.yml`, `*/pom.xml` |
| 2 | FeignClient `inventory-service` (lấy sản phẩm, trừ tồn kho) + LoadBalancer + Resilience4j `@CircuitBreaker` + fallback; chạy 2 instance inventory-service | `order-service` |
| 3 | Kafka (zookeeper + kafka), topic `order`, producer/consumer; Saga (Choreography hoặc Orchestration) + compensating; chứng minh rollback khi thanh toán lỗi; (nâng cao) consumer reactive | `docker-compose.yml`, cả 3 service |
| 4 | Redis + `@Cacheable` / `@CachePut` / `@CacheEvict` cho sản phẩm | `inventory-service` |
| 5 | Clean code, không hard-code cấu hình, log SLF4J, unit test + ít nhất 1 test rollback | toàn project |

> Gợi ý chạy 2 instance inventory-service: IntelliJ → Edit Configurations → Copy configuration →
> thêm VM option `-Dserver.port=8084`.

## 6. Những gì đã hoàn thành (Câu 1 – 4) và cách minh chứng

### Thứ tự khởi động (quan trọng)
```
docker compose up -d                 # MySQL + Kafka(KRaft) + Redis
mvn clean install                    # build toàn bộ
# 1) config-server  (bắt buộc chạy TRƯỚC - các service nạp cấu hình từ đây)
mvn -pl config-server   spring-boot:run
# 2) eureka-server
mvn -pl eureka-server   spring-boot:run
# 3) gateway + các service (chạy song song ở các terminal khác nhau)
mvn -pl api-gateway       spring-boot:run
mvn -pl inventory-service spring-boot:run
mvn -pl payment-service   spring-boot:run
mvn -pl order-service     spring-boot:run
# 4) instance thứ 2 của inventory-service (minh chứng Load Balancing)
mvn -pl inventory-service spring-boot:run -Dspring-boot.run.jvmArguments="-Dserver.port=8084"
```

### Câu 1 – Config Server, Eureka, Gateway
- Config Server: `@EnableConfigServer`, native profile đọc `config-repo/` → kiểm tra
  `curl http://localhost:8888/order-service/default` (thấy cấu hình ghép `application.yml` + `order-service.yml`).
- Eureka dashboard: http://localhost:8761 — thấy `ORDER-SERVICE`, `INVENTORY-SERVICE` (2 instance),
  `PAYMENT-SERVICE`, `API-GATEWAY` đã đăng ký.
- Gateway: mọi request đi qua cổng 8080:
  - `GET http://localhost:8080/api/order` → order-service
  - `GET http://localhost:8080/api/inventory/products` → inventory-service
  - `GET http://localhost:8080/api/payment` → payment-service

### Câu 2 – FeignClient + Circuit Breaker + LoadBalancing
- `InventoryClient` (`@FeignClient(name = "inventory-service")`) gọi theo tên qua Eureka/LoadBalancer.
- `InventoryGateway` bọc Feign bằng `@CircuitBreaker(name = "inventoryService", fallbackMethod = ...)`.
- Minh chứng LB: gọi `GET http://localhost:8080/api/inventory/instance` nhiều lần → port trả về luân phiên
  `8082` / `8084`.
- Minh chứng Circuit Breaker: tắt hết inventory-service rồi gọi
  `GET http://localhost:8080/api/order/products/1` → trả về `204 No Content`
  và log `[CircuitBreaker] inventory-service KHÔNG khả dụng...`. Xem actuator: `GET http://localhost:8081/actuator/circuitbreakers`.

### Câu 3 – Saga (Choreography) + Kafka
- Hạ tầng: `apache/kafka:3.7.1` chạy KRaft (không Zookeeper), topic `order` (3 partition) tự tạo bởi `KafkaTopicConfig`.
- Luồng: `ORDER_CREATED` → inventory `INVENTORY_RESERVED`/`INVENTORY_FAILED`
  → payment `PAYMENT_COMPLETED`/`PAYMENT_FAILED`
  → nếu `PAYMENT_FAILED`: inventory hoàn kho (`INVENTORY_RELEASED`) + order chuyển `CANCELLED`.
- **Minh chứng rollback**: bật `payment.simulate-failure=true` (trong `config-repo/payment-service.yml`)
  hoặc đặt 3 chiếc MacBook (id=3, 84.000.000 > max-amount) rồi `POST /api/order`.
  → Kiểm tra `GET /api/order/{id}` = `CANCELLED` và `GET /api/inventory/products/3` = tồn kho **không đổi**
  (log SLF4J có `[SAGA][COMPENSATE] Đã hoàn tồn kho...`).
- Nâng cao: `OrderEventReactiveConsumer` (reactor-kafka + WebFlux) lắng nghe reactive, log
  `[REACTIVE][ASYNC] ...` (group `order-reactive-audit-group`).

### Câu 4 – Distributed Caching (Redis, Cache-Aside)
- `@Cacheable` (`getProductById`), `@CachePut` (create/update/decrease/increase), `@CacheEvict` (delete),
  cache name `products`, TTL 60s (cấu hình trong `config-repo/inventory-service.yml`).
- **Minh chứng**: gọi `GET http://localhost:8080/api/inventory/products/1`
  - Lần 1: log `[CACHE MISS] Querying DB for product id=1`
  - Lần 2, 3: **không** còn log `Querying DB` (dữ liệu lấy từ Redis).
  Kiểm tra Redis: `docker exec -it shopmart-redis redis-cli keys 'products*'`.

### Kiểm thử
`mvn clean install` chạy unit test cho cả 3 service (13 test), trong đó `OrderEventConsumerTest`
kiểm tra kịch bản rollback (`PAYMENT_FAILED` → đơn `CANCELLED`, không `COMPLETED`).

