package com.shopmart.gateway;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;

/**
 * Câu 1: API Gateway - cổng vào duy nhất cho toàn hệ thống.
 * Routes được khai báo trong config-repo/api-gateway.yml và trỏ tới
 * order-service / inventory-service / payment-service qua lb://<service-name>.
 */
@SpringBootApplication
@EnableDiscoveryClient
public class ApiGatewayApplication {

    public static void main(String[] args) {
        SpringApplication.run(ApiGatewayApplication.class, args);
    }
}
