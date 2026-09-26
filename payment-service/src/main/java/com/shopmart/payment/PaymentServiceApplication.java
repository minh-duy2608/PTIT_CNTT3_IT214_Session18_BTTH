package com.shopmart.payment;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;

/**
 * Câu 1: đăng ký service vào Eureka (@EnableDiscoveryClient).
 * Câu 3: tham gia Saga với vai trò consumer (INVENTORY_RESERVED) / producer (PAYMENT_*).
 */
@SpringBootApplication
@EnableDiscoveryClient
@ConfigurationPropertiesScan
public class PaymentServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(PaymentServiceApplication.class, args);
    }
}
