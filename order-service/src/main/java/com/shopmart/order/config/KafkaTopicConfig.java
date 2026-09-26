package com.shopmart.order.config;

import com.shopmart.order.event.KafkaTopics;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Câu 3: khai báo topic "order" để broker (KRaft) tự tạo khi ứng dụng khởi động.
 * 3 partition cho phép nhiều consumer chạy song song trong cùng group.
 */
@Configuration
public class KafkaTopicConfig {

    @Bean
    public NewTopic orderTopic() {
        return TopicBuilder.name(KafkaTopics.ORDER)
                .partitions(3)
                .replicas(1)
                .build();
    }
}
