package com.shopmart.inventory.messaging;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.shopmart.inventory.event.KafkaTopics;
import com.shopmart.inventory.event.OrderEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;
import reactor.core.Disposable;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Câu 3 (nâng cao - WebFlux): consumer Kafka theo kiểu REACTIVE (reactor-kafka).
 *
 * Khác với @KafkaListener (blocking, 1 thread xử lý tuần tự), consumer này nhận
 * luồng sự kiện dưới dạng Flux và xử lý bất đồng bộ bằng backpressure - minh chứng
 * tư duy Async / Event-driven. Đây là consumer "audit" chạy group riêng nên không
 * ảnh hưởng nghiệp vụ của OrderEventConsumer.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class OrderEventReactiveConsumer implements SmartLifecycle {

    private static final String GROUP_ID = "order-reactive-audit-group";

    private final ObjectMapper objectMapper;

    @Value("${spring.kafka.bootstrap-servers:localhost:9092}")
    private String bootstrapServers;

    private Disposable subscription;

    @Override
    public void start() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        ReceiverOptions<String, String> options = ReceiverOptions.<String, String>create(props)
                .subscription(Collections.singleton(KafkaTopics.ORDER));

        subscription = KafkaReceiver.create(options)
                .receiveAutoAck()
                // receiveAutoAck() trả về Flux<Flux<ConsumerRecord>> (mỗi inner Flux là 1 batch của 1 partition)
                // -> concatMap để trải phẳng thành luồng ConsumerRecord bất đồng bộ
                .concatMap(batch -> batch)
                .doOnSubscribe(s -> log.info("[REACTIVE] Bắt đầu lắng nghe topic '{}' (group={})",
                        KafkaTopics.ORDER, GROUP_ID))
                .doOnNext(record -> {
                    try {
                        OrderEvent event = objectMapper.readValue(record.value(), OrderEvent.class);
                        log.info("[REACTIVE][ASYNC] orderId={} type={} partition={} offset={}",
                                event.getOrderId(), event.getType(),
                                record.partition(), record.offset());
                    } catch (Exception ex) {
                        log.warn("[REACTIVE] Không parse được message: {}", record.value(), ex);
                    }
                })
                .doOnError(ex -> log.error("[REACTIVE] Lỗi luồng Kafka", ex))
                .subscribe();
    }

    @Override
    public void stop() {
        if (subscription != null && !subscription.isDisposed()) {
            subscription.dispose();
            log.info("[REACTIVE] Đã dừng consumer reactive");
        }
    }

    @Override
    public boolean isRunning() {
        return subscription != null && !subscription.isDisposed();
    }

    @Override
    public int getPhase() {
        // Khởi động muộn nhất và dừng sớm nhất để không cản trở các bean khác.
        return Integer.MAX_VALUE;
    }
}
