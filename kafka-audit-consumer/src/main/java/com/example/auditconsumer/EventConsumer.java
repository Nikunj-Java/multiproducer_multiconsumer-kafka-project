package com.example.auditconsumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class EventConsumer {
    @KafkaListener(topics = "business-events")
    public void consume(ConsumerRecord<String, String> record) {
        System.out.printf("[AUDIT] group=%s | partition=%d | offset=%d | key=%s | value=%s%n",
            "audit-group", record.partition(), record.offset(), record.key(), record.value());
    }
}
