package com.example.settlementconsumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class EventConsumer {
    @KafkaListener(topics = "business-events")
    public void consume(ConsumerRecord<String, String> record) {
        System.out.printf("[SETTLEMENT] group=%s | partition=%d | offset=%d | key=%s | value=%s%n",
            "settlement-group", record.partition(), record.offset(), record.key(), record.value());
    }
}
