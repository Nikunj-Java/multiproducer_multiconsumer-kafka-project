package com.example.tradeproducer;

import java.time.Instant;
import java.util.UUID;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/events")
public class ProducerController {
    private final KafkaTemplate<String, String> kafkaTemplate;
    private static final String TOPIC = "business-events";
    private static final String EVENT_TYPE = "TRADE";

    public ProducerController(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @PostMapping
    public String publish(@RequestParam String trade,
                          @RequestParam String key) {
        String eventId = UUID.randomUUID().toString();
        String payload = String.format(
            "{\"eventId\":\"%s\",\"eventType\":\"%s\",\"key\":\"%s\",\"data\":\"%s\",\"timestamp\":\"%s\"}",
            eventId, EVENT_TYPE, key, trade, Instant.now());

        kafkaTemplate.send(TOPIC, key, payload);
        return "Published " + EVENT_TYPE + " event: " + payload;
    }
}
