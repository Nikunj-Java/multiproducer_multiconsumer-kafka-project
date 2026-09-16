package com.example.paymentproducer;

import java.time.Instant;
import java.util.UUID;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/events")
public class ProducerController {
    private final KafkaTemplate<String, String> kafkaTemplate;
    private static final String TOPIC = "business-events";
    private static final String EVENT_TYPE = "PAYMENT";

    public ProducerController(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @PostMapping
    public String publish(@RequestParam String payment,
                          @RequestParam String key) {
        String eventId = UUID.randomUUID().toString();
        String payload = String.format(
            "{\"eventId\":\"%s\",\"eventType\":\"%s\",\"key\":\"%s\",\"data\":\"%s\",\"timestamp\":\"%s\"}",
            eventId, EVENT_TYPE, key, payment, Instant.now());

        kafkaTemplate.send(TOPIC, key, payload);
        return "Published " + EVENT_TYPE + " event: " + payload;
    }
}
