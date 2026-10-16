package com.example.kafkademo.producer;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.example.kafkademo.config.AppKafkaProperties;
import com.example.kafkademo.config.AppKafkaProperties.Topic;
import com.example.kafkademo.model.Events.Alert;
import com.example.kafkademo.model.Events.Severity;
import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;

@ExtendWith(MockitoExtension.class)
class AlertPublisherTest {

    @Mock
    private KafkaTemplate<String, Object> kafka;

    private AlertPublisher publisher;

    @BeforeEach
    void setUp() {
        AppKafkaProperties props = new AppKafkaProperties(
                Map.of("alerts", new Topic("system-alerts", 3, (short) 1, 604800000L, false)),
                Map.of());
        publisher = new AlertPublisher(kafka, props);
    }

    @Test
    void criticalAlertUsesPartitionZero() {
        Alert alert = alert(Severity.CRITICAL);
        publisher.publish(alert);
        verify(kafka).send(eq("system-alerts"), eq(0), eq("auth"), eq(alert));
    }

    @Test
    void warningAlertAvoidsPartitionZero() {
        Alert alert = alert(Severity.WARNING);
        int expected = 1 + Math.floorMod("auth".hashCode(), 2);
        publisher.publish(alert);
        verify(kafka).send(eq("system-alerts"), eq(expected), eq("auth"), eq(alert));
    }

    private static Alert alert(Severity severity) {
        return new Alert("auth", "cpu.usage", 0.95, 0.90, severity, Instant.EPOCH);
    }
}
