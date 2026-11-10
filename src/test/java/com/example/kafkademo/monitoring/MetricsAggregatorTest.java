package com.example.kafkademo.monitoring;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.example.kafkademo.model.Events.MetricSample;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class MetricsAggregatorTest {

    @Test
    void recordsEwmaMaxAndEndToEndLatency() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        MetricsAggregator aggregator = new MetricsAggregator(registry);
        Instant start = Instant.now().minusSeconds(1);

        aggregator.record(new MetricSample("auth", "auth-1", "cpu.usage", 1.0, start));
        aggregator.record(new MetricSample("auth", "auth-1", "cpu.usage", 0.0, start));

        assertEquals(0.8, registry.get("app.service.metric.ewma").gauge().value(), 0.0001);
        assertEquals(1.0, registry.get("app.service.metric.max").gauge().value(), 0.0001);
        assertTrue(registry.get("app.kafka.e2e.latency").timer().count() >= 2);
    }
}
