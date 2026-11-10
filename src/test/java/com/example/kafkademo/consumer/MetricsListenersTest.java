package com.example.kafkademo.consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.example.kafkademo.model.Events.Alert;
import com.example.kafkademo.model.Events.MetricSample;
import com.example.kafkademo.model.Events.Severity;
import com.example.kafkademo.monitoring.MetricsAggregator;
import com.example.kafkademo.producer.AlertPublisher;
import java.time.Instant;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class MetricsListenersTest {

    @Mock
    private MetricsAggregator aggregator;

    @Mock
    private AlertPublisher alerts;

    private MetricsListeners listeners;

    @BeforeEach
    void setUp() {
        listeners = new MetricsListeners(aggregator, alerts);
    }

    @Test
    void underThresholdDoesNotAlert() {
        listeners.evaluate(List.of(
                record(sample("cpu.usage", 0.5)),
                record(sample("latency.p99.ms", 100))));
        verify(alerts, never()).publish(org.mockito.ArgumentMatchers.any());
    }

    @Test
    void cpuJustOverThresholdIsWarning() {
        listeners.evaluate(List.of(record(sample("cpu.usage", 0.95))));
        assertSeverity(Severity.WARNING);
    }

    @Test
    void cpuAtOneIsWarning() {
        listeners.evaluate(List.of(record(sample("cpu.usage", 1.0))));
        assertSeverity(Severity.WARNING);
    }

    @Test
    void cpuAboveOnePointZeroEightIsCritical() {
        listeners.evaluate(List.of(record(sample("cpu.usage", 1.09))));
        assertSeverity(Severity.CRITICAL);
    }

    @Test
    void latencyBreachPublishesOneAlert() {
        listeners.evaluate(List.of(record(sample("latency.p99.ms", 1001))));
        verify(alerts, times(1)).publish(org.mockito.ArgumentMatchers.any());
    }

    @Test
    void unknownMetricDoesNotAlert() {
        listeners.evaluate(List.of(record(sample("disk.used", 10_000))));
        verify(alerts, never()).publish(org.mockito.ArgumentMatchers.any());
    }

    @Test
    void cooldownSuppressesTheSecondBreach() {
        listeners.evaluate(List.of(record(sample("cpu.usage", 0.95))));
        listeners.evaluate(List.of(record(sample("cpu.usage", 0.95))));
        verify(alerts, times(1)).publish(org.mockito.ArgumentMatchers.any());
    }

    @Test
    void aggregateForwardsEachSample() {
        MetricSample sample = sample("cpu.usage", 0.2);
        listeners.aggregate(List.of(record(sample)));
        verify(aggregator).record(sample);
    }

    private void assertSeverity(Severity expected) {
        ArgumentCaptor<Alert> captor = ArgumentCaptor.forClass(Alert.class);
        verify(alerts).publish(captor.capture());
        assertEquals(expected, captor.getValue().severity());
    }

    private static MetricSample sample(String metric, double value) {
        return new MetricSample("auth", "auth-1", metric, value, Instant.now());
    }

    private static ConsumerRecord<String, MetricSample> record(MetricSample sample) {
        return new ConsumerRecord<>("system-metrics", 0, 0L, sample.instance(), sample);
    }
}
