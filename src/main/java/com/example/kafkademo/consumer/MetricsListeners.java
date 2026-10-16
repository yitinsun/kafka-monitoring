package com.example.kafkademo.consumer;

import com.example.kafkademo.model.Events.Alert;
import com.example.kafkademo.model.Events.MetricSample;
import com.example.kafkademo.model.Events.Severity;
import com.example.kafkademo.monitoring.MetricsAggregator;
import com.example.kafkademo.producer.AlertPublisher;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class MetricsListeners {

    private static final Duration COOLDOWN = Duration.ofSeconds(30);

    private final MetricsAggregator aggregator;
    private final AlertPublisher alerts;
    private final Map<String, Instant> lastAlert = new ConcurrentHashMap<>();

    public MetricsListeners(MetricsAggregator aggregator, AlertPublisher alerts) {
        this.aggregator = aggregator;
        this.alerts = alerts;
    }

    @KafkaListener(
            id = "metrics-aggregator",
            topics = "${app.kafka.topics.metrics.name}",
            groupId = "${app.kafka.groups.metrics-aggregator.group-id}",
            concurrency = "${app.kafka.groups.metrics-aggregator.concurrency}",
            containerFactory = "batchFactory")
    public void aggregate(List<ConsumerRecord<String, MetricSample>> batch) {
        batch.forEach(record -> aggregator.record(record.value()));
    }

    @KafkaListener(
            id = "metrics-alerting",
            topics = "${app.kafka.topics.metrics.name}",
            groupId = "${app.kafka.groups.metrics-alerting.group-id}",
            concurrency = "${app.kafka.groups.metrics-alerting.concurrency}",
            containerFactory = "batchFactory")
    public void evaluate(List<ConsumerRecord<String, MetricSample>> batch) {
        for (var record : batch) {
            MetricSample sample = record.value();
            double threshold = switch (sample.metric()) {
                case "cpu.usage" -> 0.90;
                case "latency.p99.ms" -> 1000;
                default -> Double.MAX_VALUE;
            };
            if (sample.value() <= threshold) {
                continue;
            }

            String key = sample.instance() + "|" + sample.metric();
            Instant now = Instant.now();
            Instant prev = lastAlert.get(key);
            if (prev != null && Duration.between(prev, now).compareTo(COOLDOWN) < 0) {
                continue;
            }
            lastAlert.put(key, now);

            Severity severity = sample.value() > threshold * 1.2 ? Severity.CRITICAL : Severity.WARNING;
            alerts.publish(new Alert(sample.service(), sample.metric(), sample.value(), threshold, severity, now));
        }
    }
}
