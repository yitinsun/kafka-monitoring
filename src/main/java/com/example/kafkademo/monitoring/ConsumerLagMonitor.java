package com.example.kafkademo.monitoring;

import com.example.kafkademo.config.AppKafkaProperties;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Publishes app.kafka.group.lag{group=...} as the sum over partitions of (log end offset - committed offset).
 */
@Component
@ConditionalOnProperty(name = "app.monitoring.lag-enabled", havingValue = "true", matchIfMissing = true)
public class ConsumerLagMonitor {

    private static final Logger log = LoggerFactory.getLogger(ConsumerLagMonitor.class);

    private final KafkaAdmin kafkaAdmin;
    private final AppKafkaProperties props;
    private final MeterRegistry registry;
    private final Map<String, AtomicLong> lagByGroup = new ConcurrentHashMap<>();

    public ConsumerLagMonitor(KafkaAdmin kafkaAdmin, AppKafkaProperties props, MeterRegistry registry) {
        this.kafkaAdmin = kafkaAdmin;
        this.props = props;
        this.registry = registry;
    }

    @Scheduled(fixedDelay = 15_000, initialDelay = 20_000)
    public void poll() {
        try (Admin admin = Admin.create(kafkaAdmin.getConfigurationProperties())) {
            for (var group : props.groups().values()) {
                Map<TopicPartition, OffsetAndMetadata> committed = admin
                        .listConsumerGroupOffsets(group.groupId())
                        .partitionsToOffsetAndMetadata().get(5, TimeUnit.SECONDS);
                if (committed.isEmpty()) {
                    continue;
                }

                var latest = admin.listOffsets(committed.keySet().stream()
                                .collect(Collectors.toMap(tp -> tp, tp -> OffsetSpec.latest())))
                        .all().get(5, TimeUnit.SECONDS);

                long lag = committed.entrySet().stream()
                        .mapToLong(e -> latest.get(e.getKey()).offset() - e.getValue().offset()).sum();

                lagByGroup.computeIfAbsent(group.groupId(),
                        id -> registry.gauge("app.kafka.group.lag", Tags.of("group", id), new AtomicLong())).set(lag);
            }
        } catch (Exception e) {
            log.warn("Lag polling failed: {}", e.toString());
        }
    }
}
