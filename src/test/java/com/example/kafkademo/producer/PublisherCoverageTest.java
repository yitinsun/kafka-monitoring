package com.example.kafkademo.producer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.example.kafkademo.config.AppKafkaProperties;
import com.example.kafkademo.config.AppKafkaProperties.Topic;
import com.example.kafkademo.model.Events.MetricSample;
import com.example.kafkademo.model.Events.UserStatus;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

class PublisherCoverageTest {

    @Test
    void userStatusPublishLogsSuccessAndFailure() {
        @SuppressWarnings("unchecked")
        KafkaTemplate<String, Object> kafka = mock(KafkaTemplate.class);
        AppKafkaProperties props = new AppKafkaProperties(
                Map.of("user-status", new Topic("user-status-events", 12, (short) 1, 1L, false)),
                Map.of());
        UserStatusPublisher publisher = new UserStatusPublisher(kafka, props);

        CompletableFuture<SendResult<String, Object>> ok = new CompletableFuture<>();
        when(kafka.send(eq("user-status-events"), eq("u42"), any())).thenReturn(ok);
        publisher.publish("u42", UserStatus.ONLINE, "login");
        RecordMetadata meta = new RecordMetadata(new TopicPartition("user-status-events", 3), 9L, 0, 0L, 0, 0);
        ok.complete(new SendResult<>(new ProducerRecord<>("user-status-events", "u42", "event"), meta));

        CompletableFuture<SendResult<String, Object>> failed = new CompletableFuture<>();
        when(kafka.send(eq("user-status-events"), eq("u7"), any())).thenReturn(failed);
        publisher.publish("u7", UserStatus.OFFLINE, "logout");
        failed.completeExceptionally(new RuntimeException("broker down"));

        verify(kafka, times(2)).send(eq("user-status-events"), any(), any());
    }

    @Test
    void metricsPublisherSendsKeyedSamples() {
        @SuppressWarnings("unchecked")
        KafkaTemplate<String, Object> kafka = mock(KafkaTemplate.class);
        AppKafkaProperties props = new AppKafkaProperties(
                Map.of("metrics", new Topic("system-metrics", 24, (short) 1, 1L, false)),
                Map.of());
        MetricsPublisher publisher = new MetricsPublisher(kafka, props);
        MetricSample sample = new MetricSample("auth", "auth-1", "cpu.usage", 0.4, Instant.now());
        publisher.send(sample);
        verify(kafka).send("system-metrics", "auth-1", sample);
        publisher.generate();
        verify(kafka, atLeastOnce()).send(eq("system-metrics"), any(), any());
    }
}
