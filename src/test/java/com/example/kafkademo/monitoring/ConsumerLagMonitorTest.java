package com.example.kafkademo.monitoring;

import static org.mockito.Mockito.when;

import com.example.kafkademo.config.AppKafkaProperties;
import com.example.kafkademo.config.AppKafkaProperties.Group;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.Map;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaAdmin;

class ConsumerLagMonitorTest {

    @Test
    void pollFailureIsSwallowed() {
        KafkaAdmin admin = org.mockito.Mockito.mock(KafkaAdmin.class);
        when(admin.getConfigurationProperties()).thenReturn(Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:1",
                AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "200",
                AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "200"));
        AppKafkaProperties props = new AppKafkaProperties(Map.of(),
                Map.of("user-status-processor", new Group("user-status-processor", 6)));
        ConsumerLagMonitor monitor = new ConsumerLagMonitor(admin, props, new SimpleMeterRegistry());
        monitor.poll();
    }
}
