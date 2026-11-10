package com.example.kafkademo.producer;

import com.example.kafkademo.config.AppKafkaProperties;
import com.example.kafkademo.model.Events.MetricSample;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/** Demo load generator: each service instance reports CPU and p99 latency every second. */
@Component
@ConditionalOnProperty(name = "app.demo.generate-metrics", havingValue = "true", matchIfMissing = true)
public class MetricsPublisher {

    private static final List<String> SERVICES = List.of("auth", "orders", "payments", "search", "notifications");

    private final KafkaTemplate<String, Object> kafka;
    private final AppKafkaProperties props;

    public MetricsPublisher(KafkaTemplate<String, Object> kafka, AppKafkaProperties props) {
        this.kafka = kafka;
        this.props = props;
    }

    @Scheduled(fixedRate = 1000)
    void generate() {
        var rnd = ThreadLocalRandom.current();
        for (String service : SERVICES) {
            for (int i = 1; i <= 4; i++) {
                String instance = service + "-" + i;
                send(new MetricSample(service, instance, "cpu.usage", rnd.nextDouble(0.1, 1.0), Instant.now()));
                send(new MetricSample(service, instance, "latency.p99.ms", rnd.nextDouble(20, 1400), Instant.now()));
            }
        }
    }

    public void send(MetricSample sample) {
        kafka.send(props.topic("metrics"), sample.instance(), sample);
    }
}
