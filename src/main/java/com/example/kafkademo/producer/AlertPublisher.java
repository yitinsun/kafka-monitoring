package com.example.kafkademo.producer;

import com.example.kafkademo.config.AppKafkaProperties;
import com.example.kafkademo.model.Events.Alert;
import com.example.kafkademo.model.Events.Severity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class AlertPublisher {

    private final KafkaTemplate<String, Object> kafka;
    private final AppKafkaProperties props;

    public AlertPublisher(KafkaTemplate<String, Object> kafka, AppKafkaProperties props) {
        this.kafka = kafka;
        this.props = props;
    }

    /**
     * Explicit partition choice (bypasses key hashing): CRITICAL alerts always use partition 0, so a
     * flood of warnings on the other partitions cannot starve them.
     */
    public void publish(Alert alert) {
        int partitions = props.partitions("alerts");
        int partition = alert.severity() == Severity.CRITICAL
                ? 0
                : 1 + Math.floorMod(alert.service().hashCode(), partitions - 1);
        kafka.send(props.topic("alerts"), partition, alert.service(), alert);
    }
}
