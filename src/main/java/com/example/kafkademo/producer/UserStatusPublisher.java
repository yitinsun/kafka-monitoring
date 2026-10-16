package com.example.kafkademo.producer;

import com.example.kafkademo.config.AppKafkaProperties;
import com.example.kafkademo.model.Events.UserStatus;
import com.example.kafkademo.model.Events.UserStatusEvent;
import java.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class UserStatusPublisher {

    private static final Logger log = LoggerFactory.getLogger(UserStatusPublisher.class);

    private final KafkaTemplate<String, Object> kafka;
    private final AppKafkaProperties props;

    public UserStatusPublisher(KafkaTemplate<String, Object> kafka, AppKafkaProperties props) {
        this.kafka = kafka;
        this.props = props;
    }

    public void publish(String userId, UserStatus status, String reason) {
        Instant now = Instant.now();
        var event = new UserStatusEvent(userId, status, reason, now, now.toEpochMilli());
        kafka.send(props.topic("user-status"), userId, event).whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("Failed to publish status for {}", userId, ex);
            } else {
                log.debug("Status {} for {} -> partition {} offset {}", status, userId,
                        result.getRecordMetadata().partition(), result.getRecordMetadata().offset());
            }
        });
    }
}
