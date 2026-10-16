package com.example.kafkademo.consumer;

import com.example.kafkademo.config.AppKafkaProperties;
import com.example.kafkademo.model.Events.UserStatusEvent;
import com.example.kafkademo.service.UserStatusStore;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class UserStatusListeners {

    private static final Logger log = LoggerFactory.getLogger(UserStatusListeners.class);

    private final UserStatusStore store;
    private final KafkaTemplate<String, Object> kafka;
    private final AppKafkaProperties props;

    public UserStatusListeners(UserStatusStore store, KafkaTemplate<String, Object> kafka, AppKafkaProperties props) {
        this.store = store;
        this.kafka = kafka;
        this.props = props;
    }

    @KafkaListener(
            id = "user-status-processor",
            topics = "${app.kafka.topics.user-status.name}",
            groupId = "${app.kafka.groups.user-status-processor.group-id}",
            concurrency = "${app.kafka.groups.user-status-processor.concurrency}",
            containerFactory = "recordFactory")
    public void process(ConsumerRecord<String, UserStatusEvent> record) {
        UserStatusEvent event = record.value();
        if (event.status() == null || event.userId() == null || event.userId().isBlank()) {
            throw new IllegalArgumentException("Invalid status event at offset " + record.offset());
        }
        if (store.apply(event)) {
            kafka.send(props.topic("user-status-changelog"), event.userId(), event);
            log.info("[processor p{}] {} -> {}", record.partition(), event.userId(), event.status());
        } else {
            log.debug("Ignored stale event for {}", event.userId());
        }
    }

    @KafkaListener(
            id = "user-status-audit",
            topics = "${app.kafka.topics.user-status.name}",
            groupId = "${app.kafka.groups.user-status-audit.group-id}",
            concurrency = "${app.kafka.groups.user-status-audit.concurrency}",
            containerFactory = "recordFactory")
    public void audit(ConsumerRecord<String, UserStatusEvent> record) {
        UserStatusEvent event = record.value();
        log.debug("[audit p{} o{}] user={} status={} reason={}",
                record.partition(), record.offset(), event.userId(), event.status(), event.reason());
    }
}
