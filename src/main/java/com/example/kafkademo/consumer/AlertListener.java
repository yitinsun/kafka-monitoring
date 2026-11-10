package com.example.kafkademo.consumer;

import com.example.kafkademo.model.Events.Alert;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class AlertListener {

    private static final Logger log = LoggerFactory.getLogger(AlertListener.class);

    @KafkaListener(
            id = "alert-notifier",
            topics = "${app.kafka.topics.alerts.name}",
            groupId = "${app.kafka.groups.alert-notifier.group-id}",
            concurrency = "${app.kafka.groups.alert-notifier.concurrency}",
            containerFactory = "recordFactory")
    public void notify(ConsumerRecord<String, Alert> record) {
        Alert alert = record.value();
        if (record.partition() == 0) {
            log.warn("PAGE ON-CALL: {} {} = {} (limit {})", alert.service(), alert.metric(), alert.value(), alert.threshold());
        } else {
            log.info("Ticket: {} {} = {} (limit {})", alert.service(), alert.metric(), alert.value(), alert.threshold());
        }
    }
}
