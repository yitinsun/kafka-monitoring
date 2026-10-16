package com.example.kafkademo.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;

class KafkaListenerConfigTest {

    @Test
    void factoriesUseRecordAndBatchAckModesAndDeadLetterHandler() {
        KafkaListenerConfig config = new KafkaListenerConfig();
        @SuppressWarnings("unchecked")
        KafkaTemplate<String, Object> template = mock(KafkaTemplate.class);
        @SuppressWarnings("unchecked")
        ConsumerFactory<String, Object> consumerFactory = mock(ConsumerFactory.class);

        DefaultErrorHandler handler = config.kafkaErrorHandler(template);
        ConsumerRecord<String, Object> record = new ConsumerRecord<>("user-status-events", 0, 1L, "u42", "bad");
        assertNotNull(handler);

        ConcurrentKafkaListenerContainerFactory<String, Object> records = config.recordFactory(consumerFactory, handler);
        ConcurrentKafkaListenerContainerFactory<String, Object> batches = config.batchFactory(consumerFactory, handler);

        assertEquals(ContainerProperties.AckMode.RECORD, records.getContainerProperties().getAckMode());
        assertTrue(records.getContainerProperties().isObservationEnabled());
        assertEquals(ContainerProperties.AckMode.BATCH, batches.getContainerProperties().getAckMode());
        assertTrue(batches.isBatchListener());
        assertNotNull(record.topic());
    }
}
