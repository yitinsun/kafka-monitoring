package com.example.kafkademo.consumer;

import static org.junit.jupiter.api.Assertions.assertTrue;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.example.kafkademo.model.Events.Alert;
import com.example.kafkademo.model.Events.Severity;
import java.time.Instant;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

class AlertListenerTest {

    @Test
    void partitionZeroPagesAndOtherPartitionsTicket() {
        Logger logger = (Logger) LoggerFactory.getLogger(AlertListener.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        logger.addAppender(appender);
        try {
            AlertListener listener = new AlertListener();
            Alert alert = new Alert("auth", "cpu.usage", 1.2, 0.90, Severity.CRITICAL, Instant.EPOCH);
            listener.notify(new ConsumerRecord<>("system-alerts", 0, 0L, "auth", alert));
            listener.notify(new ConsumerRecord<>("system-alerts", 1, 1L, "auth", alert));
        } finally {
            logger.detachAppender(appender);
        }
        assertTrue(appender.list.stream().anyMatch(event -> event.getFormattedMessage().contains("PAGE ON-CALL")));
        assertTrue(appender.list.stream().anyMatch(event -> event.getFormattedMessage().contains("Ticket")));
    }
}
