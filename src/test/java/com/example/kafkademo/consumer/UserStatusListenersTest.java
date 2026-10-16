package com.example.kafkademo.consumer;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.example.kafkademo.config.AppKafkaProperties;
import com.example.kafkademo.model.Events.UserStatus;
import com.example.kafkademo.model.Events.UserStatusEvent;
import com.example.kafkademo.service.UserStatusStore;
import java.time.Instant;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;

@ExtendWith(MockitoExtension.class)
class UserStatusListenersTest {

    @Mock
    private UserStatusStore store;

    @Mock
    private KafkaTemplate<String, Object> kafka;

    @Mock
    private AppKafkaProperties props;

    private UserStatusListeners listeners;

    @BeforeEach
    void setUp() {
        listeners = new UserStatusListeners(store, kafka, props);
    }

    @Test
    void nullStatusOrBlankUserGoesToTheCallerAsIllegalArgument() {
        assertThrows(IllegalArgumentException.class, () -> listeners.process(record(event(null, null))));
        assertThrows(IllegalArgumentException.class, () -> listeners.process(record(event("  ", UserStatus.ONLINE))));
        verify(kafka, never()).send(org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.any());
    }

    @Test
    void appliedEventIsWrittenToTheChangelog() {
        UserStatusEvent event = event("u42", UserStatus.ONLINE);
        when(store.apply(event)).thenReturn(true);
        when(props.topic("user-status-changelog")).thenReturn("user-status-changelog");
        listeners.process(record(event));
        verify(kafka).send("user-status-changelog", "u42", event);
    }

    @Test
    void staleEventDoesNotWriteTheChangelog() {
        UserStatusEvent event = event("u42", UserStatus.ONLINE);
        when(store.apply(event)).thenReturn(false);
        listeners.process(record(event));
        verify(kafka, never()).send(org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.any());
    }

    @Test
    void auditReadsTheRecord() {
        listeners.audit(record(event("u42", UserStatus.AWAY)));
    }

    private static UserStatusEvent event(String userId, UserStatus status) {
        return new UserStatusEvent(userId, status, "login", Instant.EPOCH, 5L);
    }

    private static ConsumerRecord<String, UserStatusEvent> record(UserStatusEvent event) {
        return new ConsumerRecord<>("user-status-events", 1, 4L, event.userId(), event);
    }
}
