package com.example.kafkademo.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.example.kafkademo.model.Events.UserStatus;
import com.example.kafkademo.model.Events.UserStatusEvent;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class UserStatusStoreTest {

    private final UserStatusStore store = new UserStatusStore();

    @Test
    void firstEventIsApplied() {
        UserStatusEvent event = event("u42", 10L);
        assertTrue(store.apply(event));
        assertEquals(event, store.get("u42").orElseThrow());
        assertEquals(1, store.size());
    }

    @Test
    void newerVersionReplacesStoredEvent() {
        store.apply(event("u42", 10L));
        UserStatusEvent newer = new UserStatusEvent("u42", UserStatus.AWAY, "idle", Instant.EPOCH, 11L);
        assertTrue(store.apply(newer));
        assertEquals(newer, store.get("u42").orElseThrow());
    }

    @Test
    void staleVersionIsIgnored() {
        UserStatusEvent current = event("u42", 10L);
        store.apply(current);
        assertFalse(store.apply(event("u42", 9L)));
        assertEquals(current, store.get("u42").orElseThrow());
    }

    @Test
    void equalVersionIsIgnored() {
        UserStatusEvent current = event("u42", 10L);
        store.apply(current);
        assertFalse(store.apply(event("u42", 10L)));
        assertEquals(current, store.get("u42").orElseThrow());
    }

    @Test
    void unknownUserIsEmpty() {
        assertTrue(store.get("missing").isEmpty());
    }

    private static UserStatusEvent event(String userId, long version) {
        return new UserStatusEvent(userId, UserStatus.ONLINE, "login", Instant.EPOCH, version);
    }
}
