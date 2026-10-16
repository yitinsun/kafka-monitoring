package com.example.kafkademo.service;

import com.example.kafkademo.model.Events.UserStatusEvent;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.stereotype.Component;

/** In-memory read model of current user status. The compacted changelog is the rebuild source; v1 does not replay it at startup. */
@Component
public class UserStatusStore {

    private final Map<String, UserStatusEvent> current = new ConcurrentHashMap<>();

    /** @return true if the event was newer than what we had (replays and stale events are ignored). */
    public boolean apply(UserStatusEvent event) {
        boolean[] applied = {false};
        current.compute(event.userId(), (id, old) -> {
            if (old == null || event.version() > old.version()) {
                applied[0] = true;
                return event;
            }
            return old;
        });
        return applied[0];
    }

    public Optional<UserStatusEvent> get(String userId) {
        return Optional.ofNullable(current.get(userId));
    }

    public int size() {
        return current.size();
    }
}
