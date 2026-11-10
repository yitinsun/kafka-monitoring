package com.example.kafkademo.web;

import com.example.kafkademo.model.Events.UserStatus;
import com.example.kafkademo.model.Events.UserStatusEvent;
import com.example.kafkademo.producer.UserStatusPublisher;
import com.example.kafkademo.service.UserStatusStore;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/users")
public class UserStatusController {

    public record StatusChange(UserStatus status, String reason) {}

    private final UserStatusPublisher publisher;
    private final UserStatusStore store;

    public UserStatusController(UserStatusPublisher publisher, UserStatusStore store) {
        this.publisher = publisher;
        this.store = store;
    }

    /** 202 means the event was handed to the producer. The read model catches up after that. */
    @PutMapping("/{userId}/status")
    public ResponseEntity<Void> change(@PathVariable String userId, @RequestBody StatusChange body) {
        publisher.publish(userId, body.status(), body.reason());
        return ResponseEntity.accepted().build();
    }

    @GetMapping("/{userId}/status")
    public ResponseEntity<UserStatusEvent> get(@PathVariable String userId) {
        return store.get(userId).map(ResponseEntity::ok).orElse(ResponseEntity.notFound().build());
    }
}
