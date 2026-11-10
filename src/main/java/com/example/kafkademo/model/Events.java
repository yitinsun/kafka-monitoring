package com.example.kafkademo.model;

import java.time.Instant;

public final class Events {
    private Events() {}

    public enum UserStatus { ONLINE, AWAY, OFFLINE, SUSPENDED, DELETED }

    public enum Severity { WARNING, CRITICAL }

    /** key = userId, version = event time millis (used to ignore stale or replayed updates). */
    public record UserStatusEvent(String userId, UserStatus status, String reason, Instant occurredAt, long version) {}

    /** key = service-instance. */
    public record MetricSample(String service, String instance, String metric, double value, Instant ts) {}

    public record Alert(String service, String metric, double value, double threshold,
                        Severity severity, Instant raisedAt) {}
}
