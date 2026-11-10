package com.example.kafkademo.config;

import java.util.Map;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "app.kafka")
public record AppKafkaProperties(Map<String, Topic> topics, Map<String, Group> groups) {

    public record Topic(String name, int partitions, short replicas, Long retentionMs, boolean compacted) {}

    public record Group(String groupId, int concurrency) {}

    public String topic(String key) {
        return topics.get(key).name();
    }

    public int partitions(String key) {
        return topics.get(key).partitions();
    }
}
