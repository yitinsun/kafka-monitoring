package com.example.kafkademo.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;

/**
 * Declares every topic from application.yml. KafkaAdmin creates missing topics at startup and
 * increases the partition count when the configured number is larger. Kafka never decreases
 * partitions, and increasing them remaps keys, so the catalog is sized with headroom.
 */
@Configuration
public class KafkaTopicConfig {

    @Bean
    KafkaAdmin.NewTopics appTopics(AppKafkaProperties props) {
        NewTopic[] topics = props.topics().values().stream().map(t -> {
            TopicBuilder builder = TopicBuilder.name(t.name())
                    .partitions(t.partitions())
                    .replicas(t.replicas())
                    .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, t.replicas() > 1 ? "2" : "1");
            if (t.compacted()) {
                builder.config(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT);
            } else if (t.retentionMs() != null) {
                builder.config(TopicConfig.RETENTION_MS_CONFIG, String.valueOf(t.retentionMs()));
            }
            return builder.build();
        }).toArray(NewTopic[]::new);
        return new KafkaAdmin.NewTopics(topics);
    }
}
