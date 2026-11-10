package com.example.kafkademo.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.example.kafkademo.config.AppKafkaProperties.Topic;
import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Map;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaAdmin;

class KafkaTopicConfigTest {

    @Test
    void buildsCompactedRetentionAndReplicaSettingsFromProperties() throws Exception {
        AppKafkaProperties props = new AppKafkaProperties(Map.of(
                "user-status-changelog", new Topic("user-status-changelog", 6, (short) 1, null, true),
                "user-status", new Topic("user-status-events", 12, (short) 1, 604800000L, false),
                "user-status-dlt", new Topic("user-status-events.DLT", 3, (short) 3, null, false)
        ), Map.of());

        assertEquals("user-status-events", props.topic("user-status"));
        assertEquals(12, props.partitions("user-status"));

        KafkaAdmin.NewTopics declared = new KafkaTopicConfig().appTopics(props);
        Collection<NewTopic> topics = topicsOf(declared);

        NewTopic changelog = find(topics, "user-status-changelog");
        assertEquals(6, changelog.numPartitions());
        assertEquals(TopicConfig.CLEANUP_POLICY_COMPACT, changelog.configs().get(TopicConfig.CLEANUP_POLICY_CONFIG));
        assertEquals("1", changelog.configs().get(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG));

        NewTopic events = find(topics, "user-status-events");
        assertEquals(12, events.numPartitions());
        assertEquals("604800000", events.configs().get(TopicConfig.RETENTION_MS_CONFIG));
        assertEquals("1", events.configs().get(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG));

        NewTopic dlt = find(topics, "user-status-events.DLT");
        assertEquals(3, dlt.numPartitions());
        assertNull(dlt.configs().get(TopicConfig.RETENTION_MS_CONFIG));
        assertEquals("2", dlt.configs().get(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG));
    }

    private static NewTopic find(Collection<NewTopic> topics, String name) {
        return topics.stream().filter(topic -> topic.name().equals(name)).findFirst().orElseThrow();
    }

    @SuppressWarnings("unchecked")
    private static Collection<NewTopic> topicsOf(KafkaAdmin.NewTopics declared) throws Exception {
        Field field = KafkaAdmin.NewTopics.class.getDeclaredField("newTopics");
        field.setAccessible(true);
        return (Collection<NewTopic>) field.get(declared);
    }
}
