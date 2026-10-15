# Technology Stack Profile: Spring Boot Kafka Monitoring (Java 21)

This document is the **only** allowed toolchain for v1. Packages not listed here are high-impact (see `ORCHESTRATION_MANIFEST.md` §5.1).

**Explicitly out of v1 (do not add):** Spring Security, Redis, a JDBC driver, Flyway/Liquibase, Schema Registry, Kafka Streams, Resilience4j. The reference module in `.notes/kafka-demo.zip` is the dependency baseline.

## 1. Core Tooling Versions

* **Language Runtime:** Java 21
* **Framework:** Spring Boot **3.3.x** (parent POM). The reference zip uses **3.3.5**.
* **Build System:** Apache Maven 3.9+
* **Broker image:** `apache/kafka:3.8.0` (single-node KRaft)
* **Client:** `spring-kafka` managed by the Boot parent (do not pin a second Kafka client version)
* **Metrics:** Micrometer Prometheus registry via Actuator
* **Coverage:** JaCoCo Maven plugin, **minimum 80% line coverage**, `mvn clean test` fails below that bar
* **Coordinates:** `groupId` `com.example`, `artifactId` `kafka-demo`, package `com.example.kafkademo` (matches the reference zip and `spring.json.trusted.packages`)

## 2. Application Dependency Suite (`pom.xml`)

Use `spring-boot-starter-parent` version `3.3.5`. Do not pin starter versions separately.

```xml
<properties>
    <java.version>21</java.version>
    <jacoco.version>0.8.12</jacoco.version>
</properties>

<dependencies>
    <dependency>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-web</artifactId>
    </dependency>
    <dependency>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-actuator</artifactId>
    </dependency>
    <dependency>
        <groupId>org.springframework.kafka</groupId>
        <artifactId>spring-kafka</artifactId>
    </dependency>
    <dependency>
        <groupId>io.micrometer</groupId>
        <artifactId>micrometer-registry-prometheus</artifactId>
    </dependency>
    <dependency>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-test</artifactId>
        <scope>test</scope>
    </dependency>
    <dependency>
        <groupId>org.springframework.kafka</groupId>
        <artifactId>spring-kafka-test</artifactId>
        <scope>test</scope>
    </dependency>
</dependencies>
```

`spring-kafka-test` is test scope only. It is not in the reference zip; it is declared here so Gate 4 can use `@EmbeddedKafka` without a later high-impact dependency add.

Configure `jacoco-maven-plugin` so `mvn clean test` runs `prepare-agent` and a `check` rule: **LINE covered ratio ≥ 0.80**. Do not add other plugins without Gate 2 / high-impact approval. The Boot plugin is required to package and run the service.

## 3. Configuration Setup (`application.yml`)

Copy the catalog from `.notes/application.yml`. Hosts use `${PROPERTY:DEFAULT}`. Defaults are **lab-only**.

```yaml
spring:
  application.name: kafka-demo
  kafka:
    bootstrap-servers: ${KAFKA_BOOTSTRAP:localhost:9092}
    producer:
      acks: all
      compression-type: lz4
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.springframework.kafka.support.serializer.JsonSerializer
      properties:
        enable.idempotence: true
        linger.ms: 10
        batch.size: 65536
    consumer:
      auto-offset-reset: earliest
      enable-auto-commit: false
      max-poll-records: 500
      key-deserializer: org.springframework.kafka.support.serializer.ErrorHandlingDeserializer
      value-deserializer: org.springframework.kafka.support.serializer.ErrorHandlingDeserializer
      properties:
        spring.deserializer.key.delegate.class: org.apache.kafka.common.serialization.StringDeserializer
        spring.deserializer.value.delegate.class: org.springframework.kafka.support.serializer.JsonDeserializer
        spring.json.trusted.packages: com.example.kafkademo.model
        partition.assignment.strategy: org.apache.kafka.clients.consumer.CooperativeStickyAssignor
        session.timeout.ms: 15000

app:
  demo:
    generate-metrics: true
  kafka:
    topics:
      user-status:
        name: user-status-events
        partitions: 12
        replicas: ${KAFKA_REPLICAS:1}
        retention-ms: 604800000
      user-status-changelog:
        name: user-status-changelog
        partitions: 6
        replicas: ${KAFKA_REPLICAS:1}
        compacted: true
      metrics:
        name: system-metrics
        partitions: 24
        replicas: ${KAFKA_REPLICAS:1}
        retention-ms: 86400000
      alerts:
        name: system-alerts
        partitions: 3
        replicas: ${KAFKA_REPLICAS:1}
        retention-ms: 604800000
      user-status-dlt:
        name: user-status-events.DLT
        partitions: 3
        replicas: ${KAFKA_REPLICAS:1}
      metrics-dlt:
        name: system-metrics.DLT
        partitions: 3
        replicas: ${KAFKA_REPLICAS:1}
    groups:
      user-status-processor:
        group-id: user-status-processor
        concurrency: 6
      user-status-audit:
        group-id: user-status-audit
        concurrency: 3
      metrics-aggregator:
        group-id: metrics-aggregator
        concurrency: 12
      metrics-alerting:
        group-id: metrics-alerting
        concurrency: 4
      alert-notifier:
        group-id: alert-notifier
        concurrency: 3

management:
  endpoints.web.exposure.include: health,metrics,prometheus
```

`AppKafkaProperties` is a `@ConfigurationProperties(prefix = "app.kafka")` record of `Map<String, Topic>` and `Map<String, Group>`. Topic fields: `name`, `partitions`, `replicas`, `retentionMs`, `compacted`. Group fields: `groupId`, `concurrency`.

`docker-compose.yml` publishes broker port `9092` only. It does not embed the application. Lab bootstrap stays `localhost:9092`.

Tests that need a broker use `@EmbeddedKafka` (or mock the template and store). Do not point `@SpringBootTest` at a shared production cluster.

## 4. Development Constraints & Coding Standards

* **API and event types:** `UserStatusEvent`, `MetricSample`, `Alert`, and the `PUT` body `StatusChange` are Java **`record`s**. Enums: `UserStatus`, `Severity`.
* **Listeners:** `@KafkaListener` topics, group ids, and concurrency are `${...}` placeholders. The Java source does not contain the integers 12, 6, 24, 4, or 3 as the topology.
* **Scheduling:** `@EnableScheduling` on the application class. `@Scheduled` is used for the demo metric generator (fixed rate 1000 ms) and the lag poll (fixed delay 15000 ms, initial delay 20000 ms). No extra scheduler library.
* **Demo load:** `MetricsPublisher` is `@ConditionalOnProperty(name = "app.demo.generate-metrics", havingValue = "true", matchIfMissing = true)`. Tests that boot the full context must set the property to `false` unless the test is about generation.
* **Logging:** SLF4J `LoggerFactory`. Lombok is not on the allow-list for v1.
* **Reference layout** (from the zip; keep these type names):
  * `KafkaDemoApplication`
  * `config/AppKafkaProperties`, `config/KafkaTopicConfig`, `config/KafkaListenerConfig`
  * `model/Events`
  * `producer/UserStatusPublisher`, `producer/MetricsPublisher`, `producer/AlertPublisher`
  * `consumer/UserStatusListeners`, `consumer/MetricsListeners`, `consumer/AlertListener`
  * `service/UserStatusStore`
  * `monitoring/MetricsAggregator`, `monitoring/ConsumerLagMonitor`
  * `web/UserStatusController`
