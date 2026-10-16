package com.example.kafkademo.web;

import static org.awaitility.Awaitility.await;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.example.kafkademo.model.Events.UserStatus;
import com.example.kafkademo.model.Events.UserStatusEvent;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.servlet.MockMvc;

@SpringBootTest
@AutoConfigureMockMvc
@EmbeddedKafka(partitions = 1, bootstrapServersProperty = "spring.kafka.bootstrap-servers")
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@TestPropertySource(properties = {
        "app.demo.generate-metrics=false",
        "app.monitoring.lag-enabled=false",
        "spring.task.scheduling.enabled=false",
        "spring.kafka.listener.missing-topics-fatal=false",
        "app.kafka.groups.user-status-processor.concurrency=1",
        "app.kafka.groups.user-status-audit.concurrency=1",
        "app.kafka.groups.metrics-aggregator.concurrency=1",
        "app.kafka.groups.metrics-alerting.concurrency=1",
        "app.kafka.groups.alert-notifier.concurrency=1"
})
class UserStatusPipelineIT {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private KafkaTemplate<String, Object> kafka;

    @Test
    void statusIsAppliedIgnoredWhenStaleAndReplacedWhenNewer() throws Exception {
        changeStatus("ONLINE", "login");
        awaitStatus("ONLINE");

        kafka.send("user-status-events", "u42",
                new UserStatusEvent("u42", UserStatus.OFFLINE, "stale", Instant.EPOCH, 1L))
                .get(10, TimeUnit.SECONDS);
        await().during(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(8)).untilAsserted(this::assertOnline);

        changeStatus("AWAY", "idle");
        awaitStatus("AWAY");
    }

    private void changeStatus(String status, String reason) throws Exception {
        mockMvc.perform(put("/api/users/u42/status")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"status\":\"" + status + "\",\"reason\":\"" + reason + "\"}"))
                .andExpect(status().isAccepted());
    }

    private void awaitStatus(String status) {
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                mockMvc.perform(get("/api/users/u42/status"))
                        .andExpect(status().isOk())
                        .andExpect(jsonPath("$.status").value(status)));
    }

    private void assertOnline() throws Exception {
        mockMvc.perform(get("/api/users/u42/status"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("ONLINE"));
    }
}
