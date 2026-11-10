package com.example.kafkademo.web;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.example.kafkademo.model.Events.UserStatus;
import com.example.kafkademo.model.Events.UserStatusEvent;
import com.example.kafkademo.producer.UserStatusPublisher;
import com.example.kafkademo.service.UserStatusStore;
import java.time.Instant;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

@WebMvcTest(UserStatusController.class)
class UserStatusControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private UserStatusPublisher publisher;

    @MockBean
    private UserStatusStore store;

    @Test
    void putQueuesTheStatusChange() throws Exception {
        mockMvc.perform(put("/api/users/u42/status")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"status\":\"ONLINE\",\"reason\":\"login\"}"))
                .andExpect(status().isAccepted());
        verify(publisher).publish("u42", UserStatus.ONLINE, "login");
    }

    @Test
    void getMissingUserIsNotFound() throws Exception {
        when(store.get("u42")).thenReturn(Optional.empty());
        mockMvc.perform(get("/api/users/u42/status")).andExpect(status().isNotFound());
    }

    @Test
    void getReturnsTheStoredEvent() throws Exception {
        UserStatusEvent event = new UserStatusEvent("u42", UserStatus.ONLINE, "login", Instant.parse("2026-10-15T00:00:00Z"), 10L);
        when(store.get("u42")).thenReturn(Optional.of(event));
        mockMvc.perform(get("/api/users/u42/status"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.userId").value("u42"))
                .andExpect(jsonPath("$.status").value("ONLINE"))
                .andExpect(jsonPath("$.reason").value("login"));
    }
}
