package com.example.kafkademo.monitoring;

import com.example.kafkademo.model.Events.MetricSample;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.springframework.stereotype.Component;

/** Turns the metric stream into Prometheus gauges plus an end-to-end pipeline latency timer. */
@Component
public class MetricsAggregator {

    private final MeterRegistry registry;
    private final ConcurrentMap<String, Stats> stats = new ConcurrentHashMap<>();

    public MetricsAggregator(MeterRegistry registry) {
        this.registry = registry;
    }

    public void record(MetricSample sample) {
        Stats st = stats.computeIfAbsent(sample.service() + "|" + sample.metric(), key -> {
            Stats created = new Stats();
            Gauge.builder("app.service.metric.ewma", created, Stats::ewma)
                    .tags("service", sample.service(), "metric", sample.metric()).register(registry);
            Gauge.builder("app.service.metric.max", created, Stats::max)
                    .tags("service", sample.service(), "metric", sample.metric()).register(registry);
            return created;
        });
        st.add(sample.value());

        registry.timer("app.kafka.e2e.latency", "topic", "system-metrics")
                .record(Duration.between(sample.ts(), Instant.now()));
    }

    static final class Stats {
        private double ewma = Double.NaN;
        private double max;

        synchronized void add(double value) {
            ewma = Double.isNaN(ewma) ? value : 0.2 * value + 0.8 * ewma;
            max = Math.max(max, value);
        }

        synchronized double ewma() {
            return Double.isNaN(ewma) ? 0 : ewma;
        }

        synchronized double max() {
            return max;
        }
    }
}
