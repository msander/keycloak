package org.keycloak.valkey.metrics;

import java.time.Duration;
import java.util.Locale;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;

/**
 * Centralises Micrometer instrumentation for Valkey-backed providers.
 */
public final class ValkeyMetrics {

    private static final String METRIC_PREFIX = "keycloak.valkey";
    private static final String TIMER_NAME = METRIC_PREFIX + ".operation.duration";
    private static final String COUNTER_NAME = METRIC_PREFIX + ".operation.outcome";
    private static final String TAG_CATEGORY = "category";
    private static final String TAG_OPERATION = "operation";
    private static final String TAG_OUTCOME = "outcome";

    private ValkeyMetrics() {
    }

    /**
     * Starts a timer sample using the global Micrometer registry.
     */
    public static Timer.Sample startTimer() {
        return Timer.start(Metrics.globalRegistry);
    }

    /**
     * Records the elapsed time captured by the supplied sample.
     */
    public static void record(String category, String operation, Timer.Sample sample, Outcome outcome) {
        if (sample == null) {
            return;
        }
        Timer timer = Timer.builder(TIMER_NAME)
                .description("Latency of Valkey-backed operations.")
                .publishPercentileHistogram()
                .tag(TAG_CATEGORY, category)
                .tag(TAG_OPERATION, operation)
                .tag(TAG_OUTCOME, outcome.tagValue)
                .register(Metrics.globalRegistry);
        sample.stop(timer);
    }

    /**
     * Records the supplied duration for the given operation.
     */
    public static void record(String category, String operation, Duration duration, Outcome outcome) {
        Timer timer = Timer.builder(TIMER_NAME)
                .description("Latency of Valkey-backed operations.")
                .publishPercentileHistogram()
                .tag(TAG_CATEGORY, category)
                .tag(TAG_OPERATION, operation)
                .tag(TAG_OUTCOME, outcome.tagValue)
                .register(Metrics.globalRegistry);
        timer.record(duration);
    }

    /**
     * Increments an outcome counter for the specified operation.
     */
    public static void count(String category, String operation, Outcome outcome) {
        Counter counter = Counter.builder(COUNTER_NAME)
                .description("Outcome counter for Valkey-backed operations.")
                .tag(TAG_CATEGORY, category)
                .tag(TAG_OPERATION, operation)
                .tag(TAG_OUTCOME, outcome.tagValue)
                .register(Metrics.globalRegistry);
        counter.increment();
    }

    public enum Outcome {
        SUCCESS,
        FAILURE,
        TIMEOUT,
        ERROR;

        private final String tagValue;

        Outcome() {
            this.tagValue = name().toLowerCase(Locale.ROOT);
        }
    }
}

