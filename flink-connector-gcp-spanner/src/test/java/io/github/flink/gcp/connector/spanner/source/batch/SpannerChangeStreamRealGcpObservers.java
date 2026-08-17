/*
 * Copyright 2026 The flink-gcp authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.github.flink.gcp.connector.spanner.source.batch;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.configuration.StateRecoveryOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.testutils.InMemoryReporter;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.util.Collector;

import com.google.gson.JsonParser;
import io.github.flink.gcp.connector.spanner.source.SpannerChangeStreamSource;
import io.github.flink.gcp.connector.spanner.source.changestream.DataChangeRecord;
import io.github.flink.gcp.connector.spanner.source.changestream.Mod;
import io.github.flink.gcp.connector.spanner.source.serializer.SpannerChangeStreamDeserializationSchema;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The MiniCluster observers the gated real-GCP Change Streams acceptance runs its job with, and the
 * run state they record for it to assert on.
 */
final class SpannerChangeStreamRealGcpObservers {

    private static final String REAL_PREFIX = "real:";

    private static final Map<String, Observation> OBSERVATIONS = new ConcurrentHashMap<>();
    private static final Map<String, AtomicLong> COMPLETED_CHECKPOINTS = new ConcurrentHashMap<>();
    private static final Set<String> ARMED_FAILURES = ConcurrentHashMap.newKeySet();
    private static final Set<String> FAILED = ConcurrentHashMap.newKeySet();
    private static final Map<String, RunMetrics> METRICS = new ConcurrentHashMap<>();

    private SpannerChangeStreamRealGcpObservers() {}

    static JobClient start(
            SpannerChangeStreamSource<String> source,
            String runId,
            @Nullable String savepoint,
            boolean restartOnce)
            throws Exception {
        Configuration configuration = new Configuration();
        if (restartOnce) {
            configuration.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
            configuration.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 1);
            configuration.set(
                    RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY,
                    java.time.Duration.ZERO);
        } else {
            configuration.set(RestartStrategyOptions.RESTART_STRATEGY, "none");
        }
        if (savepoint != null) {
            configuration.set(StateRecoveryOptions.SAVEPOINT_PATH, savepoint);
        }
        InMemoryReporter reporter = InMemoryReporter.createWithRetainedMetrics();
        reporter.addToConfiguration(configuration);

        StreamExecutionEnvironment environment =
                StreamExecutionEnvironment.createLocalEnvironment(configuration);
        environment.setParallelism(2);
        environment.enableCheckpointing(250L);
        environment
                .fromSource(source, WatermarkStrategy.noWatermarks(), "spanner-change-stream")
                .uid("spanner-change-stream")
                .map(new FailAfterCompletedCheckpoint(runId))
                .uid("checkpoint-failure")
                .keyBy(ignored -> 0)
                .process(new ObservingProcess(runId))
                .uid("change-stream-observer")
                .sinkTo(new DiscardingSink<>())
                .uid("discarding-sink");
        JobClient job = environment.executeAsync("Spanner Change Streams real-GCP " + runId);
        METRICS.put(runId, new RunMetrics(reporter, job.getJobID()));
        return job;
    }

    static SpannerChangeStreamDeserializationSchema<String> realDeserializer() {
        return new RealChangeDeserializer();
    }

    static void reset(String runId) {
        OBSERVATIONS.put(runId, new Observation());
        COMPLETED_CHECKPOINTS.put(runId, new AtomicLong(-1L));
        ARMED_FAILURES.remove(runId);
        FAILED.remove(runId);
        METRICS.remove(runId);
    }

    static int realRecordCount(String runId) {
        return observation(runId).realRecords.get();
    }

    static int uniqueIds(String runId) {
        return observation(runId).ids.size();
    }

    static Set<Long> ids(String runId) {
        return new LinkedHashSet<>(observation(runId).ids);
    }

    static int duplicateCount(String runId) {
        return realRecordCount(runId) - uniqueIds(runId);
    }

    static boolean watermarkAdvanced(String runId) {
        return observation(runId).watermarkAdvanced.get();
    }

    static int timestampMismatches(String runId) {
        return observation(runId).timestampMismatches.get();
    }

    static int allRecords(String runId) {
        return observation(runId).allRecords.get();
    }

    static long completedCheckpoint(String runId) {
        return COMPLETED_CHECKPOINTS.get(runId).get();
    }

    static void armFailure(String runId) {
        ARMED_FAILURES.add(runId);
    }

    static boolean failed(String runId) {
        return FAILED.contains(runId);
    }

    static long counter(String runId, String name) {
        return metrics(runId).counter(name);
    }

    static Map<String, Long> counterBySubtask(String runId, String name) {
        return metrics(runId).counterByIdentifier(name);
    }

    static Map<String, Long> sampleActiveQueries(String runId) {
        return metrics(runId).sampleGauges("activeChangeStreamQueries");
    }

    static Map<String, Long> peakActiveQueries(String runId) {
        return metrics(runId).peakGauges("activeChangeStreamQueries");
    }

    static String metricSummary(String runId) {
        return metrics(runId).summary();
    }

    static String jobStatus(JobClient job) {
        try {
            return job.getJobStatus().join().toString();
        } catch (RuntimeException e) {
            return "unreadable (" + e + ")";
        }
    }

    private static Observation observation(String runId) {
        Observation observation = OBSERVATIONS.get(runId);
        if (observation == null) {
            throw new IllegalStateException("Unknown Change Streams run " + runId);
        }
        return observation;
    }

    private static RunMetrics metrics(String runId) {
        RunMetrics metrics = METRICS.get(runId);
        if (metrics == null) {
            throw new IllegalStateException("No metric reporter for Change Streams run " + runId);
        }
        return metrics;
    }

    private static final class RealChangeDeserializer
            implements SpannerChangeStreamDeserializationSchema<String> {

        private static final long serialVersionUID = 1L;

        @Override
        public void deserialize(DataChangeRecord record, Collector<String> out) {
            for (Mod mod : record.getMods()) {
                long id =
                        JsonParser.parseString(mod.getKeysJson())
                                .getAsJsonObject()
                                .get("id")
                                .getAsLong();
                out.collect(REAL_PREFIX + id + ":" + record.getCommitTimestamp().toEpochMilli());
            }
        }

        @Override
        public TypeInformation<String> getProducedType() {
            return TypeInformation.of(String.class);
        }
    }

    private static final class FailAfterCompletedCheckpoint extends RichMapFunction<String, String>
            implements CheckpointListener {

        private static final long serialVersionUID = 1L;

        private final String runId;

        private FailAfterCompletedCheckpoint(String runId) {
            this.runId = runId;
        }

        @Override
        public String map(String value) {
            if (ARMED_FAILURES.contains(runId) && FAILED.add(runId)) {
                throw new IllegalStateException(
                        "Failing the gated Spanner Change Streams job after a completed"
                                + " checkpoint.");
            }
            return value;
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) {
            COMPLETED_CHECKPOINTS.get(runId).accumulateAndGet(checkpointId, Math::max);
        }
    }

    private static final class ObservingProcess
            extends KeyedProcessFunction<Integer, String, String> {

        private static final long serialVersionUID = 1L;

        private final String runId;
        private transient boolean timerRegistered;

        private ObservingProcess(String runId) {
            this.runId = runId;
        }

        @Override
        public void processElement(String value, Context context, Collector<String> out) {
            Observation observation = observation(runId);
            observation.allRecords.incrementAndGet();
            if (value.startsWith(REAL_PREFIX)) {
                int separator = value.indexOf(':', REAL_PREFIX.length());
                long id = Long.parseLong(value.substring(REAL_PREFIX.length(), separator));
                long commitTimestamp = Long.parseLong(value.substring(separator + 1));
                observation.realRecords.incrementAndGet();
                observation.ids.add(id);
                if (context.timestamp() == null || context.timestamp() != commitTimestamp) {
                    observation.timestampMismatches.incrementAndGet();
                }
                if (!timerRegistered && context.timestamp() != null) {
                    context.timerService().registerEventTimeTimer(context.timestamp() + 2_000L);
                    timerRegistered = true;
                }
            }
            out.collect(value);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext context, Collector<String> out) {
            observation(runId).watermarkAdvanced.set(true);
        }
    }

    private static final class Observation {

        private final Set<Long> ids = ConcurrentHashMap.newKeySet();
        private final AtomicInteger allRecords = new AtomicInteger();
        private final AtomicInteger realRecords = new AtomicInteger();
        private final AtomicInteger timestampMismatches = new AtomicInteger();
        private final AtomicBoolean watermarkAdvanced = new AtomicBoolean();
    }

    private static final class RunMetrics {

        private final InMemoryReporter reporter;
        private final JobID jobId;
        private final Map<String, Map<String, Long>> peakGauges = new ConcurrentHashMap<>();

        private RunMetrics(InMemoryReporter reporter, JobID jobId) {
            this.reporter = reporter;
            this.jobId = jobId;
        }

        private long counter(String name) {
            return namedMetrics(name).values().stream()
                    .filter(Counter.class::isInstance)
                    .map(Counter.class::cast)
                    .mapToLong(Counter::getCount)
                    .sum();
        }

        private Map<String, Long> counterByIdentifier(String name) {
            Map<String, Long> values = new LinkedHashMap<>();
            namedMetrics(name)
                    .forEach(
                            (identifier, metric) -> {
                                if (metric instanceof Counter) {
                                    values.put(identifier, ((Counter) metric).getCount());
                                }
                            });
            return values;
        }

        private Map<String, Long> sampleGauges(String name) {
            Map<String, Long> values = new LinkedHashMap<>();
            namedMetrics(name)
                    .forEach(
                            (identifier, metric) -> {
                                if (!(metric instanceof Gauge)) {
                                    return;
                                }
                                Object value = ((Gauge<?>) metric).getValue();
                                if (value instanceof Number) {
                                    values.put(identifier, ((Number) value).longValue());
                                }
                            });
            Map<String, Long> peaks =
                    peakGauges.computeIfAbsent(name, unused -> new ConcurrentHashMap<>());
            values.forEach((identifier, value) -> peaks.merge(identifier, value, Math::max));
            return values;
        }

        private Map<String, Metric> namedMetrics(String name) {
            Map<String, Metric> values = new LinkedHashMap<>();
            int duplicate = 0;
            for (Map.Entry<MetricGroup, Map<String, Metric>> group :
                    reporter.getMetricsByGroup().entrySet()) {
                String metricJobId = group.getKey().getAllVariables().get("<job_id>");
                if (metricJobId != null && !jobId.toString().equals(metricJobId)) {
                    continue;
                }
                Metric metric = group.getValue().get(name);
                if (metric == null) {
                    continue;
                }
                String identifier = group.getKey().getMetricIdentifier(name);
                values.put(identifier + '#' + duplicate++, metric);
            }
            return values;
        }

        private Map<String, Long> peakGauges(String name) {
            sampleGauges(name);
            return new LinkedHashMap<>(peakGauges.getOrDefault(name, Collections.emptyMap()));
        }

        private String summary() {
            return "queriesStarted="
                    + counterByIdentifier("changeStreamQueriesStarted")
                    + ", active="
                    + sampleGauges("activeChangeStreamQueries")
                    + ", peakActive="
                    + peakGauges("activeChangeStreamQueries")
                    + ", partitionsDiscovered="
                    + counter("changeStreamPartitionsDiscovered");
        }
    }
}
