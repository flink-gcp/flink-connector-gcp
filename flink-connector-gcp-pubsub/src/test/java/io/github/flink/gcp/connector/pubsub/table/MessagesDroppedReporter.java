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

package io.github.flink.gcp.connector.pubsub.table;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.metrics.reporter.MetricReporterFactory;

import io.github.flink.gcp.connector.pubsub.PubSubMetricNames;

import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Keeps the {@code messagesDropped} counters of the sources a local test cluster runs, so a test
 * can wait for a message to have been dropped rather than for time to pass.
 *
 * <p>Flink builds the reporter itself, from the factory this class also is, so what it keeps is
 * static; {@link #reset()} clears it before a test configures a cluster with {@link
 * #configuration()}.
 */
public final class MessagesDroppedReporter implements MetricReporter, MetricReporterFactory {

    private static final String NAME = "messages-dropped";

    private static final Set<Counter> COUNTERS = ConcurrentHashMap.newKeySet();

    /** Creates the reporter, as Flink's service loader does. */
    public MessagesDroppedReporter() {}

    /** A configuration that installs this reporter on the cluster a test starts. */
    static Configuration configuration() {
        Configuration configuration = new Configuration();
        configuration.set(MetricOptions.REPORTERS_LIST, NAME);
        MetricOptions.forReporter(configuration, NAME)
                .set(MetricOptions.REPORTER_FACTORY_CLASS, MessagesDroppedReporter.class.getName());
        return configuration;
    }

    static void reset() {
        COUNTERS.clear();
    }

    /** The messages the running sources have dropped so far, across their subtasks. */
    static long dropped() {
        long dropped = 0;
        for (Counter counter : COUNTERS) {
            dropped += counter.getCount();
        }
        return dropped;
    }

    @Override
    public MetricReporter createMetricReporter(Properties properties) {
        return new MessagesDroppedReporter();
    }

    @Override
    public void open(MetricConfig config) {}

    @Override
    public void close() {}

    @Override
    public void notifyOfAddedMetric(Metric metric, String metricName, MetricGroup group) {
        if (PubSubMetricNames.MESSAGES_DROPPED.equals(metricName) && metric instanceof Counter) {
            COUNTERS.add((Counter) metric);
        }
    }

    @Override
    public void notifyOfRemovedMetric(Metric metric, String metricName, MetricGroup group) {
        // Kept, so a count stays readable after the job that made it ends.
    }
}
