/*
 * Copyright 2026 laughingman7743
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

package io.github.flink.gcp.connector.bigquery.source.reader;

import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.groups.SourceReaderMetricGroup;
import org.apache.flink.util.SimpleUserCodeClassLoader;
import org.apache.flink.util.UserCodeClassLoader;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Minimal {@link SourceReaderContext} for reader unit tests.
 *
 * <p>Unlike the Pub/Sub source's, {@link #sendSplitRequest()} records instead of throwing: asking
 * for the next split is the whole assignment protocol of this source, so a test has to be able to
 * count the asks.
 */
final class FakeSourceReaderContext implements SourceReaderContext {

    private final SourceReaderMetricGroup metricGroup;
    private final Configuration configuration;
    private final AtomicInteger splitRequests = new AtomicInteger();

    FakeSourceReaderContext(SourceReaderMetricGroup metricGroup) {
        this(metricGroup, new Configuration());
    }

    FakeSourceReaderContext(SourceReaderMetricGroup metricGroup, Configuration configuration) {
        this.metricGroup = metricGroup;
        this.configuration = configuration;
    }

    int splitRequests() {
        return splitRequests.get();
    }

    @Override
    public SourceReaderMetricGroup metricGroup() {
        return metricGroup;
    }

    @Override
    public Configuration getConfiguration() {
        return configuration;
    }

    @Override
    public String getLocalHostName() {
        return "localhost";
    }

    @Override
    public int getIndexOfSubtask() {
        return 0;
    }

    @Override
    public void sendSplitRequest() {
        splitRequests.incrementAndGet();
    }

    @Override
    public void sendSourceEventToCoordinator(SourceEvent sourceEvent) {
        throw new UnsupportedOperationException(
                "The BigQuery source sends no source events; add support here when it does.");
    }

    @Override
    public UserCodeClassLoader getUserCodeClassLoader() {
        return SimpleUserCodeClassLoader.create(getClass().getClassLoader());
    }

    @Override
    public int currentParallelism() {
        return 1;
    }
}
