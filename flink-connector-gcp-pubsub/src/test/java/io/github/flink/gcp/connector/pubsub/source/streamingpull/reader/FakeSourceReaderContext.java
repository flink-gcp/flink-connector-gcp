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

package io.github.flink.gcp.connector.pubsub.source.streamingpull.reader;

import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.groups.SourceReaderMetricGroup;
import org.apache.flink.util.SimpleUserCodeClassLoader;
import org.apache.flink.util.UserCodeClassLoader;

import java.util.ArrayList;
import java.util.List;

/**
 * Minimal {@link SourceReaderContext} for reader unit tests.
 *
 * <p>Source events sent to the coordinator are recorded in order. Split requests throw: the source
 * never sends one.
 */
final class FakeSourceReaderContext implements SourceReaderContext {

    private final SourceReaderMetricGroup metricGroup;
    private final Configuration configuration;
    private final List<SourceEvent> sentEvents = new ArrayList<>();
    private int indexOfSubtask;

    FakeSourceReaderContext(SourceReaderMetricGroup metricGroup) {
        this(metricGroup, new Configuration());
    }

    FakeSourceReaderContext(SourceReaderMetricGroup metricGroup, Configuration configuration) {
        this.metricGroup = metricGroup;
        this.configuration = configuration;
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
        return indexOfSubtask;
    }

    /** Sets the subtask index the reader sees; zero by default. */
    void setIndexOfSubtask(int indexOfSubtask) {
        this.indexOfSubtask = indexOfSubtask;
    }

    @Override
    public void sendSplitRequest() {
        throw new UnsupportedOperationException(
                "The Pub/Sub source assigns splits from the enumerator and never requests them.");
    }

    @Override
    public void sendSourceEventToCoordinator(SourceEvent sourceEvent) {
        sentEvents.add(sourceEvent);
    }

    /** Returns the source events sent to the coordinator, in order. */
    List<SourceEvent> sentEvents() {
        return sentEvents;
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
