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

package io.github.flink.gcp.connector.datastore.source.batch.reader;

import org.apache.flink.metrics.groups.SourceReaderMetricGroup;
import org.apache.flink.metrics.testutils.MetricListener;
import org.apache.flink.runtime.metrics.groups.InternalSourceReaderMetricGroup;

/** The reader's metrics registered on a group whose counters a test can read back. */
final class TestReaderMetrics {

    private final MetricListener listener = new MetricListener();
    private final SourceReaderMetricGroup metricGroup;
    private final DatastoreSourceReaderMetrics metrics;

    TestReaderMetrics() {
        this.metricGroup = InternalSourceReaderMetricGroup.mock(listener.getMetricGroup());
        this.metrics = new DatastoreSourceReaderMetrics(metricGroup);
    }

    DatastoreSourceReaderMetrics metrics() {
        return metrics;
    }

    long counter(String name) {
        return listener.getCounter(name)
                .orElseThrow(() -> new AssertionError("No counter named " + name + " registered."))
                .getCount();
    }
}
