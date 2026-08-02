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

package io.github.flink.gcp.connector.bigtable.sink.writer;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.metrics.groups.OperatorIOMetricGroup;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;

/**
 * A {@link SinkWriterMetricGroup} whose counters are stable instances, so a test can read what the
 * writer incremented.
 *
 * <p>{@code UnregisteredMetricsGroup.createSinkWriterMetricGroup()} cannot be used for that: it
 * hands out a fresh {@code SimpleCounter} on every call, so the counter the writer holds is
 * unreachable from the group afterwards. Everything else is inherited from it, including the
 * methods this sink never calls.
 */
final class RecordingSinkWriterMetricGroup extends UnregisteredMetricsGroup
        implements SinkWriterMetricGroup {

    private final Counter numRecordsSend = new SimpleCounter();
    private final Counter numBytesSend = new SimpleCounter();
    private final Counter numRecordsSendErrors = new SimpleCounter();

    @Override
    public Counter getNumRecordsOutErrorsCounter() {
        return numRecordsSendErrors;
    }

    @Override
    public Counter getNumRecordsSendErrorsCounter() {
        return numRecordsSendErrors;
    }

    @Override
    public Counter getNumRecordsSendCounter() {
        return numRecordsSend;
    }

    @Override
    public Counter getNumBytesSendCounter() {
        return numBytesSend;
    }

    @Override
    public void setCurrentSendTimeGauge(Gauge<Long> currentSendTimeGauge) {}

    @Override
    public OperatorIOMetricGroup getIOMetricGroup() {
        return UnregisteredMetricsGroup.createOperatorIOMetricGroup();
    }
}
