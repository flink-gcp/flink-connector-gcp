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

package io.github.flink.gcp.connector.spanner.source.changestream.reader;

import io.github.flink.gcp.connector.spanner.source.changestream.DataChangeRecord;
import io.github.flink.gcp.connector.spanner.source.changestream.ModType;
import io.github.flink.gcp.connector.spanner.source.changestream.SpannerChangeStreamPartitionSplit;
import io.github.flink.gcp.connector.spanner.source.changestream.ValueCaptureType;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/** Deterministic, service-free partition script for source rescaling integration tests. */
public final class ScriptedSpannerChangeStreamQueryClientFactory
        implements SpannerChangeStreamQueryClientFactory {

    private static final long serialVersionUID = 1L;

    private final int partitionCount;

    public ScriptedSpannerChangeStreamQueryClientFactory(int partitionCount) {
        this.partitionCount = partitionCount;
    }

    @Override
    public SpannerChangeStreamQueryClient create() {
        return new ScriptedClient(partitionCount);
    }

    private static final class ScriptedClient implements SpannerChangeStreamQueryClient {

        private static final AtomicInteger THREAD_NUMBER = new AtomicInteger();

        private final int partitionCount;
        private final ScheduledExecutorService callbacks =
                Executors.newSingleThreadScheduledExecutor(
                        runnable -> {
                            Thread thread =
                                    new Thread(
                                            runnable,
                                            "spanner-change-stream-script-"
                                                    + THREAD_NUMBER.incrementAndGet());
                            thread.setDaemon(true);
                            return thread;
                        });

        private ScriptedClient(int partitionCount) {
            this.partitionCount = partitionCount;
        }

        @Override
        public QueryHandle open(
                SpannerChangeStreamPartitionSplit split,
                SpannerChangeStreamQueryListener listener) {
            ScriptedQuery query = new ScriptedQuery(split, listener, partitionCount, callbacks);
            // The production client invokes listeners asynchronously. Delay this callback until
            // after open() has returned so the reader has installed the query handle as well.
            callbacks.schedule(query::publishFirst, 10, TimeUnit.MILLISECONDS);
            return query;
        }

        @Override
        public void close() {
            callbacks.shutdownNow();
        }
    }

    private static final class ScriptedQuery implements SpannerChangeStreamQueryClient.QueryHandle {

        private final SpannerChangeStreamPartitionSplit split;
        private final SpannerChangeStreamQueryClient.SpannerChangeStreamQueryListener listener;
        private final int partitionCount;
        private final ScheduledExecutorService callbacks;

        private boolean initialPublished;
        private boolean closed;

        private ScriptedQuery(
                SpannerChangeStreamPartitionSplit split,
                SpannerChangeStreamQueryClient.SpannerChangeStreamQueryListener listener,
                int partitionCount,
                ScheduledExecutorService callbacks) {
            this.split = split;
            this.listener = listener;
            this.partitionCount = partitionCount;
            this.callbacks = callbacks;
        }

        private synchronized void publishFirst() {
            if (closed) {
                return;
            }
            initialPublished = true;
            if (split.getPartitionToken() == null) {
                List<SpannerChangeStreamRecord.Child> children = new ArrayList<>();
                for (int partition = 0; partition < partitionCount; partition++) {
                    children.add(
                            new SpannerChangeStreamRecord.Child(
                                    "partition-" + partition, Collections.emptyList(), true));
                }
                listener.record(
                        new SpannerChangeStreamRecord.Children(
                                split.getCurrentPosition(), children));
                return;
            }

            Instant timestamp = split.getCurrentPosition().plusMillis(1);
            DataChangeRecord record =
                    new DataChangeRecord(
                            timestamp,
                            split.splitId() + "|" + split.getCurrentPosition().toEpochMilli(),
                            "scripted-transaction",
                            true,
                            "scripted_table",
                            Collections.emptyList(),
                            Collections.emptyList(),
                            ModType.UPDATE,
                            ValueCaptureType.NEW_VALUES,
                            1,
                            1,
                            "",
                            false);
            listener.record(new SpannerChangeStreamRecord.Data(record));
        }

        @Override
        public synchronized void resume() {
            if (!closed && initialPublished && split.getPartitionToken() == null) {
                callbacks.execute(listener::finished);
            }
        }

        @Override
        public void cancel() {
            close();
        }

        @Override
        public synchronized void close() {
            closed = true;
        }
    }
}
