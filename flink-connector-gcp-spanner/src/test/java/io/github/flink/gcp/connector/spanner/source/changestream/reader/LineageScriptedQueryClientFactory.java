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

package io.github.flink.gcp.connector.spanner.source.changestream.reader;

import io.github.flink.gcp.connector.base.metrics.ClientMetrics;
import io.github.flink.gcp.connector.spanner.source.changestream.ChangeStreamPartitionSplit;
import io.github.flink.gcp.connector.spanner.source.changestream.DataChangeRecord;
import io.github.flink.gcp.connector.spanner.source.changestream.ModType;
import io.github.flink.gcp.connector.spanner.source.changestream.ValueCaptureType;

import javax.annotation.Nullable;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Scripts a two-level lineage that ends: the initial query reports {@code parent-0} .. {@code
 * parent-(n-1)}; each parent emits one data record naming itself, reports {@code child-i}, and
 * finishes; each child emits one data record naming itself and finishes. A child is therefore the
 * child of a partition other than the initial one, which is what the partition hand-off holds.
 */
public final class LineageScriptedQueryClientFactory
        implements SpannerChangeStreamQueryClientFactory {

    private static final long serialVersionUID = 1L;

    private final int parents;

    public LineageScriptedQueryClientFactory(int parents) {
        this.parents = parents;
    }

    @Override
    public SpannerChangeStreamQueryClient create(@Nullable ClientMetrics clientMetrics) {
        return new Client(parents);
    }

    private static final class Client implements SpannerChangeStreamQueryClient {

        private static final AtomicInteger THREAD_NUMBER = new AtomicInteger();

        private final int parents;
        private final ScheduledExecutorService callbacks =
                Executors.newSingleThreadScheduledExecutor(
                        runnable -> {
                            Thread thread =
                                    new Thread(
                                            runnable,
                                            "spanner-change-stream-lineage-"
                                                    + THREAD_NUMBER.incrementAndGet());
                            thread.setDaemon(true);
                            return thread;
                        });

        private Client(int parents) {
            this.parents = parents;
        }

        @Override
        public QueryHandle open(
                ChangeStreamPartitionSplit split, SpannerChangeStreamQueryListener listener) {
            Query query = new Query(split, listener, parents, callbacks);
            // As the production client does, call the listener only after open() has returned.
            callbacks.schedule(query::next, 10, TimeUnit.MILLISECONDS);
            return query;
        }

        @Override
        public void close() {
            callbacks.shutdownNow();
        }
    }

    private static final class Query implements SpannerChangeStreamQueryClient.QueryHandle {

        private final ChangeStreamPartitionSplit split;
        private final SpannerChangeStreamQueryClient.SpannerChangeStreamQueryListener listener;
        private final int parents;
        private final ScheduledExecutorService callbacks;
        private int step;
        private boolean closed;

        private Query(
                ChangeStreamPartitionSplit split,
                SpannerChangeStreamQueryClient.SpannerChangeStreamQueryListener listener,
                int parents,
                ScheduledExecutorService callbacks) {
            this.split = split;
            this.listener = listener;
            this.parents = parents;
            this.callbacks = callbacks;
        }

        /** Publishes the next scripted result; the reader asks for each after the last drains. */
        private synchronized void next() {
            if (closed) {
                return;
            }
            String token = split.getPartitionToken();
            Instant position = split.getCurrentPosition();
            int current = step++;
            if (token == null) {
                if (current == 0) {
                    List<SpannerChangeStreamRecord.Child> children = new ArrayList<>();
                    for (int i = 0; i < parents; i++) {
                        children.add(
                                new SpannerChangeStreamRecord.Child(
                                        "parent-" + i, Collections.emptyList(), true));
                    }
                    listener.record(new SpannerChangeStreamRecord.Children(position, children));
                } else {
                    listener.finished();
                }
            } else if (token.startsWith("parent-")) {
                if (current == 0) {
                    listener.record(data(token, position.plusMillis(1)));
                } else if (current == 1) {
                    String child = "child-" + token.substring("parent-".length());
                    listener.record(
                            new SpannerChangeStreamRecord.Children(
                                    position.plusMillis(2),
                                    Collections.singletonList(
                                            new SpannerChangeStreamRecord.Child(
                                                    child,
                                                    Collections.singletonList(token),
                                                    false))));
                } else {
                    listener.finished();
                }
            } else if (current == 0) {
                listener.record(data(token, position.plusMillis(1)));
            } else {
                listener.finished();
            }
        }

        private static SpannerChangeStreamRecord.Data data(String partition, Instant timestamp) {
            return new SpannerChangeStreamRecord.Data(
                    DataChangeRecord.builder()
                            .commitTimestamp(timestamp)
                            .recordSequence(partition + "|" + timestamp.toEpochMilli())
                            .serverTransactionId("scripted-transaction")
                            .lastRecordInTransactionInPartition(true)
                            .tableName(partition)
                            .columnTypes(Collections.emptyList())
                            .mods(Collections.emptyList())
                            .modType(ModType.UPDATE)
                            .valueCaptureType(ValueCaptureType.NEW_VALUES)
                            .numberOfRecordsInTransaction(1)
                            .numberOfPartitionsInTransaction(1)
                            .transactionTag("")
                            .systemTransaction(false)
                            .build());
        }

        @Override
        public synchronized void resume() {
            if (!closed) {
                callbacks.execute(this::next);
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
