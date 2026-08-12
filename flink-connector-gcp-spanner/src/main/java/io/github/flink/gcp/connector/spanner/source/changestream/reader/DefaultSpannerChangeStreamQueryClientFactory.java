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

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.util.Preconditions;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.cloud.spanner.AsyncResultSet;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Options;
import com.google.cloud.spanner.ReadOnlyTransaction;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.TimestampBound;
import io.github.flink.gcp.connector.base.lifecycle.Closers;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.spanner.SpannerClients;
import io.github.flink.gcp.connector.spanner.SpannerDatabase;
import io.github.flink.gcp.connector.spanner.SpannerRpcPriority;
import io.github.flink.gcp.connector.spanner.source.changestream.SpannerChangeStreamPartitionSplit;

import javax.annotation.Nullable;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/** Opens the Spanner client and bounded callback executor owned by one reader subtask. */
@Internal
public final class DefaultSpannerChangeStreamQueryClientFactory
        implements SpannerChangeStreamQueryClientFactory {

    private static final long serialVersionUID = 1L;

    private final SpannerDatabase database;
    private final String changeStreamName;
    private final SpannerRpcPriority rpcPriority;
    private final int maxConcurrentQueries;
    @Nullable private final EmulatorEndpoint emulatorEndpoint;

    public DefaultSpannerChangeStreamQueryClientFactory(
            SpannerDatabase database,
            String changeStreamName,
            SpannerRpcPriority rpcPriority,
            int maxConcurrentQueries,
            @Nullable EmulatorEndpoint emulatorEndpoint) {
        this.database = Preconditions.checkNotNull(database, "database must not be null");
        this.changeStreamName =
                Preconditions.checkNotNull(changeStreamName, "changeStreamName must not be null");
        this.rpcPriority = Preconditions.checkNotNull(rpcPriority, "rpcPriority must not be null");
        Preconditions.checkArgument(
                maxConcurrentQueries > 0, "maxConcurrentQueries must be positive");
        this.maxConcurrentQueries = maxConcurrentQueries;
        this.emulatorEndpoint = emulatorEndpoint;
    }

    @Override
    public SpannerChangeStreamQueryClient create() throws Exception {
        Spanner spanner = SpannerClients.open(database, emulatorEndpoint);
        try {
            DatabaseClient client =
                    spanner.getDatabaseClient(
                            DatabaseId.of(
                                    database.getProject(),
                                    database.getInstance(),
                                    database.getDatabase()));
            return new DefaultClient(
                    spanner,
                    client,
                    client.getDialect(),
                    changeStreamName,
                    rpcPriority,
                    maxConcurrentQueries);
        } catch (Throwable error) {
            Closers.closeAllSuppressing(error, spanner::close);
            throw error;
        }
    }

    private static final class DefaultClient implements SpannerChangeStreamQueryClient {

        private final Spanner spanner;
        private final DatabaseClient client;
        private final Dialect dialect;
        private final String changeStreamName;
        private final SpannerRpcPriority rpcPriority;
        private final ExecutorService callbacks;
        private final Set<DefaultHandle> queries = new LinkedHashSet<>();
        private boolean closed;

        private DefaultClient(
                Spanner spanner,
                DatabaseClient client,
                Dialect dialect,
                String changeStreamName,
                SpannerRpcPriority rpcPriority,
                int maxConcurrentQueries) {
            this.spanner = spanner;
            this.client = client;
            this.dialect = dialect;
            this.changeStreamName = changeStreamName;
            this.rpcPriority = rpcPriority;
            this.callbacks =
                    Executors.newFixedThreadPool(maxConcurrentQueries, new CallbackThreadFactory());
        }

        @Override
        public synchronized QueryHandle open(
                SpannerChangeStreamPartitionSplit split, SpannerChangeStreamQueryListener listener)
                throws Exception {
            Preconditions.checkState(!closed, "Change Streams query client is closed.");
            ReadOnlyTransaction transaction =
                    client.singleUseReadOnlyTransaction(TimestampBound.strong());
            try {
                Statement statement =
                        SpannerChangeStreamStatements.forSplit(dialect, changeStreamName, split);
                AsyncResultSet rows =
                        transaction.executeQueryAsync(
                                statement, Options.priority(rpcPriority.toSpanner()));
                SpannerChangeStreamRecordDecoder decoder =
                        dialect == Dialect.GOOGLE_STANDARD_SQL
                                ? new SpannerChangeStreamGoogleSqlRecordDecoder()
                                : new SpannerChangeStreamPostgreSqlRecordDecoder();
                DefaultHandle handle =
                        new DefaultHandle(this, transaction, rows, decoder, listener);
                queries.add(handle);
                handle.start(callbacks);
                return handle;
            } catch (Throwable error) {
                Closers.closeAllSuppressing(error, transaction::close);
                throw error;
            }
        }

        private synchronized void released(DefaultHandle handle) {
            queries.remove(handle);
        }

        @Override
        public void close() throws Exception {
            ArrayList<DefaultHandle> closing;
            synchronized (this) {
                if (closed) {
                    return;
                }
                closed = true;
                closing = new ArrayList<>(queries);
                queries.clear();
            }
            ArrayList<AutoCloseable> resources = new ArrayList<>(closing);
            resources.add(callbacks::shutdownNow);
            resources.add(spanner::close);
            Closers.closeAll(resources);
        }
    }

    @VisibleForTesting
    static final class DefaultHandle implements SpannerChangeStreamQueryClient.QueryHandle {

        private final DefaultClient owner;
        private final ReadOnlyTransaction transaction;
        private final AsyncResultSet rows;
        private final SpannerChangeStreamRecordDecoder decoder;
        private final SpannerChangeStreamQueryClient.SpannerChangeStreamQueryListener listener;
        private final AtomicBoolean terminal = new AtomicBoolean();
        private final AtomicBoolean closed = new AtomicBoolean();

        private DefaultHandle(
                DefaultClient owner,
                ReadOnlyTransaction transaction,
                AsyncResultSet rows,
                SpannerChangeStreamRecordDecoder decoder,
                SpannerChangeStreamQueryClient.SpannerChangeStreamQueryListener listener) {
            this.owner = owner;
            this.transaction = transaction;
            this.rows = rows;
            this.decoder = decoder;
            this.listener = listener;
        }

        private void start(ExecutorService callbacks) {
            SerializingExecutor orderedCallbacks = new SerializingExecutor(callbacks);
            ApiFuture<Void> consumption =
                    rows.setCallback(
                            orderedCallbacks, resultSet -> ready(resultSet, orderedCallbacks));
            ApiFutures.addCallback(
                    consumption,
                    new ApiFutureCallback<Void>() {
                        @Override
                        public void onFailure(Throwable error) {
                            fail(error);
                            close();
                        }

                        @Override
                        public void onSuccess(Void ignored) {
                            close();
                        }
                    },
                    Runnable::run);
        }

        private AsyncResultSet.CallbackResponse ready(
                AsyncResultSet resultSet, Executor orderedCallbacks) {
            if (closed.get()) {
                return AsyncResultSet.CallbackResponse.DONE;
            }
            try {
                switch (resultSet.tryNext()) {
                    case OK:
                        SpannerChangeStreamRecord record = decoder.decode(resultSet);
                        return pauseBeforePublishing(record, orderedCallbacks, this::publish);
                    case DONE:
                        if (terminal.compareAndSet(false, true)) {
                            listener.finished();
                        }
                        return AsyncResultSet.CallbackResponse.DONE;
                    case NOT_READY:
                        return AsyncResultSet.CallbackResponse.CONTINUE;
                    default:
                        throw new AssertionError("Unknown AsyncResultSet cursor state.");
                }
            } catch (Throwable error) {
                fail(error);
                return AsyncResultSet.CallbackResponse.DONE;
            }
        }

        @VisibleForTesting
        static AsyncResultSet.CallbackResponse pauseBeforePublishing(
                SpannerChangeStreamRecord record,
                Executor orderedCallbacks,
                Consumer<SpannerChangeStreamRecord> publish) {
            // AsyncResultSet ignores resume() until its callback has returned PAUSE. Queue
            // publication behind this callback so the mailbox cannot acknowledge the handover
            // during the CONSUMING-to-PAUSED transition.
            orderedCallbacks.execute(() -> publish.accept(record));
            return AsyncResultSet.CallbackResponse.PAUSE;
        }

        private void publish(SpannerChangeStreamRecord record) {
            if (closed.get()) {
                return;
            }
            try {
                listener.record(record);
            } catch (Throwable error) {
                fail(error);
                close();
            }
        }

        private void fail(Throwable error) {
            if (!closed.get() && terminal.compareAndSet(false, true)) {
                listener.failed(error);
            }
        }

        @Override
        public void resume() {
            if (!closed.get() && !terminal.get()) {
                rows.resume();
            }
        }

        @Override
        public void cancel() {
            if (!closed.get()) {
                rows.cancel();
            }
        }

        @Override
        public void close() {
            if (!closed.compareAndSet(false, true)) {
                return;
            }
            try {
                rows.cancel();
            } finally {
                try {
                    transaction.close();
                } finally {
                    owner.released(this);
                }
            }
        }
    }

    /** Executes reentrant submissions only after the currently running task has returned. */
    @VisibleForTesting
    static final class SerializingExecutor implements Executor {

        private final Executor delegate;
        private final Deque<Runnable> queued = new ArrayDeque<>();
        private boolean running;

        SerializingExecutor(Executor delegate) {
            this.delegate = delegate;
        }

        @Override
        public synchronized void execute(Runnable command) {
            queued.addLast(
                    () -> {
                        try {
                            command.run();
                        } finally {
                            scheduleNext();
                        }
                    });
            if (!running) {
                running = true;
                scheduleNext();
            }
        }

        private synchronized void scheduleNext() {
            Runnable next = queued.pollFirst();
            if (next == null) {
                running = false;
            } else {
                delegate.execute(next);
            }
        }
    }

    private static final class CallbackThreadFactory implements ThreadFactory {

        private final AtomicInteger ids = new AtomicInteger();

        @Override
        public Thread newThread(Runnable runnable) {
            Thread thread = new Thread(runnable, "spanner-change-stream-" + ids.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        }
    }
}
