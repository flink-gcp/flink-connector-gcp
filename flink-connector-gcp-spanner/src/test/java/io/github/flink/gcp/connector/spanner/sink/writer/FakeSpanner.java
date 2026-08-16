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

package io.github.flink.gcp.connector.spanner.sink.writer;

import com.google.api.gax.core.ExecutorProvider;
import com.google.cloud.spanner.BatchClient;
import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.InstanceAdminClient;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;

import javax.annotation.Nullable;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * A {@link Spanner} service handle that hands out nothing and counts its closes.
 *
 * <p>Every method the factory does not call throws, which is the point: the interface is small
 * enough to implement outright, so the test states exactly which two operations it depends on
 * rather than leaving the rest silently answerable.
 */
final class FakeSpanner implements Spanner {

    private final AtomicInteger closes = new AtomicInteger();
    @Nullable private final RuntimeException databaseClientFailure;
    private boolean closed;

    FakeSpanner() {
        this(null);
    }

    FakeSpanner(@Nullable RuntimeException databaseClientFailure) {
        this.databaseClientFailure = databaseClientFailure;
    }

    int closes() {
        return closes.get();
    }

    @Override
    public DatabaseClient getDatabaseClient(DatabaseId db) {
        if (databaseClientFailure != null) {
            throw databaseClientFailure;
        }
        throw new UnsupportedOperationException("no database client is needed by this test");
    }

    @Override
    public void close() {
        closed = true;
        closes.incrementAndGet();
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public SpannerOptions getOptions() {
        throw new UnsupportedOperationException();
    }

    @Override
    public DatabaseAdminClient getDatabaseAdminClient() {
        throw new UnsupportedOperationException();
    }

    @Override
    public InstanceAdminClient getInstanceAdminClient() {
        throw new UnsupportedOperationException();
    }

    @Override
    public BatchClient getBatchClient(DatabaseId db) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ExecutorProvider getAsyncExecutorProvider() {
        throw new UnsupportedOperationException();
    }
}
