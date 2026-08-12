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

package io.github.flink.gcp.connector.spanner.source.changestream.enumerator;

import com.google.api.gax.core.ExecutorProvider;
import com.google.cloud.spanner.BatchClient;
import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.InstanceAdminClient;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import io.github.flink.gcp.connector.spanner.SpannerDatabase;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DefaultSpannerChangeStreamCoordinatorClientFactoryTest {

    @Test
    void usesSevenDaysWhenNoAbsentRetentionFallbackIsConfigured() {
        SpannerDatabase database = SpannerDatabase.of("project", "instance", "database");

        DefaultSpannerChangeStreamCoordinatorClientFactory factoryWithDefault =
                new DefaultSpannerChangeStreamCoordinatorClientFactory(database, "orders", null);
        DefaultSpannerChangeStreamCoordinatorClientFactory factoryWithExplicitFallback =
                new DefaultSpannerChangeStreamCoordinatorClientFactory(
                        database, "orders", Duration.ofDays(7), null);

        assertThat(factoryWithDefault)
                .usingRecursiveComparison()
                .isEqualTo(factoryWithExplicitFallback);
    }

    @Test
    void closesTheServiceHandleWhenTheDatabaseClientCannotBeOpened() {
        IllegalStateException failure = new IllegalStateException("no such database");
        FailingSpanner spanner = new FailingSpanner(failure);
        DefaultSpannerChangeStreamCoordinatorClientFactory factory =
                new DefaultSpannerChangeStreamCoordinatorClientFactory(
                        SpannerDatabase.of("project", "instance", "database"),
                        "orders",
                        Duration.ofDays(7),
                        null);

        assertThatThrownBy(() -> factory.create(spanner)).isSameAs(failure);
        assertThat(spanner.closeCalls).hasValue(1);
    }

    private static final class FailingSpanner implements Spanner {

        private final RuntimeException failure;
        private final AtomicInteger closeCalls = new AtomicInteger();

        private FailingSpanner(RuntimeException failure) {
            this.failure = failure;
        }

        @Override
        public DatabaseClient getDatabaseClient(DatabaseId databaseId) {
            throw failure;
        }

        @Override
        public void close() {
            closeCalls.incrementAndGet();
        }

        @Override
        public boolean isClosed() {
            return closeCalls.get() > 0;
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
        public BatchClient getBatchClient(DatabaseId databaseId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public ExecutorProvider getAsyncExecutorProvider() {
            throw new UnsupportedOperationException();
        }
    }
}
