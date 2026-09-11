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

package io.github.flink.gcp.connector.bigtable.sink.singlerow.committer;

import org.apache.flink.api.connector.sink2.Committer.CommitRequest;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;

import com.google.api.core.ApiFuture;
import com.google.api.core.SettableApiFuture;
import com.google.cloud.bigtable.data.v2.models.ConditionalRowMutation;
import com.google.cloud.bigtable.data.v2.models.ReadModifyWriteRow;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowMutationEntry;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.sink.BigtableStagedOptions;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.BigtableCommittable;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.BigtableRequestOptions;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.writer.SingleRowClient;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.writer.SingleRowClientFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Timeout(15)
class BigtableStagedCommitterTest {
    @Test
    void interruptCancelsOriginalFuturesAndNeverSubmitsPastTheBound() throws Exception {
        FakeFactory factory = new FakeFactory();
        var options =
                BigtableStagedOptions.builder()
                        .markerFamily("markers")
                        .requestOptions(
                                BigtableRequestOptions.builder().maxInFlightRequests(2).build())
                        .build();
        try (var committer =
                new BigtableStagedCommitter(
                        options,
                        (table, profile, marker, expected) -> {},
                        profile -> factory,
                        Map.of(),
                        new UnregisteredMetricsGroup())) {
            AtomicReference<Throwable> failure = new AtomicReference<>();
            Thread thread =
                    new Thread(
                            () -> {
                                try {
                                    committer.commit(
                                            List.of(
                                                    request("i", "a"),
                                                    request("i", "a"),
                                                    request("i", "a")));
                                } catch (Throwable caught) {
                                    failure.set(caught);
                                }
                            });
            thread.start();
            try {
                ApiFuture<Boolean> first = factory.calls.poll(5, TimeUnit.SECONDS);
                ApiFuture<Boolean> second = factory.calls.poll(5, TimeUnit.SECONDS);
                assertThat(first).isNotNull();
                assertThat(second).isNotNull();
                thread.interrupt();
                thread.join(5000);
                assertThat(thread.isAlive()).isFalse();
                assertThat(failure.get()).isInstanceOf(InterruptedException.class);
                assertThat(first.isCancelled()).isTrue();
                assertThat(second.isCancelled()).isTrue();
                assertThat(factory.calls).isEmpty();
            } finally {
                thread.interrupt();
                thread.join(5000);
            }
        }
        assertThat(factory.closes).isEqualTo(1);
    }

    @Test
    void synchronousSubmissionFailureCancelsEarlierRequestsAndPreservesFailure() throws Exception {
        FakeFactory factory = new FakeFactory();
        factory.throwOnSecond = true;
        var options = BigtableStagedOptions.builder().markerFamily("markers").build();
        try (var committer =
                new BigtableStagedCommitter(
                        options,
                        (table, profile, marker, expected) -> {},
                        profile -> factory,
                        Map.of(),
                        new UnregisteredMetricsGroup())) {
            assertThatThrownBy(
                            () -> committer.commit(List.of(request("i", "a"), request("i", "a"))))
                    .isInstanceOf(LinkageError.class)
                    .hasMessage("submission failure");
            assertThat(factory.calls.remove().isCancelled()).isTrue();
        }
        assertThat(factory.closes).isEqualTo(1);
    }

    @Test
    void capacityClosesPreviousClientBeforeCreatingNextAndUsesRestoredProfile() throws Exception {
        List<String> lifecycle = new ArrayList<>();
        var options =
                BigtableStagedOptions.builder()
                        .markerFamily("current-marker")
                        .requestOptions(
                                BigtableRequestOptions.builder().maxActiveInstances(1).build())
                        .build();
        var committer =
                new BigtableStagedCommitter(
                        options,
                        (table, profile, marker, expected) -> {
                            assertThat(marker).isEqualTo("markers");
                            lifecycle.add("validate:" + profile);
                        },
                        profile -> {
                            lifecycle.add("create:" + profile);
                            FakeFactory factory = new FakeFactory();
                            factory.immediate = true;
                            factory.onClose = () -> lifecycle.add("close:" + profile);
                            return factory;
                        },
                        Map.of(),
                        new UnregisteredMetricsGroup());
        try (committer) {
            committer.commit(
                    List.of(request("first", "old-profile"), request("second", "other-profile")));
        }
        assertThat(lifecycle)
                .containsExactly(
                        "create:old-profile",
                        "validate:old-profile",
                        "close:old-profile",
                        "create:other-profile",
                        "validate:other-profile",
                        "close:other-profile");
    }

    @Test
    void closeAttemptsEveryClientEvenWhenOneCloseFails() throws Exception {
        List<FakeFactory> factories = new ArrayList<>();
        var committer =
                new BigtableStagedCommitter(
                        BigtableStagedOptions.builder().markerFamily("markers").build(),
                        (table, profile, marker, expected) -> {},
                        profile -> {
                            FakeFactory factory = new FakeFactory();
                            factory.immediate = true;
                            factory.failClose = true;
                            factories.add(factory);
                            return factory;
                        },
                        Map.of(),
                        new UnregisteredMetricsGroup());
        committer.commit(List.of(request("first", "a"), request("second", "b")));
        assertThatThrownBy(committer::close).isInstanceOf(IOException.class);
        assertThat(factories).allSatisfy(factory -> assertThat(factory.closes).isEqualTo(1));
        committer.close();
        assertThat(factories).allSatisfy(factory -> assertThat(factory.closes).isEqualTo(1));
    }

    @Test
    void idleTableReleasesItsLeaseAndValidationWhileAnotherTableKeepsTheOwnerActive()
            throws Exception {
        FakeFactory factory = new FakeFactory();
        factory.immediate = true;
        AtomicLong clock = new AtomicLong();
        List<String> validations = new ArrayList<>();
        var options =
                BigtableStagedOptions.builder()
                        .markerFamily("markers")
                        .requestOptions(
                                BigtableRequestOptions.builder()
                                        .destinationIdleTimeout(Duration.ofSeconds(1))
                                        .build())
                        .build();
        try (var committer =
                new BigtableStagedCommitter(
                        options,
                        (table, profile, marker, expected) -> validations.add(table.getTable()),
                        profile -> factory,
                        Map.of(),
                        new UnregisteredMetricsGroup(),
                        clock::get)) {
            committer.commit(List.of(request("i", "a", "cold")));
            clock.set(500_000_000);
            committer.commit(List.of(request("i", "a", "hot")));
            clock.set(1_100_000_000);
            committer.commit(List.of(request("i", "a", "hot")));
            assertThat(factory.released).containsExactly(TableDestination.of("p", "i", "cold"));
            assertThat(factory.closes).isZero();
            committer.commit(List.of(request("i", "a", "cold")));
            assertThat(validations).containsExactly("cold", "hot", "cold");
            assertThat(factory.created)
                    .containsExactly(
                            TableDestination.of("p", "i", "cold"),
                            TableDestination.of("p", "i", "hot"),
                            TableDestination.of("p", "i", "cold"));
            clock.set(3_000_000_000L);
            committer.commit(List.of());
            assertThat(factory.released).hasSize(3);
            assertThat(factory.closes).isEqualTo(1);
        }
        assertThat(factory.closes).isEqualTo(1);
    }

    private static Request request(String instance, String profile) throws IOException {
        return request(instance, profile, "t");
    }

    private static Request request(String instance, String profile, String table)
            throws IOException {
        return new Request(
                BigtableCommittable.stage(
                        TableDestination.of("p", instance, table),
                        profile,
                        "markers",
                        RowMutationEntry.create("r").setCell("data", "q", 1000, "v").toProto(),
                        new SecureRandom()));
    }

    private static final class Request implements CommitRequest<BigtableCommittable> {
        private final BigtableCommittable value;

        Request(BigtableCommittable value) {
            this.value = value;
        }

        @Override
        public BigtableCommittable getCommittable() {
            return value;
        }

        @Override
        public int getNumberOfRetries() {
            return 0;
        }

        @Override
        public void signalAlreadyCommitted() {}

        @Override
        public void retryLater() {
            throw new AssertionError("Must fail the commit");
        }

        @Override
        public void updateAndRetryLater(BigtableCommittable value) {
            throw new AssertionError("Must retain identity");
        }

        @Override
        public void signalFailedWithKnownReason(Throwable failure) {
            throw new AssertionError("Must fail the commit");
        }

        @Override
        public void signalFailedWithUnknownReason(Throwable failure) {
            throw new AssertionError("Must fail the commit");
        }
    }

    private static final class FakeFactory implements SingleRowClientFactory, SingleRowClient {
        private static final long serialVersionUID = 1L;
        final LinkedBlockingQueue<SettableApiFuture<Boolean>> calls = new LinkedBlockingQueue<>();
        final List<TableDestination> created = new ArrayList<>();
        final List<TableDestination> released = new ArrayList<>();
        int closes;
        boolean throwOnSecond;
        boolean immediate;
        boolean failClose;
        Runnable onClose = () -> {};

        @Override
        public SingleRowClient create(TableDestination table) {
            created.add(table);
            return this;
        }

        @Override
        public void release(TableDestination table) {
            released.add(table);
        }

        @Override
        public void close() throws IOException {
            closes++;
            onClose.run();
            if (failClose) {
                throw new IOException("close failed");
            }
        }

        @Override
        public ApiFuture<Boolean> checkAndMutateRow(ConditionalRowMutation mutation) {
            if (throwOnSecond && !calls.isEmpty()) {
                throw new LinkageError("submission failure");
            }
            SettableApiFuture<Boolean> future = SettableApiFuture.create();
            calls.add(future);
            if (immediate) {
                future.set(false);
            }
            return future;
        }

        @Override
        public ApiFuture<Row> readModifyWriteRow(ReadModifyWriteRow mutation) {
            throw new AssertionError("Only conditional requests protect replay");
        }
    }
}
