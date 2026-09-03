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

package io.github.flink.gcp.connector.bigtable.sink.singlerow.writer;

import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.retrying.RetrySettings;
import com.google.api.gax.rpc.UnaryCallSettings;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.models.TableId;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.bigtable.BigtableClientReaper;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.BigtableRequestOptions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for the settings {@link DefaultSingleRowClientFactory} builds and for the client pool it
 * keeps — the options-to-settings mapping is otherwise invisible (a deadline that never reaches the
 * client looks exactly like one that does), and the pool's sharing is observable only by identity.
 *
 * <p>{@code @Timeout} because the pool tests build and close a real SDK client, and the reasoning
 * of {@code DefaultMutationBatcherFactoryTest} about what {@code SAME_THREAD} buys applies
 * unchanged.
 */
@Timeout(30)
class DefaultSingleRowClientFactoryTest {

    private static final TableDestination TABLE = TableDestination.of("p", "i", "orders");

    /**
     * What google-cloud-bigtable ships for both RPCs, measured at 2.81.0 (2026-09-03). The
     * runtime's "no retry is added" claim rests on the codes being empty; a BOM bump that changes
     * either value fails here, where the claim is re-examined, rather than in production.
     */
    private static final Duration SDK_DEFAULT_TOTAL_TIMEOUT = Duration.ofSeconds(20);

    /** Read-and-clear, unconditionally; see {@code DefaultMutationBatcherFactoryTest}. */
    @AfterEach
    void clearAnyInterruptThisClassSet() {
        Thread.interrupted();
    }

    @Test
    void theClientsOwnDefaultsAreWhatTheSettingsAreBuiltOn() {
        BigtableDataSettings shipped =
                BigtableDataSettings.newBuilderForEmulator("localhost", 1)
                        .setProjectId("p")
                        .setInstanceId("i")
                        .build();

        UnaryCallSettings<?, ?> checkAndMutate =
                shipped.getStubSettings().checkAndMutateRowSettings();
        UnaryCallSettings<?, ?> readModifyWrite =
                shipped.getStubSettings().readModifyWriteRowSettings();
        assertThat(checkAndMutate.getRetryableCodes()).isEmpty();
        assertThat(readModifyWrite.getRetryableCodes()).isEmpty();
        assertThat(checkAndMutate.getRetrySettings().getTotalTimeoutDuration())
                .isEqualTo(SDK_DEFAULT_TOTAL_TIMEOUT);
        assertThat(readModifyWrite.getRetrySettings().getTotalTimeoutDuration())
                .isEqualTo(SDK_DEFAULT_TOTAL_TIMEOUT);
    }

    @Test
    void appliesTheRequestTimeoutAsOneAttemptOnBothRpcs() {
        Duration requestTimeout = Duration.ofSeconds(7);
        BigtableDataSettings settings =
                factory(
                                null,
                                BigtableRequestOptions.builder()
                                        .requestTimeout(requestTimeout)
                                        .build(),
                                "localhost:8086")
                        .settings(TABLE);

        for (UnaryCallSettings<?, ?> rpc :
                new UnaryCallSettings<?, ?>[] {
                    settings.getStubSettings().checkAndMutateRowSettings(),
                    settings.getStubSettings().readModifyWriteRowSettings()
                }) {
            RetrySettings retry = rpc.getRetrySettings();
            assertThat(retry.getTotalTimeoutDuration()).isEqualTo(requestTimeout);
            assertThat(retry.getInitialRpcTimeoutDuration()).isEqualTo(requestTimeout);
            assertThat(retry.getMaxRpcTimeoutDuration()).isEqualTo(requestTimeout);
            assertThat(retry.getMaxAttempts()).isEqualTo(1);
            assertThat(rpc.getRetryableCodes()).isEmpty();
        }
    }

    @Test
    void leavesTheBatchingRpcAsTheClientShippedIt() {
        // The deadline is this family's own; the other RPCs of the shared client keep theirs.
        BigtableDataSettings settings =
                factory(
                                null,
                                BigtableRequestOptions.builder()
                                        .requestTimeout(Duration.ofSeconds(7))
                                        .build(),
                                "localhost:8086")
                        .settings(TABLE);

        assertThat(settings.getStubSettings().mutateRowSettings().getRetryableCodes()).isNotEmpty();
    }

    @Test
    void carriesTheDestinationAndTheApplicationProfile() {
        BigtableDataSettings settings =
                factory("request-profile", BigtableRequestOptions.builder().build(), "localhost:1")
                        .settings(TABLE);

        assertThat(settings.getProjectId()).isEqualTo("p");
        assertThat(settings.getInstanceId()).isEqualTo("i");
        assertThat(settings.getAppProfileId()).isEqualTo("request-profile");
    }

    @Test
    void pointsTheClientAtTheEmulatorEndpoint() {
        BigtableDataSettings settings =
                factory(null, BigtableRequestOptions.builder().build(), "bigtable.example:9035")
                        .settings(TABLE);

        assertThat(settings.getStubSettings().getEndpoint()).isEqualTo("bigtable.example:9035");
        assertThat(settings.getStubSettings().getCredentialsProvider().getClass().getSimpleName())
                .isEqualTo("NoCredentialsProvider");
    }

    @Test
    void injectsTheRuntimeCredentialProvider() {
        NoCredentialsProvider provider = NoCredentialsProvider.create();
        DefaultSingleRowClientFactory factory =
                new DefaultSingleRowClientFactory(
                        null, BigtableRequestOptions.builder().build(), null, provider);

        assertThat(factory.settings(TABLE).getStubSettings().getCredentialsProvider())
                .isSameAs(provider);
    }

    @Test
    void sharesOneClientAcrossTheTablesOfAnInstanceAndBuildsOnePerInstance() throws Exception {
        DefaultSingleRowClientFactory factory =
                factory(null, BigtableRequestOptions.builder().build(), "localhost:1");

        try {
            BigtableDataClient orders = factory.client(TABLE);

            assertThat(factory.client(TableDestination.of("p", "i", "events"))).isSameAs(orders);
            assertThat(factory.client(TableDestination.of("p", "other", "orders")))
                    .isNotSameAs(orders);
            assertThat(factory.client(TableDestination.of("other", "i", "orders")))
                    .isNotSameAs(orders);
            assertThat(factory.activeClientCount()).isEqualTo(3);
        } finally {
            factory.close();
        }
    }

    @Test
    void releasesAnInstanceClientAfterItsLastTable() throws Exception {
        DefaultSingleRowClientFactory factory =
                factory(null, BigtableRequestOptions.builder().build(), "localhost:1");
        TableDestination events = TableDestination.of("p", "i", "events");
        BigtableDataClient client = factory.client(TABLE);
        factory.create(TABLE);
        factory.create(events);

        factory.release(TABLE);

        assertThat(factory.activeClientCount()).isEqualTo(1);
        // Still open: the client's own close is unobservable, so the consequence of it stands in
        // — a closed client's executor refuses the work a batcher schedules on it.
        client.newBulkMutationBatcher(TableId.of("still-alive")).close();

        factory.release(events);
        factory.awaitReleasedClients();

        assertThat(factory.activeClientCount()).isZero();
        assertThatThrownBy(() -> client.newBulkMutationBatcher(TableId.of("closed")))
                .isInstanceOf(RejectedExecutionException.class);

        factory.create(TABLE);
        assertThat(factory.client(TABLE)).isNotSameAs(client);
        factory.release(TABLE);
        factory.close();
    }

    @Test
    void aReleaseWithoutALeaseIsADefect() throws Exception {
        DefaultSingleRowClientFactory factory =
                factory(null, BigtableRequestOptions.builder().build(), "localhost:1");

        assertThatThrownBy(() -> factory.release(TABLE))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("No Bigtable client exists");

        try {
            factory.create(TABLE);
            factory.release(TABLE);
            assertThatThrownBy(() -> factory.release(TABLE))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("No Bigtable client lease exists");
        } finally {
            factory.close();
        }
    }

    @Test
    void aNewInstanceWaitsForAReleasedClientsSlotAtTheCap() throws Exception {
        // maxActiveInstances bounds open-or-closing clients: the second instance's creation takes
        // the permit the first instance's close returns, rather than opening a client early. The
        // wait is observed where it happens — the contender parked in acquireSlot — so a factory
        // that never took the permit fails here rather than passing by creating at once.
        DefaultSingleRowClientFactory factory =
                factory(
                        null,
                        BigtableRequestOptions.builder().maxActiveInstances(1).build(),
                        "localhost:1");
        TableDestination other = TableDestination.of("p", "other", "orders");
        CompletableFuture<SingleRowClient> contended = new CompletableFuture<>();
        Thread contender =
                new Thread(
                        () -> {
                            try {
                                contended.complete(factory.create(other));
                            } catch (Throwable t) {
                                contended.completeExceptionally(t);
                            }
                        },
                        "contender");

        try {
            factory.create(TABLE);
            contender.start();
            awaitOrFail(
                    () -> isParkedIn(contender, BigtableClientReaper.class, "acquireSlot"),
                    "the second instance's creation never waited for a slot");
            assertThat(contended).isNotDone();

            // The release schedules the first client's close; its physical close returns the
            // permit, which is the only thing that lets the contender through. The two threads
            // touch the pool in that order and never at once.
            factory.release(TABLE);
            contender.join(Duration.ofSeconds(20).toMillis());

            assertThat(contended).isCompletedWithValueMatching(client -> client != null);
            assertThat(factory.activeClientCount()).isEqualTo(1);
            factory.release(other);
        } finally {
            contender.interrupt();
            factory.close();
        }
    }

    @Test
    void aFailedCreationReturnsItsSlotSoTheNextAttemptIsNotStranded() throws Exception {
        // The permit is taken before the client is built and given back when the build fails; a
        // factory that kept it would park the retry forever at maxActiveInstances(1). The retry
        // runs on a bounded thread so a kept permit fails here by name, and close() comes last,
        // outside any finally: closeAll waits for every permit, so under the same defect it would
        // turn the failure into a hang.
        AtomicInteger attempts = new AtomicInteger();
        DefaultSingleRowClientFactory factory =
                new DefaultSingleRowClientFactory(
                        null,
                        BigtableRequestOptions.builder().maxActiveInstances(1).build(),
                        null,
                        () -> {
                            throw new IOException(
                                    "credentials attempt " + attempts.incrementAndGet());
                        });

        assertThatThrownBy(() -> factory.create(TABLE))
                .isInstanceOf(IOException.class)
                .hasMessage("credentials attempt 1");
        assertThat(factory.activeClientCount()).isZero();

        CompletableFuture<Throwable> retried = new CompletableFuture<>();
        Thread retry =
                new Thread(
                        () -> {
                            try {
                                factory.create(TABLE);
                                retried.complete(null);
                            } catch (Throwable t) {
                                retried.complete(t);
                            }
                        },
                        "retry");
        retry.start();
        retry.join(Duration.ofSeconds(20).toMillis());
        if (retry.isAlive()) {
            retry.interrupt();
            throw new AssertionError(
                    "the retry never got a slot: the failed creation kept its permit");
        }

        assertThat(retried.get())
                .isInstanceOf(IOException.class)
                .hasMessage("credentials attempt 2");
        assertThat(factory.activeClientCount()).isZero();
        factory.close();
    }

    private static boolean isParkedIn(Thread thread, Class<?> owner, String method) {
        Thread.State state = thread.getState();
        if (state != Thread.State.WAITING && state != Thread.State.TIMED_WAITING) {
            return false;
        }
        for (StackTraceElement frame : thread.getStackTrace()) {
            if (frame.getClassName().equals(owner.getName())
                    && frame.getMethodName().equals(method)) {
                return true;
            }
        }
        return false;
    }

    private static void awaitOrFail(BooleanSupplier condition, String whatNeverHappened)
            throws InterruptedException {
        long deadlineNanos = System.nanoTime() + Duration.ofSeconds(20).toNanos();
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() - deadlineNanos > 0) {
                throw new AssertionError(whatNeverHappened);
            }
            Thread.sleep(1);
        }
    }

    @Test
    void closesEveryClientItBuiltAndForgetsThem() throws Exception {
        DefaultSingleRowClientFactory factory =
                factory(null, BigtableRequestOptions.builder().build(), "localhost:1");
        BigtableDataClient client = factory.client(TABLE);
        factory.create(TABLE);

        factory.close();

        assertThatThrownBy(() -> client.newBulkMutationBatcher(TableId.of("orders")))
                .isInstanceOf(RejectedExecutionException.class);
        assertThatCode(factory::close).doesNotThrowAnyException();
        assertThat(factory.activeClientCount()).isZero();
        BigtableDataClient rebuilt = factory.client(TABLE);
        assertThat(rebuilt).isNotSameAs(client);
        factory.close();
    }

    private static DefaultSingleRowClientFactory factory(
            String appProfileId, BigtableRequestOptions options, String emulatorEndpoint) {
        return new DefaultSingleRowClientFactory(
                appProfileId,
                options,
                EmulatorEndpoint.parse(emulatorEndpoint, "emulatorEndpoint"),
                null);
    }
}
