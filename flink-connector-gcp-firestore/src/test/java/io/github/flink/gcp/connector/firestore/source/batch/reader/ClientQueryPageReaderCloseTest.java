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

package io.github.flink.gcp.connector.firestore.source.batch.reader;

import org.apache.flink.util.InstantiationUtil;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.cloud.Timestamp;
import com.google.cloud.firestore.QuerySnapshot;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The page reader's close ends a page still in flight: a fetcher can outlive the reader's close
 * timeout, and the client closes with {@code shutdownNow()}, which drops a retry gax has scheduled
 * without ever completing the read's future, so the close cancels the page's own wait first
 * (#2330). {@code ClientQueryPageReaderCloseITCase} closes a real client mid-page.
 *
 * <p>The page's query is never read by the reads these tests start, so it is {@code null}.
 */
@Timeout(60)
class ClientQueryPageReaderCloseTest {

    private static final Timestamp READ_TIME = Timestamp.ofTimeSecondsAndNanos(1_700_000_000, 0);

    /**
     * A read that never completes and ignores cancellation, stricter than the library's own future,
     * which a cancellation completes: the page's wait must end on the derived future, and the read
     * is still asked to cancel.
     */
    @Test
    void closeEndsAPageWhoseReadMissesTheCancellation() throws Exception {
        DeafFuture read = new DeafFuture();
        CountDownLatch started = new CountDownLatch(1);
        ClientQueryPageReader reader =
                new ClientQueryPageReader(
                        (page, readTime) -> {
                            started.countDown();
                            return read;
                        });
        CompletableFuture<Throwable> page =
                CompletableFuture.supplyAsync(
                        () -> {
                            try {
                                reader.read(null, READ_TIME);
                                return null;
                            } catch (Throwable t) {
                                return t;
                            }
                        });
        assertThat(started.await(10, TimeUnit.SECONDS)).as("the page's read started").isTrue();

        reader.close();

        // The read is started and recorded in one critical section under the reader's monitor, so
        // the close, which takes the same monitor, always finds it.
        assertThat(page.get(10, TimeUnit.SECONDS))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("closed while a page was being read");
        assertThat(read.cancelRequested).as("the read was asked to cancel").isTrue();
        assertThatThrownBy(() -> reader.read(null, READ_TIME))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("closed before use");
    }

    /** Every page in flight ends on close, not only the last one started. */
    @Test
    void closeEndsEveryPageInFlight() throws Exception {
        DeafFuture first = new DeafFuture();
        DeafFuture second = new DeafFuture();
        CountDownLatch started = new CountDownLatch(2);
        ClientQueryPageReader reader =
                new ClientQueryPageReader(
                        (page, readTime) -> {
                            started.countDown();
                            return started.getCount() == 1 ? first : second;
                        });
        CompletableFuture<Throwable> firstPage = readOnAnotherThread(reader);
        CompletableFuture<Throwable> secondPage = readOnAnotherThread(reader);
        assertThat(started.await(10, TimeUnit.SECONDS)).as("both reads started").isTrue();

        reader.close();

        assertThat(firstPage.get(10, TimeUnit.SECONDS))
                .hasMessageContaining("closed while a page was being read");
        assertThat(secondPage.get(10, TimeUnit.SECONDS))
                .hasMessageContaining("closed while a page was being read");
        assertThat(first.cancelRequested).as("the first read was asked to cancel").isTrue();
        assertThat(second.cancelRequested).as("the second read was asked to cancel").isTrue();
    }

    @Test
    void aReadsFailureFailsThePageWithItsCause() throws Exception {
        IllegalStateException failure = new IllegalStateException("refused");
        ClientQueryPageReader reader =
                new ClientQueryPageReader(
                        (page, readTime) -> ApiFutures.immediateFailedFuture(failure));
        try {
            assertThatThrownBy(() -> reader.read(null, READ_TIME))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("Failed to read a page")
                    .hasCause(failure);
        } finally {
            reader.close();
        }
    }

    /** An interrupted wait cancels the read and keeps the interrupt. */
    @Test
    void anInterruptCancelsThePage() throws Exception {
        DeafFuture read = new DeafFuture();
        ClientQueryPageReader reader = new ClientQueryPageReader((page, readTime) -> read);
        Thread.currentThread().interrupt();
        try {
            assertThatThrownBy(() -> reader.read(null, READ_TIME))
                    .isInstanceOf(InterruptedIOException.class);
            assertThat(Thread.currentThread().isInterrupted()).as("the interrupt").isTrue();
            assertThat(read.cancelRequested).as("the read was asked to cancel").isTrue();
        } finally {
            Thread.interrupted();
            reader.close();
        }
    }

    /**
     * The reader travels in the job graph, so what it holds is a named class rather than a lambda
     * the connector minted (ADR-0125), and a restored copy still refuses a page once it is closed.
     */
    @Test
    void theReaderCrossesTheJobGraphWithoutALambda() throws Exception {
        ClientQueryPageReader reader =
                new ClientQueryPageReader(
                        EmulatorEndpoint.parse("localhost:1", "emulatorEndpoint"));

        byte[] bytes = InstantiationUtil.serializeObject(reader);

        assertThat(new String(bytes, StandardCharsets.ISO_8859_1))
                .doesNotContain("SerializedLambda");
        ClientQueryPageReader restored =
                InstantiationUtil.deserializeObject(bytes, getClass().getClassLoader());
        restored.close();
        assertThatThrownBy(() -> restored.read(null, READ_TIME))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("closed before use");
    }

    private static CompletableFuture<Throwable> readOnAnotherThread(ClientQueryPageReader reader) {
        return CompletableFuture.supplyAsync(
                () -> {
                    try {
                        reader.read(null, READ_TIME);
                        return null;
                    } catch (Throwable t) {
                        return t;
                    }
                });
    }

    /** A read that never completes and ignores cancellation, recording that it was asked to. */
    private static final class DeafFuture implements ApiFuture<QuerySnapshot> {

        private volatile boolean cancelRequested;

        @Override
        public void addListener(Runnable listener, Executor executor) {}

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            cancelRequested = true;
            return false;
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public boolean isDone() {
            return false;
        }

        @Override
        public QuerySnapshot get() {
            throw new AssertionError("nothing waits on the read itself");
        }

        @Override
        public QuerySnapshot get(long timeout, TimeUnit unit) {
            throw new AssertionError("nothing waits on the read itself");
        }
    }
}
