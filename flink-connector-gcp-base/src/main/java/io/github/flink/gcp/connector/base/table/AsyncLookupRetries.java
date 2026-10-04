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

package io.github.flink.gcp.connector.base.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.RowData;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * Retries an asynchronous Table API lookup and converts its successful result to rows. Read and
 * conversion behavior and transient-failure classification belong to the connector (ADR-0039).
 *
 * <p>Callbacks run directly, without a helper-owned executor. A work counter turns an immediate
 * failure's retry into a loop, so the retry budget cannot grow the calling thread's stack. Each
 * invocation has its own counter, retry budget and result.
 *
 * <p>Cancellation of the returned future stops further retries but does not cancel an in-flight
 * read. Flink's {@code AsyncLookupFunction.eval} and {@code CachingAsyncLookupFunction} chain
 * completion without propagating cancellation to the lookup's future (Flink 1.20.4 and 2.2.1). The
 * client futures also differ: Bigtable's classic point-read future supports upstream cancellation,
 * while Spanner's single-row and Firestore's document futures do not wire cancellation to their
 * streams (libraries-bom 26.90.0). The helper therefore carries no active-call cancellation handle.
 */
@Internal
public final class AsyncLookupRetries {

    private AsyncLookupRetries() {}

    /**
     * Starts one read and retries classified failures up to {@code maxRetries} times after the
     * initial attempt. A read that throws at once is handled like a failed future. Conversion
     * failures complete the result exceptionally without another read.
     *
     * @param read starts a new read of the same key on each call
     * @param converter converts the successful value, including a nullable missing-row value
     * @param isRetryable classifies read failures using the connector's policy
     * @param maxRetries the connector-validated non-negative number of retries
     * @param <T> the client library's read-result type
     * @return the rows or the last read or conversion failure
     */
    public static <T> CompletableFuture<Collection<RowData>> lookup(
            Supplier<ApiFuture<T>> read,
            RowConverter<T> converter,
            Predicate<Throwable> isRetryable,
            int maxRetries) {
        LookupAttempt<T> attempt = new LookupAttempt<>(read, converter, isRetryable, maxRetries);
        attempt.schedule();
        return attempt.result;
    }

    /** Converts a client value to rows, allowing a deserializer's checked exception. */
    @Internal
    @FunctionalInterface
    public interface RowConverter<T> {

        /** Converts the value; a connector may represent a missing row with {@code null}. */
        Collection<RowData> convert(@Nullable T value) throws Exception;
    }

    private static final class LookupAttempt<T> {

        private final Supplier<ApiFuture<T>> read;
        private final RowConverter<T> converter;
        private final Predicate<Throwable> isRetryable;
        private final int maxRetries;
        private final CompletableFuture<Collection<RowData>> result = new CompletableFuture<>();
        private final AtomicInteger work = new AtomicInteger();
        private int retry;

        private LookupAttempt(
                Supplier<ApiFuture<T>> read,
                RowConverter<T> converter,
                Predicate<Throwable> isRetryable,
                int maxRetries) {
            this.read = read;
            this.converter = converter;
            this.isRetryable = isRetryable;
            this.maxRetries = maxRetries;
        }

        private void schedule() {
            if (work.getAndIncrement() != 0) {
                return;
            }
            int pending = 1;
            do {
                if (!result.isDone()) {
                    issue();
                }
                pending = work.addAndGet(-pending);
            } while (pending != 0);
        }

        private void issue() {
            final ApiFuture<T> future;
            try {
                future = read.get();
            } catch (RuntimeException failure) {
                handleFailure(failure);
                return;
            }
            ApiFutures.addCallback(
                    future,
                    new ApiFutureCallback<T>() {
                        @Override
                        public void onFailure(Throwable failure) {
                            handleFailure(failure);
                        }

                        @Override
                        public void onSuccess(@Nullable T value) {
                            if (result.isDone()) {
                                return;
                            }
                            try {
                                result.complete(converter.convert(value));
                            } catch (Exception failure) {
                                result.completeExceptionally(failure);
                            }
                        }
                    },
                    Runnable::run);
        }

        private void handleFailure(Throwable failure) {
            if (result.isDone()) {
                return;
            }
            if (retry < maxRetries && isRetryable.test(failure)) {
                retry++;
                schedule();
            } else {
                result.completeExceptionally(failure);
            }
        }
    }
}
