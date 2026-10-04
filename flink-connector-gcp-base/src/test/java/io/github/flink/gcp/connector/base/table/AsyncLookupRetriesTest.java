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

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.AsyncLookupFunction;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.api.core.SettableApiFuture;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AsyncLookupRetriesTest {

    private static final Collection<RowData> ROWS =
            Collections.singletonList(GenericRowData.of(7L));

    @Test
    void convertsAnImmediateSuccessOnce() throws Exception {
        AtomicInteger reads = new AtomicInteger();
        AtomicInteger conversions = new AtomicInteger();
        CompletableFuture<Collection<RowData>> result =
                AsyncLookupRetries.lookup(
                        () -> {
                            reads.incrementAndGet();
                            return ApiFutures.immediateFuture(7L);
                        },
                        value -> {
                            conversions.incrementAndGet();
                            assertThat(value).isEqualTo(7L);
                            return ROWS;
                        },
                        failure -> true,
                        3);

        assertThat(result.get(10, SECONDS)).isSameAs(ROWS);
        assertThat(reads).hasValue(1);
        assertThat(conversions).hasValue(1);
    }

    @Test
    void passesANullableMissingRowToTheConverter() throws Exception {
        CompletableFuture<Collection<RowData>> result =
                AsyncLookupRetries.lookup(
                        () -> ApiFutures.immediateFuture(null),
                        value -> {
                            assertThat(value).isNull();
                            return Collections.emptyList();
                        },
                        failure -> true,
                        3);

        assertThat(result.get(10, SECONDS)).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void retriesAnImmediateReadFailureWithinTheBudget(boolean throwsAtOnce) throws Exception {
        RuntimeException transientFailure = new IllegalStateException("transient");
        AtomicInteger reads = new AtomicInteger();
        CompletableFuture<Collection<RowData>> result =
                AsyncLookupRetries.lookup(
                        () -> {
                            if (reads.getAndIncrement() < 2) {
                                if (throwsAtOnce) {
                                    throw transientFailure;
                                }
                                return ApiFutures.immediateFailedFuture(transientFailure);
                            }
                            return ApiFutures.immediateFuture(7L);
                        },
                        value -> ROWS,
                        failure -> failure == transientFailure,
                        2);

        assertThat(result.get(10, SECONDS)).isSameAs(ROWS);
        assertThat(reads).hasValue(3);
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 2})
    void exhaustionReturnsTheLastReadFailure(int maxRetries) {
        AtomicInteger reads = new AtomicInteger();
        RuntimeException lastFailure = new IllegalStateException("last");
        CompletableFuture<Collection<RowData>> result =
                AsyncLookupRetries.lookup(
                        () ->
                                ApiFutures.immediateFailedFuture(
                                        reads.getAndIncrement() < maxRetries
                                                ? new IllegalStateException("earlier")
                                                : lastFailure),
                        value -> ROWS,
                        failure -> true,
                        maxRetries);

        assertThatThrownBy(result::join).hasCauseReference(lastFailure);
        assertThat(reads).hasValue(maxRetries + 1);
    }

    @Test
    void doesNotRetryAnUnclassifiedFailure() {
        AtomicInteger reads = new AtomicInteger();
        RuntimeException permanentFailure = new IllegalArgumentException("permanent");
        CompletableFuture<Collection<RowData>> result =
                AsyncLookupRetries.lookup(
                        () -> {
                            reads.incrementAndGet();
                            return ApiFutures.immediateFailedFuture(permanentFailure);
                        },
                        value -> ROWS,
                        failure -> false,
                        3);

        assertThatThrownBy(result::join).hasCauseReference(permanentFailure);
        assertThat(reads).hasValue(1);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void conversionFailuresAreNotRetried(boolean checked) {
        Exception conversionFailure =
                checked ? new IOException("conversion") : new IllegalStateException("conversion");
        AtomicInteger reads = new AtomicInteger();
        AtomicInteger classifications = new AtomicInteger();
        CompletableFuture<Collection<RowData>> result =
                AsyncLookupRetries.lookup(
                        () -> {
                            reads.incrementAndGet();
                            return ApiFutures.immediateFuture(7L);
                        },
                        value -> {
                            throw conversionFailure;
                        },
                        failure -> {
                            classifications.incrementAndGet();
                            return true;
                        },
                        3);

        assertThatThrownBy(result::join).hasCauseReference(conversionFailure);
        assertThat(reads).hasValue(1);
        assertThat(classifications).hasValue(0);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void immediateFailuresDoNotGrowTheCallingThreadStack(boolean throwsAtOnce) throws Exception {
        int budget = 5_000;
        AtomicInteger reads = new AtomicInteger();
        RuntimeException transientFailure = new IllegalStateException("transient");
        Supplier<ApiFuture<Long>> read =
                () -> {
                    if (reads.getAndIncrement() < budget) {
                        if (throwsAtOnce) {
                            throw transientFailure;
                        }
                        return ApiFutures.immediateFailedFuture(transientFailure);
                    }
                    return ApiFutures.immediateFuture(7L);
                };
        CompletableFuture<Collection<RowData>> observed = new CompletableFuture<>();
        // A recursive retry overflows this stack even on a JVM with a large default stack.
        Thread caller =
                new Thread(
                        null,
                        () -> {
                            try {
                                AsyncLookupRetries.lookup(
                                                read, value -> ROWS, failure -> true, budget)
                                        .whenComplete(
                                                (rows, failure) -> {
                                                    if (failure == null) {
                                                        observed.complete(rows);
                                                    } else {
                                                        observed.completeExceptionally(failure);
                                                    }
                                                });
                            } catch (Throwable failure) {
                                observed.completeExceptionally(failure);
                            }
                        },
                        "small-stack-lookup",
                        256 * 1024);
        caller.setDaemon(true);
        caller.start();

        assertThat(observed.get(10, SECONDS)).isSameAs(ROWS);
        caller.join(10_000);
        assertThat(caller.isAlive()).isFalse();
        assertThat(reads).hasValue(budget + 1);
    }

    @Test
    void retriesALaterFailureAndConvertsALaterSuccessOnAnotherThread() throws Exception {
        SettableApiFuture<Long> first = SettableApiFuture.create();
        SettableApiFuture<Long> second = SettableApiFuture.create();
        AtomicInteger reads = new AtomicInteger();
        CompletableFuture<Collection<RowData>> result =
                AsyncLookupRetries.lookup(
                        () -> reads.getAndIncrement() == 0 ? first : second,
                        value -> ROWS,
                        failure -> true,
                        1);
        assertThat(result).isNotDone();

        CompletableFuture.runAsync(() -> first.setException(new IllegalStateException("transient")))
                .get(10, SECONDS);
        assertThat(result).isNotDone();
        assertThat(reads).hasValue(2);

        CompletableFuture.runAsync(() -> second.set(7L)).get(10, SECONDS);
        assertThat(result.get(10, SECONDS)).isSameAs(ROWS);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void cancelledResultsIgnoreLateCallbacksWithoutCancellingTheRead(boolean failure)
            throws Exception {
        SettableApiFuture<Long> pending = SettableApiFuture.create();
        AtomicInteger reads = new AtomicInteger();
        AtomicInteger conversions = new AtomicInteger();
        CompletableFuture<Collection<RowData>> result =
                AsyncLookupRetries.lookup(
                        () -> {
                            reads.incrementAndGet();
                            return pending;
                        },
                        value -> {
                            conversions.incrementAndGet();
                            return ROWS;
                        },
                        ignored -> true,
                        3);

        assertThat(result.cancel(true)).isTrue();
        assertThat(pending.isCancelled()).isFalse();
        CompletableFuture.runAsync(
                        () -> {
                            if (failure) {
                                pending.setException(new IllegalStateException("late"));
                            } else {
                                pending.set(7L);
                            }
                        })
                .get(10, SECONDS);

        assertThat(result).isCancelled();
        assertThat(reads).hasValue(1);
        assertThat(conversions).hasValue(0);
    }

    @Test
    void flinkEvalCancellationDoesNotReachTheLookupResult() throws Exception {
        SettableApiFuture<Long> pending = SettableApiFuture.create();
        CompletableFuture<Collection<RowData>> lookup =
                AsyncLookupRetries.lookup(() -> pending, value -> ROWS, failure -> true, 1);
        AsyncLookupFunction function =
                new AsyncLookupFunction() {
                    @Override
                    public CompletableFuture<Collection<RowData>> asyncLookup(RowData key) {
                        return lookup;
                    }
                };
        CompletableFuture<Collection<RowData>> flinkResult = new CompletableFuture<>();
        function.eval(flinkResult, 7L);

        assertThat(flinkResult.cancel(true)).isTrue();
        assertThat(lookup).isNotDone();
        assertThat(pending.isCancelled()).isFalse();
        pending.set(7L);
        assertThat(lookup.get(10, SECONDS)).isSameAs(ROWS);
        assertThat(flinkResult).isCancelled();
    }

    @Test
    void overlappingLookupsHaveIndependentResultsAndRetryBudgets() throws Exception {
        SettableApiFuture<Long> firstRead = SettableApiFuture.create();
        SettableApiFuture<Long> secondRead = SettableApiFuture.create();
        AtomicInteger reads = new AtomicInteger();
        Supplier<ApiFuture<Long>> read =
                () -> {
                    int attempt = reads.getAndIncrement();
                    if (attempt == 0) {
                        return firstRead;
                    }
                    if (attempt == 1) {
                        return secondRead;
                    }
                    return ApiFutures.immediateFuture(7L);
                };
        CompletableFuture<Collection<RowData>> first =
                AsyncLookupRetries.lookup(read, value -> ROWS, failure -> true, 1);
        CompletableFuture<Collection<RowData>> second =
                AsyncLookupRetries.lookup(read, value -> ROWS, failure -> true, 1);

        firstRead.setException(new IllegalStateException("first"));
        secondRead.setException(new IllegalStateException("second"));

        assertThat(first.get(10, SECONDS)).isSameAs(ROWS);
        assertThat(second.get(10, SECONDS)).isSameAs(ROWS);
        assertThat(reads).hasValue(4);
    }
}
