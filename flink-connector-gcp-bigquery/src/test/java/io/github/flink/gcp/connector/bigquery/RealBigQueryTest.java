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

package io.github.flink.gcp.connector.bigquery;

import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.BigQueryException;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

class RealBigQueryTest {
    @Test
    void everyTableDeletionIsAttemptedAndFailuresRemainVisible() {
        List<String> attempted = new ArrayList<>();
        RuntimeException first = new RuntimeException("first");
        RuntimeException second = new RuntimeException("second");
        assertThatThrownBy(
                        () ->
                                RealBigQuery.deleteTables(
                                        table -> {
                                            attempted.add(table);
                                            if (table.equals("a")) {
                                                throw first;
                                            }
                                            if (table.equals("b")) {
                                                throw second;
                                            }
                                        },
                                        millis -> {
                                            throw new AssertionError("Unexpected retry");
                                        },
                                        "a",
                                        "b",
                                        "c"))
                .isSameAs(first)
                .hasSuppressedException(second);
        assertThat(attempted).containsExactly("a", "b", "c");
    }

    @Test
    void shortTermRateLimitCanRecoverOnTheLastAttempt() {
        AtomicInteger attempts = new AtomicInteger();
        List<Long> waits = new ArrayList<>();
        BigQueryException rateLimit = error(403, "rateLimitExceeded");
        assertThat(rateLimit.isRetryable())
                .as("the pinned SDK leaves this to the caller")
                .isFalse();

        RealBigQuery.deleteTables(
                table -> {
                    if (attempts.incrementAndGet() < 6) {
                        throw rateLimit;
                    }
                },
                waits::add,
                "a");

        assertThat(attempts).hasValue(6);
        assertThat(waits).containsExactly(1000L, 2000L, 4000L, 8000L, 16000L);
    }

    @Test
    void exhaustedRateLimitRemainsVisibleAndDoesNotSkipOtherTables() {
        List<String> attempted = new ArrayList<>();
        List<Long> waits = new ArrayList<>();
        BigQueryException rateLimit = error(403, "rateLimitExceeded");
        BigQueryException denied = error(403, "accessDenied");

        assertThatThrownBy(
                        () ->
                                RealBigQuery.deleteTables(
                                        table -> {
                                            attempted.add(table);
                                            if (table.equals("a")) {
                                                throw rateLimit;
                                            }
                                            if (table.equals("b")) {
                                                throw denied;
                                            }
                                        },
                                        waits::add,
                                        "a",
                                        "b",
                                        "c"))
                .isSameAs(rateLimit)
                .hasSuppressedException(denied);
        assertThat(attempted).containsExactly("a", "a", "a", "a", "a", "a", "b", "c");
        assertThat(waits).hasSize(5);
    }

    @Test
    void otherReasonsAndCodesDoNotAcquireAnOuterRetryLoop() {
        for (BigQueryException error :
                new BigQueryException[] {
                    error(403, "accessDenied"),
                    error(403, "quotaExceeded"),
                    new BigQueryException(403, "rateLimitExceeded"),
                    error(400, "rateLimitExceeded"),
                    error(503, "backendError")
                }) {
            AtomicInteger attempts = new AtomicInteger();
            assertThatThrownBy(
                            () ->
                                    RealBigQuery.deleteTables(
                                            table -> {
                                                attempts.incrementAndGet();
                                                throw error;
                                            },
                                            millis -> {
                                                throw new AssertionError("Unexpected retry");
                                            },
                                            "a"))
                    .isSameAs(error);
            assertThat(attempts).hasValue(1);
        }
    }

    @Test
    void interruptionStopsRetriesAndRemainingTablesWithoutLosingFailures() {
        List<String> attempted = new ArrayList<>();
        BigQueryException denied = error(403, "accessDenied");
        BigQueryException rateLimit = error(403, "rateLimitExceeded");
        InterruptedException interrupted = new InterruptedException("stop cleanup");
        try {
            assertThatThrownBy(
                            () ->
                                    RealBigQuery.deleteTables(
                                            table -> {
                                                attempted.add(table);
                                                throw table.equals("a") ? denied : rateLimit;
                                            },
                                            millis -> {
                                                throw interrupted;
                                            },
                                            "a",
                                            "b",
                                            "c"))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageEndingWith("table b")
                    .hasCause(interrupted)
                    .hasSuppressedException(denied);
            assertThat(interrupted).hasSuppressedException(rateLimit);
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
            assertThat(attempted).containsExactly("a", "b");
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    void interruptionAfterBackoffRetainsTheRateLimitAndStartsNoMoreDeletes() {
        AtomicInteger attempts = new AtomicInteger();
        BigQueryException rateLimit = error(403, "rateLimitExceeded");
        try {
            Throwable failure =
                    catchThrowable(
                            () ->
                                    RealBigQuery.deleteTables(
                                            table -> {
                                                attempts.incrementAndGet();
                                                throw rateLimit;
                                            },
                                            millis -> Thread.currentThread().interrupt(),
                                            "a",
                                            "b"));
            assertThat(failure)
                    .isInstanceOf(IllegalStateException.class)
                    .hasCauseInstanceOf(InterruptedException.class);
            assertThat(failure.getCause()).hasSuppressedException(rateLimit);
            assertThat(attempts).hasValue(1);
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    void anAlreadyInterruptedCleanupStartsNoDelete() {
        Thread.currentThread().interrupt();
        try {
            assertThatThrownBy(
                            () ->
                                    RealBigQuery.deleteTables(
                                            table -> {
                                                throw new AssertionError("Unexpected deletion");
                                            },
                                            millis -> {
                                                throw new AssertionError("Unexpected retry");
                                            },
                                            "a"))
                    .isInstanceOf(IllegalStateException.class)
                    .hasCauseInstanceOf(InterruptedException.class);
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
        } finally {
            Thread.interrupted();
        }
    }

    private static BigQueryException error(int code, String reason) {
        return new BigQueryException(code, reason, new BigQueryError(reason, null, reason));
    }
}
