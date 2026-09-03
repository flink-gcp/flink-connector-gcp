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

package io.github.flink.gcp.connector.base.metrics;

import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.metrics.ThreadSafeSimpleCounter;
import org.apache.flink.metrics.testutils.MetricListener;

import com.google.api.gax.rpc.StatusCode;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link ErrorClassCounters}, asserting through the names the counters register under.
 */
class ErrorClassCountersTest {

    private final MetricListener listener = new MetricListener();
    private final ErrorClassCounters counters = new ErrorClassCounters(listener.getMetricGroup());

    @Test
    void countsEachStatusCodeUnderItsOwnErrorClass() {
        counters.count(StatusCode.Code.UNAVAILABLE);
        counters.count(StatusCode.Code.UNAVAILABLE);
        counters.count(StatusCode.Code.INVALID_ARGUMENT);

        assertThat(errors("UNAVAILABLE")).isEqualTo(2);
        assertThat(errors("INVALID_ARGUMENT")).isEqualTo(1);
    }

    @Test
    void countsAnUnclassifiedFailureUnderTheUnclassifiedName() {
        counters.count(null);
        counters.count(null);

        assertThat(errors(ErrorClassCounters.UNCLASSIFIED)).isEqualTo(2);
    }

    @Test
    void registersNothingForACodeThatNeverOccurred() {
        counters.count(StatusCode.Code.NOT_FOUND);

        assertThat(listener.getCounter("errorClass", "PERMISSION_DENIED", "errors")).isEmpty();
        assertThat(listener.getCounter("errorClass", ErrorClassCounters.UNCLASSIFIED, "errors"))
                .isEmpty();
    }

    @Test
    void reusesOneCounterPerCodeRatherThanRegisteringItAgain() {
        // The lazy creation must be memoized: re-registering the same name on a real metric group
        // logs a warning and keeps the first metric, so a fresh counter per call would silently
        // stop counting at one.
        counters.count(StatusCode.Code.RESOURCE_EXHAUSTED);
        counters.count(StatusCode.Code.RESOURCE_EXHAUSTED);
        counters.count(StatusCode.Code.RESOURCE_EXHAUSTED);

        assertThat(errors("RESOURCE_EXHAUSTED")).isEqualTo(3);
    }

    @Test
    void everyGaxStatusCodeIsANameTheGroupAccepts() {
        // The bound the class rests on: the error-class dimension is the enum plus one, so an
        // unconditional subgroup per code cannot grow without limit.
        for (StatusCode.Code code : StatusCode.Code.values()) {
            counters.count(code);
        }

        for (StatusCode.Code code : StatusCode.Code.values()) {
            assertThat(errors(code.name())).isEqualTo(1);
        }
    }

    @Test
    void registersTheSuppliedCounterTypeUnderTheSameNames() {
        // The type is the whole point of the overload: a connector counting from SDK callback
        // threads must find the thread-safe counter it asked for under the name a scrape reads.
        ErrorClassCounters threadSafe =
                new ErrorClassCounters(listener.getMetricGroup(), ThreadSafeSimpleCounter::new);

        threadSafe.count(StatusCode.Code.DEADLINE_EXCEEDED);
        threadSafe.count(null);

        assertThat(listener.getCounter("errorClass", "DEADLINE_EXCEEDED", "errors"))
                .get()
                .isInstanceOf(ThreadSafeSimpleCounter.class);
        assertThat(errors("DEADLINE_EXCEEDED")).isEqualTo(1);
        assertThat(errors(ErrorClassCounters.UNCLASSIFIED)).isEqualTo(1);
    }

    @Test
    void theDefaultCounterIsThePlainOne() {
        counters.count(StatusCode.Code.ABORTED);

        assertThat(listener.getCounter("errorClass", "ABORTED", "errors"))
                .get()
                .isExactlyInstanceOf(SimpleCounter.class);
    }

    @Test
    void countsExactlyUnderConcurrentFirstUseFromManyThreads() throws Exception {
        // The lazy registration races when every thread meets a code for the first time at once:
        // a plain map here would register two counters under one name and lose one of them.
        ErrorClassCounters threadSafe =
                new ErrorClassCounters(listener.getMetricGroup(), ThreadSafeSimpleCounter::new);
        int threads = 8;
        int perThread = 500;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<?>> futures = new ArrayList<>();
        try {
            for (int t = 0; t < threads; t++) {
                futures.add(
                        pool.submit(
                                () -> {
                                    start.await();
                                    for (int i = 0; i < perThread; i++) {
                                        threadSafe.count(StatusCode.Code.UNAVAILABLE);
                                        threadSafe.count(null);
                                    }
                                    return null;
                                }));
            }
            start.countDown();
            for (Future<?> future : futures) {
                future.get(30, TimeUnit.SECONDS);
            }
        } finally {
            pool.shutdownNow();
        }

        assertThat(errors("UNAVAILABLE")).isEqualTo((long) threads * perThread);
        assertThat(errors(ErrorClassCounters.UNCLASSIFIED)).isEqualTo((long) threads * perThread);
    }

    private long errors(String errorClass) {
        return listener.getCounter("errorClass", errorClass, "errors")
                .orElseThrow(
                        () ->
                                new AssertionError(
                                        "No counter registered for errorClass " + errorClass + "."))
                .getCount();
    }
}
