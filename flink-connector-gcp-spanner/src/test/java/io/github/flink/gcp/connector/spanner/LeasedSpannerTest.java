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

package io.github.flink.gcp.connector.spanner;

import com.google.cloud.spanner.Spanner;
import io.github.flink.gcp.connector.base.rpc.GrpcEventLoops;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link LeasedSpanner}. */
class LeasedSpannerTest {

    @AfterEach
    void noLeaseIsLeftOpen() {
        assertThat(GrpcEventLoops.isRunning()).as("a lease was left open").isFalse();
    }

    @Test
    void theHandleClosesBeforeTheLeaseIsReleased() {
        List<Boolean> leaseOpenAtClose = new ArrayList<>();
        LeasedSpanner leased =
                new LeasedSpanner(
                        handle(() -> leaseOpenAtClose.add(GrpcEventLoops.isRunning())),
                        GrpcEventLoops.acquire());

        leased.close();

        assertThat(leaseOpenAtClose).containsExactly(true);
        assertThat(GrpcEventLoops.isRunning()).isFalse();
    }

    @Test
    void theLeaseIsReleasedWhenTheHandlesCloseFails() {
        LeasedSpanner leased =
                new LeasedSpanner(
                        handle(
                                () -> {
                                    throw new IllegalStateException("close failed");
                                }),
                        GrpcEventLoops.acquire());

        assertThatThrownBy(leased::close).hasMessage("close failed");
        assertThat(GrpcEventLoops.isRunning()).isFalse();
    }

    @Test
    void anInterruptTheHandlesCloseSwallowsIsRestored() {
        // The client consumes the interrupt its close's wait throws on and does not restore it.
        // Restored before the release, it lets a release inside
        // GrpcEventLoops.interruptibleReleases() end its wait; this test checks the restoration
        // only, since the group's wait cannot be held up from outside base.
        List<Boolean> interruptedAtClose = new ArrayList<>();
        LeasedSpanner leased =
                new LeasedSpanner(
                        handle(
                                () -> {
                                    interruptedAtClose.add(Thread.interrupted());
                                    throw new IllegalStateException("interrupted close");
                                }),
                        GrpcEventLoops.acquire());

        Thread.currentThread().interrupt();
        try {
            assertThatThrownBy(leased::close).hasMessage("interrupted close");
            assertThat(Thread.currentThread().isInterrupted())
                    .as("the interrupt is restored after the handle's close")
                    .isTrue();
        } finally {
            Thread.interrupted();
        }
        assertThat(interruptedAtClose).containsExactly(true);
        assertThat(GrpcEventLoops.isRunning()).isFalse();
    }

    @Test
    void anInterruptThatArrivesDuringTheHandlesCloseIsRestored() {
        // The client reports an interrupt that ends its close's wait only as the cause of its own
        // exception, with the thread's flag cleared.
        LeasedSpanner leased =
                new LeasedSpanner(
                        handle(
                                () -> {
                                    throw new IllegalStateException(
                                            "interrupted close", new InterruptedException());
                                }),
                        GrpcEventLoops.acquire());

        try {
            assertThatThrownBy(leased::close).hasMessage("interrupted close");
            assertThat(Thread.currentThread().isInterrupted())
                    .as("the interrupt the close reported is restored")
                    .isTrue();
        } finally {
            Thread.interrupted();
        }
        assertThat(GrpcEventLoops.isRunning()).isFalse();
    }

    @Test
    void theHandleIsClosedOnceAndALaterCloseWaitsForTheFirst() throws Exception {
        // The client's own close refuses a second call, so only the first close reaches it.
        CountDownLatch closing = new CountDownLatch(1);
        CountDownLatch finish = new CountDownLatch(1);
        List<String> closes = new ArrayList<>();
        LeasedSpanner leased =
                new LeasedSpanner(
                        handle(
                                () -> {
                                    closes.add("close");
                                    closing.countDown();
                                    try {
                                        finish.await();
                                    } catch (InterruptedException e) {
                                        Thread.currentThread().interrupt();
                                    }
                                }),
                        GrpcEventLoops.acquire());
        Thread first = new Thread(leased::close, "first-close");
        Thread second = new Thread(leased::close, "second-close");
        try {
            first.start();
            assertThat(closing.await(10, TimeUnit.SECONDS)).isTrue();

            second.start();
            second.join(300);
            assertThat(second.isAlive()).as("the second close waits for the first").isTrue();
        } finally {
            // Whatever failed above, the first close must finish, or its lease outlives the test.
            finish.countDown();
            first.join(TimeUnit.SECONDS.toMillis(15));
            second.join(TimeUnit.SECONDS.toMillis(15));
        }
        assertThat(second.isAlive()).isFalse();
        assertThat(closes).containsExactly("close");
    }

    /** A handle whose close runs {@code onClose} and which supports nothing else. */
    private static Spanner handle(Runnable onClose) {
        return (Spanner)
                Proxy.newProxyInstance(
                        Spanner.class.getClassLoader(),
                        new Class<?>[] {Spanner.class},
                        (self, method, args) -> {
                            switch (method.getName()) {
                                case "close":
                                    onClose.run();
                                    return null;
                                case "toString":
                                    return "handle";
                                case "hashCode":
                                    return System.identityHashCode(self);
                                case "equals":
                                    return self == args[0];
                                default:
                                    throw new UnsupportedOperationException(method.getName());
                            }
                        });
    }
}
