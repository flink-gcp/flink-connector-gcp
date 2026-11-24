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

import com.google.cloud.grpc.GcpManagedChannel;
import io.github.flink.gcp.connector.testutils.LogCapture;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The two client-library executors are reached and adjusted against the client version in use
 * (#2296).
 *
 * <p>{@link SpannerStaticExecutors} only logs a field it cannot read, so that a client release that
 * renames one still opens its clients; this test is what fails instead, on the libraries-bom bump
 * that brings such a release. It reads both fields itself rather than trusting what the class
 * reached.
 */
class SpannerStaticExecutorsTest {

    @AfterEach
    void restoreTheKeepAlive() {
        SpannerStaticExecutors.keepAliveForTesting(null);
        SpannerStaticExecutors.allowIdleThreadsToTimeOut();
    }

    @Test
    void theKeepAliveIsTheMinuteTheRecordsState() {
        // ADR-0210 and the connectors page state a minute; the other tests derive from the
        // constant.
        assertThat(SpannerStaticExecutors.KEEP_ALIVE).isEqualTo(Duration.ofMinutes(1));
    }

    @Test
    void bothExecutorsAllowTheirIdleThreadsToTimeOut() throws Exception {
        ScheduledThreadPoolExecutor grpcGcp = grpcGcpExecutor();
        ScheduledThreadPoolExecutor maintainer = maintainerExecutor();
        int grpcGcpCore = grpcGcp.getCorePoolSize();

        SpannerStaticExecutors.allowIdleThreadsToTimeOut();

        for (ScheduledThreadPoolExecutor executor :
                new ScheduledThreadPoolExecutor[] {grpcGcp, maintainer}) {
            // The JDK's defaults are false, 10 ms and false (grpc-gcp sets the removal policy
            // itself).
            assertThat(executor.allowsCoreThreadTimeOut()).isTrue();
            assertThat(executor.getKeepAliveTime(TimeUnit.NANOSECONDS))
                    .isEqualTo(SpannerStaticExecutors.KEEP_ALIVE.toNanos());
            assertThat(executor.getRemoveOnCancelPolicy()).isTrue();
            // A client opened later in the same class loader schedules on the same executor.
            assertThat(executor.isShutdown()).isFalse();
        }
        // The pools keep their sizes; only their idle threads may now exit.
        assertThat(grpcGcp.getCorePoolSize()).isEqualTo(grpcGcpCore);
        assertThat(maintainer.getCorePoolSize()).isOne();
    }

    @Test
    void aTaskCancelledBeforeThePolicyLeavesTheQueue() throws Exception {
        // A client closed in this class loader before the connector's first open cancels its
        // maintenance task while the client library's removal policy is still off; the policy
        // alone does not remove what was cancelled before it, and the task would keep the last
        // thread until its next run, ten minutes later by default.
        ScheduledThreadPoolExecutor maintainer = maintainerExecutor();
        maintainer.setRemoveOnCancelPolicy(false);
        ScheduledFuture<?> task = maintainer.schedule(() -> {}, 10, TimeUnit.MINUTES);
        task.cancel(false);
        assertThat(maintainer.getQueue().contains(task))
                .as("cancelled with the policy off")
                .isTrue();

        SpannerStaticExecutors.allowIdleThreadsToTimeOut();

        assertThat(maintainer.getQueue().contains(task)).as("queued after the purge").isFalse();
    }

    @Test
    void theTestKeepAliveReachesTheExecutorsAtTheNextCall() throws Exception {
        SpannerStaticExecutors.keepAliveForTesting(Duration.ofMillis(250));

        SpannerStaticExecutors.allowIdleThreadsToTimeOut();

        assertThat(maintainerExecutor().getKeepAliveTime(TimeUnit.MILLISECONDS)).isEqualTo(250);
        assertThat(grpcGcpExecutor().getKeepAliveTime(TimeUnit.MILLISECONDS)).isEqualTo(250);
    }

    @Test
    void theTestKeepAliveRefusesWhatTheExecutorsWouldThrowOn() {
        // A zero or negative keep-alive makes the executors throw, and every client open with them.
        assertThatThrownBy(() -> SpannerStaticExecutors.keepAliveForTesting(Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> SpannerStaticExecutors.keepAliveForTesting(Duration.ofMillis(-1)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void aFieldThatCannotBeReadIsLoggedAndSkipped() {
        try (LogCapture capture = LogCapture.of(SpannerStaticExecutors.class)) {
            assertThat(SpannerStaticExecutors.reach(() -> Holder.class, "MISSING")).isNull();
            assertThat(SpannerStaticExecutors.reach(() -> Holder.class, "NOT_AN_EXECUTOR"))
                    .isNull();
            assertThat(SpannerStaticExecutors.reach(() -> Holder.class, "instanceExecutor"))
                    .isNull();
            assertThat(
                            SpannerStaticExecutors.reach(
                                    () -> {
                                        throw new ClassNotFoundException("relocated away");
                                    },
                                    "MAINTAINER_SERVICE"))
                    .isNull();

            assertThat(capture.getMessages())
                    .hasSize(4)
                    .allMatch(
                            m ->
                                    m.startsWith(
                                            "Could not reach the Spanner client library's executor"));
            assertThat(capture.getMessages().get(0)).contains(Holder.class.getName() + ".MISSING");
        }
        assertThat(SpannerStaticExecutors.reach(() -> Holder.class, "EXECUTOR"))
                .isSameAs(Holder.EXECUTOR);
    }

    /** Fields of each shape {@code SpannerStaticExecutors.reach} must tell apart. */
    @SuppressWarnings("unused")
    private static final class Holder {
        private static final ScheduledThreadPoolExecutor EXECUTOR =
                new ScheduledThreadPoolExecutor(1);
        private static final Object NOT_AN_EXECUTOR = new Object();
        private final ScheduledThreadPoolExecutor instanceExecutor = EXECUTOR;
    }

    private static ScheduledThreadPoolExecutor grpcGcpExecutor() throws Exception {
        return read(GcpManagedChannel.class, "SHARED_BACKGROUND_SERVICE");
    }

    private static ScheduledThreadPoolExecutor maintainerExecutor() throws Exception {
        return read(
                Class.forName("com.google.cloud.spanner.MultiplexedSessionDatabaseClient"),
                "MAINTAINER_SERVICE");
    }

    private static ScheduledThreadPoolExecutor read(Class<?> owner, String name) throws Exception {
        Field field = owner.getDeclaredField(name);
        field.setAccessible(true);
        return (ScheduledThreadPoolExecutor) field.get(null);
    }
}
