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

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.SupplierWithException;

import com.google.cloud.grpc.GcpManagedChannel;
import com.google.cloud.spanner.SpannerOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Stops two executors that the Spanner client library keeps in static fields from keeping a
 * finished job's class loader reachable, by letting their threads exit once idle (docs/adr/0210,
 * #2296).
 *
 * <p>grpc-gcp's {@code GcpManagedChannel.SHARED_BACKGROUND_SERVICE} and the client's {@code
 * MultiplexedSessionDatabaseClient.MAINTAINER_SERVICE} are created once per class loader, and their
 * core threads never time out. Once a client has used them, their threads outlive every client, and
 * each thread keeps the class loader that loaded the client library reachable, through the
 * executor's thread factory on its stack and through the protection domains it recorded when it
 * started. A job that ships the connector in its job jar therefore left its class loader behind on
 * every TaskManager and JobManager it ran on (google-cloud-java#15147 and #15148).
 *
 * <p>Neither executor is shut down here, because a client opened later in the same class loader,
 * after a failover for example, schedules on the same executor. Each one is instead allowed to let
 * its core threads time out once idle for {@code KEEP_ALIVE}, and to drop a cancelled task from its
 * queue, those cancelled before this ran included; the maintainer's executor would otherwise keep a
 * closed client's cancelled maintenance task, and with it a thread, until the task's next run.
 * {@code ThreadPoolExecutor.getTask} lets the last thread exit only when the queue is empty, so an
 * open client's periodic task keeps one, and scheduling new work starts a thread again (JDK 17).
 *
 * <p>The fields are private, so they are reached by reflection, and a client-library release that
 * renames or retypes one leaves that executor as the library made it: the failure is logged once
 * and the client still opens, because the cost is retained memory, not a wrong result. {@code
 * SpannerStaticExecutorsTest} fails instead, against the client version in use.
 */
@Internal
final class SpannerStaticExecutors {

    private static final Logger LOG = LoggerFactory.getLogger(SpannerStaticExecutors.class);

    /**
     * How long an executor's thread waits with nothing to run before it exits, unless it is the
     * last thread and a task is still queued. A minute, the keep-alive of a JDK cached thread pool:
     * the executors' threads exit about a minute after the class loader's last client closes, and a
     * burst of work does not start and stop threads within that minute. A periodic task that is
     * running when its client closes can be queued once more after the cancellation (JDK 17 {@code
     * ScheduledThreadPoolExecutor.reExecutePeriodic} does not check it), and then keeps the last
     * thread until that run comes due.
     */
    static final Duration KEEP_ALIVE = Duration.ofSeconds(60);

    /**
     * The keep-alive applied at the next {@link #allowIdleThreadsToTimeOut()}; test-only otherwise.
     */
    private static volatile Duration keepAlive = KEEP_ALIVE;

    private SpannerStaticExecutors() {}

    /**
     * Sets the keep-alive {@link #allowIdleThreadsToTimeOut()} applies from its next call, so that
     * a test can observe the threads exiting without waiting a minute; {@code null} restores {@code
     * KEEP_ALIVE}.
     *
     * @param value the keep-alive, positive and at most a day, or {@code null} for the default
     */
    @VisibleForTesting
    static void keepAliveForTesting(@Nullable Duration value) {
        Preconditions.checkArgument(
                value == null
                        || (value.compareTo(Duration.ZERO) > 0
                                && value.compareTo(Duration.ofDays(1)) <= 0),
                "the keep-alive must be positive and at most a day, but was %s",
                value);
        keepAlive = value == null ? KEEP_ALIVE : value;
    }

    /**
     * Allows both executors' threads to time out once idle. Called for every client the connector
     * opens, before it opens: cheap, idempotent, and it never throws.
     *
     * <p>It also purges tasks cancelled before the removal policy was set, as those of a client
     * closed in this class loader before the connector's first one; a queued task, cancelled or
     * not, keeps the last thread.
     */
    static void allowIdleThreadsToTimeOut() {
        long nanos = keepAlive.toNanos();
        for (ScheduledThreadPoolExecutor executor : Resolved.EXECUTORS) {
            executor.setRemoveOnCancelPolicy(true);
            executor.setKeepAliveTime(nanos, TimeUnit.NANOSECONDS);
            executor.allowCoreThreadTimeOut(true);
            executor.purge();
        }
    }

    /** Reads the fields once per class loader, the first time a client opens. */
    private static final class Resolved {
        static final List<ScheduledThreadPoolExecutor> EXECUTORS = resolve();
    }

    private static List<ScheduledThreadPoolExecutor> resolve() {
        List<ScheduledThreadPoolExecutor> executors = new ArrayList<>(2);
        addIfReached(executors, reach(() -> GcpManagedChannel.class, "SHARED_BACKGROUND_SERVICE"));
        // Package-private, so named through a public class beside it; that also follows the
        // relocation a shaded SQL connector jar applies to the package.
        addIfReached(
                executors,
                reach(
                        () ->
                                Class.forName(
                                        SpannerOptions.class.getPackageName()
                                                + ".MultiplexedSessionDatabaseClient",
                                        false,
                                        SpannerOptions.class.getClassLoader()),
                        "MAINTAINER_SERVICE"));
        return Collections.unmodifiableList(executors);
    }

    private static void addIfReached(
            List<ScheduledThreadPoolExecutor> executors,
            @Nullable ScheduledThreadPoolExecutor executor) {
        if (executor != null) {
            executors.add(executor);
        }
    }

    /**
     * Reads a static executor field, logging a warning and returning {@code null} if the field
     * cannot be read or holds no {@code ScheduledThreadPoolExecutor}.
     *
     * @param owner finds the class that declares the field
     * @param fieldName the field's name
     * @return the executor, or {@code null}
     */
    @VisibleForTesting
    @Nullable
    static ScheduledThreadPoolExecutor reach(
            SupplierWithException<Class<?>, ClassNotFoundException> owner, String fieldName) {
        String name = fieldName;
        try {
            Class<?> type = owner.get();
            name = type.getName() + "." + fieldName;
            Field field = type.getDeclaredField(fieldName);
            field.setAccessible(true);
            // An instance field fails here with a NullPointerException, which the catch reports.
            Object value = field.get(null);
            if (!(value instanceof ScheduledThreadPoolExecutor)) {
                throw new ClassCastException(
                        name
                                + " holds "
                                + (value == null ? "null" : value.getClass().getName())
                                + ", not a ScheduledThreadPoolExecutor");
            }
            return (ScheduledThreadPoolExecutor) value;
        } catch (ReflectiveOperationException | RuntimeException | LinkageError e) {
            LOG.warn(
                    "Could not reach the Spanner client library's executor {}, so its idle"
                            + " threads are left as the client library configures them; if it still"
                            + " keeps that executor, they keep a finished job's class loader"
                            + " reachable (docs/adr/0210).",
                    name,
                    e);
            return null;
        }
    }
}
