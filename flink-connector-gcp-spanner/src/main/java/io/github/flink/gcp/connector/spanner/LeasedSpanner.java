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
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;

import com.google.cloud.spanner.Spanner;
import io.github.flink.gcp.connector.base.rpc.GrpcEventLoops;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A Spanner service handle and the {@link GrpcEventLoops} lease its channels run under.
 *
 * <p>{@link Spanner} is not a gax {@code BackgroundResource}, so {@code GrpcEventLoops.open} cannot
 * pair the two; this holder does it for {@link SpannerClients#open(DatabaseDestination,
 * com.google.cloud.spanner.SpannerOptions)}. Closing it closes the handle, which waits for its
 * channels to terminate (google-cloud-spanner 6.127.0 waits up to ten seconds for each of its four
 * stubs and its watchdog in turn), and only then releases the lease. The lease is released whatever
 * the handle's close did, so the group never outlives its leases because a handle would not close;
 * Netty then closes any channel still open on it.
 *
 * <p>The handle's own {@code close()} refuses a second call, so the first {@link #close()} here
 * does it and a concurrent or later one waits until that has finished, unless it is interrupted
 * while it waits, as {@link GrpcEventLoops.LeasedClient} does.
 */
@Internal
public final class LeasedSpanner implements AutoCloseable {

    private final Spanner spanner;
    private final GrpcEventLoops.Lease lease;

    private final AtomicBoolean closing = new AtomicBoolean();
    private final CountDownLatch closed = new CountDownLatch(1);

    /**
     * Pairs a handle with the lease its channels were built under. Package-private so that only
     * {@link SpannerClients} pairs them, after checking the settings' channels run on the group.
     *
     * @param spanner the handle
     * @param lease the lease, released after the handle closes
     */
    LeasedSpanner(Spanner spanner, GrpcEventLoops.Lease lease) {
        this.spanner = Preconditions.checkNotNull(spanner, "spanner must not be null");
        this.lease = Preconditions.checkNotNull(lease, "lease must not be null");
    }

    /**
     * Returns the handle; close this holder, not the handle.
     *
     * @return the handle
     */
    public Spanner spanner() {
        return spanner;
    }

    /**
     * Closes the handle and then releases the lease, or waits for the close already under way. An
     * interrupt ends the wait early and is restored.
     */
    @Override
    public void close() {
        if (closing.compareAndSet(false, true)) {
            boolean interrupted = Thread.currentThread().isInterrupted();
            try {
                spanner.close();
            } catch (RuntimeException e) {
                // An interrupt that arrived during the close reaches here only as the cause of the
                // client's exception.
                interrupted |=
                        ExceptionUtils.findThrowable(e, InterruptedException.class).isPresent();
                throw e;
            } finally {
                // The client's close can consume an interrupt without restoring it (6.127.0 wraps
                // the InterruptedException its wait throws), and the lease's release lets an
                // interrupt end its wait for the group only if the thread still carries one.
                if (interrupted && !Thread.currentThread().isInterrupted()) {
                    Thread.currentThread().interrupt();
                }
                try {
                    lease.close();
                } finally {
                    closed.countDown();
                }
            }
            return;
        }
        try {
            closed.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public String toString() {
        return "LeasedSpanner{" + spanner + "}";
    }
}
