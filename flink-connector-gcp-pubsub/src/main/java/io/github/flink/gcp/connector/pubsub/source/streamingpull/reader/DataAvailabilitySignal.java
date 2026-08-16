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

package io.github.flink.gcp.connector.pubsub.source.streamingpull.reader;

import org.apache.flink.annotation.Internal;

import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * The reader-wide signal a fetch with nothing buffered parks on, raised when a subscriber has
 * messages or has failed.
 *
 * <p>The data-available signal is armed <em>before</em> draining, so a message arriving mid-drain
 * completes the armed future rather than being missed, and it is level-triggered, so a signal that
 * arrives while no wait is armed is remembered instead of dropped. Both matter: a fetch that parks
 * on a lost signal is never woken again on the shutdown path, which would leave the subscribers
 * open and their messages unnacked.
 *
 * <p>Every subscriber shares that one signal. Composing a notification future per subscriber on
 * each fetch would accumulate callbacks on the ones with nothing to deliver, which is why the
 * signal is reader-wide; a fetch then drains up to {@code maxRecordsPerFetch} messages from each
 * split rather than one.
 */
@Internal
final class DataAvailabilitySignal {

    private final Object signalLock = new Object();

    @GuardedBy("signalLock")
    private CompletableFuture<Void> dataAvailable = new CompletableFuture<>();

    /** Remembers a signal that arrived while no wait was armed, so it cannot be lost. */
    @GuardedBy("signalLock")
    private boolean signalled;

    /**
     * Returns the future the next wait parks on, which is already complete when a signal arrived
     * while no wait was in progress.
     *
     * <p>The remembered flag is what makes the signal level-triggered, and it is load-bearing:
     * {@code SplitFetcher} checks its own wake-up flag <em>before</em> entering {@code fetch()} and
     * calls {@code wakeUp()} exactly once per event, so an edge-triggered signal delivered in the
     * window between that check and the arming below would be lost. On the shutdown path that is
     * unrecoverable — nothing else would ever wake the fetcher, so the reader would never be closed
     * and its messages never nacked.
     */
    CompletableFuture<Void> arm() {
        synchronized (signalLock) {
            if (signalled) {
                signalled = false;
                return CompletableFuture.completedFuture(null);
            }
            dataAvailable = new CompletableFuture<>();
            return dataAvailable;
        }
    }

    /**
     * Raises the signal: remembers it for the next arm, and completes the future outside the lock
     * so a waiter's continuation never runs while holding it.
     */
    void raise() {
        CompletableFuture<Void> current;
        synchronized (signalLock) {
            signalled = true;
            current = dataAvailable;
        }
        current.complete(null);
    }

    /**
     * Parks on the given armed future until it completes, or for at most {@code parkTimeoutMillis}
     * when that is positive.
     */
    void await(CompletableFuture<Void> signal, long parkTimeoutMillis) throws IOException {
        try {
            if (parkTimeoutMillis > 0) {
                signal.get(parkTimeoutMillis, TimeUnit.MILLISECONDS);
            } else {
                signal.get();
            }
        } catch (TimeoutException e) {
            // Woke only to re-evaluate the checkpoint detector; the caller drains nothing and
            // returns an empty batch. Arming is level-triggered, so a signal that arrived while
            // this wait was running is remembered by the next arm rather than lost.
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            // Unreachable: the signal is only ever completed normally. Subscriber failures surface
            // from pullMessages on the following drain.
            throw new IOException("Failed while waiting for Pub/Sub messages.", e);
        }
    }
}
