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

package io.github.flink.gcp.connector.base.source;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;

import javax.annotation.Nullable;

import java.util.concurrent.Callable;

/**
 * Makes sure a split enumerator reports a failure of the start-up work it hands to {@code
 * SplitEnumeratorContext#callAsync}, even when the enumerator closes before that call's handler
 * reports it.
 *
 * <p>Flink 1.20.5 and 2.2.1 close a split enumerator and then shut the coordinator's executor down
 * with {@code shutdownNow()} ({@code SourceCoordinator.close()}, {@code
 * SourceCoordinatorContext.close()}). The handler of a {@code callAsync} call therefore either
 * finds the enumerator closed, and must not fail the job over its own teardown, or never runs.
 * Either way a failure handed to it used to leave no trace. This class keeps one:
 *
 * <ul>
 *   <li>A failure the work throws while this reporter is open is recorded. The handler {@linkplain
 *       #claim(Throwable) claims} it, which answers the exception the handler fails the job with;
 *       otherwise {@link #close()} logs it at WARN.
 *   <li>A failure the work throws once this reporter has closed is logged where it is thrown, at
 *       INFO: the enumerator's close of what the work calls may have caused it, and the two cannot
 *       be told apart.
 * </ul>
 *
 * <p>The enumerator's handler claims a failure before it looks at its own closed flag, and the
 * enumerator's {@code close()} closes this reporter first: before it sets that flag and before it
 * closes what the work calls. A closed reporter refuses the claim, so a handler that claims once
 * that {@code close()} has begun stays quiet, and a failure that closing what the work calls causes
 * is logged at INFO. Flink itself still drops a failure the handler throws if the coordinator
 * context closes before Flink's handler of a throwing callback checks it ({@code
 * SourceCoordinatorContext.handleUncaughtExceptionFromAsyncCall}); the enumerator cannot observe
 * that.
 *
 * <p>The work runs on Flink's worker thread, the handler on the coordinator thread, and {@link
 * #close()} usually on Flink's closer thread, so every access to the reporter's state synchronizes
 * on it. Lines are logged under the enumerator's logger, so a log configuration scoped to one
 * connector keeps matching them.
 */
@Internal
public final class AsyncStartupFailureReporter {

    private final Logger log;

    /**
     * The only copy of the message kept; a logged line is built from it only when a failure reaches
     * a log call, since most enumerators never fail to start.
     */
    private final String failureMessage;

    /** The failure the work threw while this reporter was open, until it is claimed or logged. */
    @Nullable private Throwable unreported;

    private boolean closed;

    /**
     * Creates a reporter.
     *
     * @param log the enumerator's logger
     * @param failureMessage the whole sentence the job fails with, such as "Failed to initialize
     *     Bigtable Change Streams partitions."; each logged line carries it too
     */
    public AsyncStartupFailureReporter(Logger log, String failureMessage) {
        this.log = Preconditions.checkNotNull(log, "log must not be null");
        this.failureMessage =
                Preconditions.checkNotNull(failureMessage, "failureMessage must not be null");
    }

    /**
     * Returns the start-up work, wrapped so that a failure it throws is recorded or logged here
     * before it reaches the handler.
     *
     * @param work the work to hand to {@code callAsync}
     * @param <T> what the work answers with
     * @return the wrapped work, which throws what {@code work} throws
     */
    public <T> Callable<T> observing(Callable<T> work) {
        Preconditions.checkNotNull(work, "work must not be null");
        return () -> {
            try {
                return work.call();
            } catch (Throwable e) {
                observe(e);
                throw e;
            }
        };
    }

    private void observe(Throwable failure) {
        synchronized (this) {
            if (!closed) {
                unreported = failure;
                return;
            }
        }
        log.info(
                "The enumerator's start-up work failed after the enumerator closed, and that close"
                        + " may have caused the failure, so the job is not failed with it: "
                        + failureMessage,
                failure);
    }

    /**
     * Takes the failure over for the handler to fail the job with, unless this reporter has closed.
     *
     * <p>The handler calls it with the failure it was handed, before it looks at the enumerator's
     * closed flag, and throws what it answers. A closed reporter answers {@code null}: the failure
     * is then logged by {@link #close()}, or was logged where the work threw it, and the handler
     * returns without failing the job over the enumerator's teardown.
     *
     * @param failure the failure the handler was handed
     * @return the exception to fail the job with, carrying the failure message and {@code failure}
     *     as its cause, or {@code null} once this reporter has closed
     */
    @Nullable
    public synchronized FlinkRuntimeException claim(Throwable failure) {
        Preconditions.checkNotNull(failure, "failure must not be null");
        if (closed) {
            return null;
        }
        unreported = null;
        return new FlinkRuntimeException(failureMessage, failure);
    }

    /**
     * Closes this reporter, and logs at WARN a failure the work threw while it was open and the
     * handler has not claimed. The enumerator calls it in its {@code close()}, before it sets its
     * own closed flag and before it closes what the work calls.
     */
    public void close() {
        Throwable failure;
        synchronized (this) {
            closed = true;
            failure = unreported;
            unreported = null;
        }
        if (failure != null) {
            log.warn(
                    failureMessage
                            + " The enumerator closed before it reported the failure, so this is"
                            + " its only trace.",
                    failure);
        }
    }
}
