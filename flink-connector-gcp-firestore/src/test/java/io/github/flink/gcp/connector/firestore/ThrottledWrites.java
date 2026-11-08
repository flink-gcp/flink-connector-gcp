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

package io.github.flink.gcp.connector.firestore;

import com.google.api.gax.rpc.StatusCode;
import com.google.cloud.datastore.DatastoreException;
import io.github.flink.gcp.connector.base.rpc.StatusCodes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.EnumSet;
import java.util.Set;
import java.util.concurrent.Callable;

/**
 * Sends a write that arranges a gated test's data again, with backoff, while the service answers it
 * {@code RESOURCE_EXHAUSTED} or {@code UNAVAILABLE} (#2079). Public, because the Datastore-mode
 * harness writes through it from the other package root.
 *
 * <p>A database created for a class can answer a write {@code RESOURCE_EXHAUSTED} ("This database
 * has exceeded their maximum bandwidth for writes, please retry with exponential backoff") or
 * {@code UNAVAILABLE} ("The service is temporarily unavailable. Please retry with exponential
 * backoff."). Both answers ask the client to retry, and the connector's own writers do (ADR-0171).
 * The scheduled run of 2026-11-07 saw the first and a manual run of 2026-11-08 both, 1 to 27
 * seconds after the databases were created. The earlier runs, from 2026-10-11, wrote from the same
 * two forks to fresh databases and saw neither. A single fresh database written from a workstation
 * on 2026-11-08 answered every one of 39 writes over its first three minutes, so the throttle is
 * not a fixed warm-up that a wait after creation could cover; retrying each write is what the
 * service asks for.
 *
 * <p>Every write a test sends this way must be safe to apply twice, such as an upsert, because a
 * write answered {@code UNAVAILABLE} may have been applied. A write a test expects to be refused
 * may go through here too, so a throttle does not stand in for the refusal; its refusal status is
 * never one this class retries, so the test still sees it. A write whose throttle a test measures,
 * such as the over-10-MiB probes, never does. Any {@code RESOURCE_EXHAUSTED} is retried, whatever
 * its message, because the cost of retrying an exhausted quota is a slower failure that still
 * carries the service's answer.
 */
public final class ThrottledWrites {

    private static final Logger LOG = LoggerFactory.getLogger(ThrottledWrites.class);

    private static final Set<StatusCode.Code> RETRIED =
            EnumSet.of(StatusCode.Code.RESOURCE_EXHAUSTED, StatusCode.Code.UNAVAILABLE);

    /**
     * No attempt of a write starts later than this after its first, so a write throttled throughout
     * ends within about three minutes, its last attempt included. A test of several such writes can
     * still reach its ten-minute bound first, which then reports the timeout, not the throttle.
     */
    private static final Duration DEADLINE = Duration.ofMinutes(2);

    private static final Duration FIRST_BACKOFF = Duration.ofSeconds(1);

    private static final Duration MAX_BACKOFF = Duration.ofSeconds(16);

    private ThrottledWrites() {}

    /**
     * Calls the write until it returns, retrying a failure that carries {@code RESOURCE_EXHAUSTED}
     * or {@code UNAVAILABLE} anywhere in its cause chain.
     *
     * @param write the write, called again from the start on each attempt
     * @return what the write returned
     * @throws Exception the write's first failure of any other status, or an {@link AssertionError}
     *     carrying the last throttled failure when another attempt would start after the deadline
     */
    public static <T> T retry(Callable<T> write) throws Exception {
        return retry(write, DEADLINE);
    }

    /** As {@link #retry(Callable)}, with the given deadline; for the unit test. */
    static <T> T retry(Callable<T> write, Duration deadlineAfter) throws Exception {
        long deadline = System.nanoTime() + deadlineAfter.toNanos();
        long backoffMillis = FIRST_BACKOFF.toMillis();
        for (int attempt = 1; ; attempt++) {
            try {
                return write.call();
            } catch (Exception e) {
                StatusCode.Code code = retriedCode(e);
                if (code == null) {
                    throw e;
                }
                if (System.nanoTime() + backoffMillis * 1_000_000L - deadline >= 0) {
                    throw new AssertionError(
                            "a write was still answered "
                                    + code
                                    + " on attempt "
                                    + attempt
                                    + ", and another would start after "
                                    + deadlineAfter,
                            e);
                }
                LOG.info(
                        "Attempt {}: a write was answered {}; retrying in {} ms.",
                        attempt,
                        code,
                        backoffMillis);
                Thread.sleep(backoffMillis);
                backoffMillis = Math.min(backoffMillis * 2, MAX_BACKOFF.toMillis());
            }
        }
    }

    /** Returns the retried status the failure carries anywhere in its cause chain, or null. */
    @Nullable
    static StatusCode.Code retriedCode(Throwable failure) {
        for (Throwable t = failure; t != null; t = t.getCause()) {
            StatusCode.Code code = codeOf(t);
            if (code != null && RETRIED.contains(code)) {
                return code;
            }
        }
        return null;
    }

    /**
     * Reads the status a throwable itself carries: the gax or gRPC status, or the reason of a
     * {@link DatastoreException}, which names the status and may carry no gax cause.
     */
    @Nullable
    private static StatusCode.Code codeOf(Throwable t) {
        StatusCode.Code code = StatusCodes.codeOf(t);
        if (code != null || !(t instanceof DatastoreException)) {
            return code;
        }
        String reason = ((DatastoreException) t).getReason();
        if (reason == null) {
            return null;
        }
        try {
            return StatusCode.Code.valueOf(reason);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }
}
