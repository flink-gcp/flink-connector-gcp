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

package io.github.flink.gcp.connector.firestore.sink.writer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.util.Preconditions;

import com.google.cloud.firestore.BulkWriter;
import com.google.cloud.firestore.BulkWriterException;
import io.grpc.Status;

import java.util.EnumSet;
import java.util.Set;

/**
 * Decides whether the client library's {@code BulkWriter} retries a failed write attempt, in place
 * of the library's own default listener.
 *
 * <p>The decision is the library's default with one number made configurable: an attempt is retried
 * while it failed with a status in {@code RETRYABLE_CODES} and fewer than {@code writeMaxAttempts}
 * attempts have failed. The code set is a pinned copy of what the default listener reads from the
 * {@code BatchWrite} call settings, and {@code BulkWriterRetryPolicyTest} holds the two equal, so a
 * client upgrade that changes the library's set fails a test rather than silently diverging from
 * it.
 *
 * <p>The listener runs on the {@code BulkWriter}'s executor while the library holds its internal
 * lock, so it only counts and decides: the two observers must return at once and must not call back
 * into the writer or the library.
 */
@Internal
public final class BulkWriterRetryPolicy implements BulkWriter.WriteErrorCallback {

    /** The statuses the client library retries a {@code BatchWrite} write on. */
    @VisibleForTesting
    static final Set<Status.Code> RETRYABLE_CODES =
            EnumSet.of(
                    Status.Code.RESOURCE_EXHAUSTED, Status.Code.UNAVAILABLE, Status.Code.ABORTED);

    /** Receives the policy's observations, on the {@code BulkWriter}'s executor. */
    interface Observer {

        /** A write attempt failed; called for every failed attempt, retried or not. */
        void attemptFailed();

        /** A failed write attempt is being retried. */
        void retrying();
    }

    private final int writeMaxAttempts;
    private final Observer observer;

    /**
     * Creates the policy.
     *
     * @param writeMaxAttempts how many attempts a write gets, the first one included
     * @param observer what to tell about each failed attempt
     */
    BulkWriterRetryPolicy(int writeMaxAttempts, Observer observer) {
        Preconditions.checkArgument(writeMaxAttempts > 0, "writeMaxAttempts must be positive");
        this.writeMaxAttempts = writeMaxAttempts;
        this.observer = Preconditions.checkNotNull(observer, "observer must not be null");
    }

    @Override
    public boolean onError(BulkWriterException error) {
        observer.attemptFailed();
        boolean retry = shouldRetry(error.getStatus().getCode(), error.getFailedAttempts());
        if (retry) {
            observer.retrying();
        }
        return retry;
    }

    /**
     * Whether a write whose latest attempt failed with {@code code}, after {@code failedAttempts}
     * failed attempts in all, gets another one.
     */
    @VisibleForTesting
    boolean shouldRetry(Status.Code code, int failedAttempts) {
        return failedAttempts < writeMaxAttempts && RETRYABLE_CODES.contains(code);
    }
}
