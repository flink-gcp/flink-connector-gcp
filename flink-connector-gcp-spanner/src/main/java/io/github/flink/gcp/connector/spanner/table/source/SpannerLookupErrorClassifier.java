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

package io.github.flink.gcp.connector.spanner.table.source;

import org.apache.flink.util.ExceptionUtils;

import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.SpannerException;

import java.util.EnumSet;
import java.util.Set;

/** Classifies point-read failures eligible for another client call. */
final class SpannerLookupErrorClassifier {

    /**
     * Statuses a point read is issued again on. These are <em>not</em> the client's own retryable
     * set: {@code singleUse().readRow(...)} reaches {@code StreamingRead} rather than the unary
     * {@code Read}, and {@code SpannerStubSettings} gives that RPC an empty retryable set
     * (google-cloud-spanner 6.120.0). What the client still retries beneath this loop comes from
     * {@code SpannerExceptionFactory.isRetryable}, which of these three names covers only {@code
     * UNAVAILABLE}, and that unless the cause is an SSL handshake or a channel shutdown. So {@code
     * ABORTED} and {@code DEADLINE_EXCEEDED} would be retried by nobody without this loop, which is
     * what {@code lookup.max-retries} buys here.
     *
     * <p>{@code RESOURCE_EXHAUSTED} is absent, and the sink's {@code SpannerErrorClassifier}, whose
     * constant of the same name includes it, is not stale. What separates the two is backoff, not
     * polarity: both sets authorize another attempt, but the writer retries on a {@code
     * RetrySchedule} and sleeps between attempts, so it can serve out the wait this status asks
     * for. The client honours that wait too, retrying the status on this read path precisely when
     * the server attached a retry delay. This loop has no backoff at all, so including the status
     * would re-issue at once and spend the whole budget against the very wait that was requested.
     * ADR-0084 records the same shape on the BigQuery read path.
     */
    private static final Set<ErrorCode> TRANSIENT_CODES =
            EnumSet.of(ErrorCode.ABORTED, ErrorCode.DEADLINE_EXCEEDED, ErrorCode.UNAVAILABLE);

    private SpannerLookupErrorClassifier() {}

    static boolean isTransient(Throwable failure) {
        return ExceptionUtils.findThrowable(failure, SpannerException.class)
                .map(SpannerException::getErrorCode)
                .map(TRANSIENT_CODES::contains)
                .orElse(false);
    }
}
