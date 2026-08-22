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

import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.SpannerExceptionFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link SpannerLookupErrorClassifier}. */
class SpannerLookupErrorClassifierTest {

    @ParameterizedTest
    @EnumSource(
            value = ErrorCode.class,
            names = {"ABORTED", "DEADLINE_EXCEEDED", "UNAVAILABLE"})
    void classifiesThePointReadRetryStatusesAsTransient(ErrorCode code) {
        assertThat(SpannerLookupErrorClassifier.isTransient(failure(code))).isTrue();
    }

    @ParameterizedTest
    @EnumSource(
            value = ErrorCode.class,
            names = {"ABORTED", "DEADLINE_EXCEEDED", "UNAVAILABLE"},
            mode = EnumSource.Mode.EXCLUDE)
    void rejectsEveryOtherStatus(ErrorCode code) {
        // RESOURCE_EXHAUSTED is the one this covers by name elsewhere: the client retries it when
        // the server attached a delay and then waits that delay, which a backoff-free loop here
        // would spend rather than observe.
        assertThat(SpannerLookupErrorClassifier.isTransient(failure(code))).isFalse();
    }

    @Test
    void findsATransientStatusThroughClientWrappers() {
        Throwable wrapped =
                new IOException("outer", new RuntimeException(failure(ErrorCode.UNAVAILABLE)));

        assertThat(SpannerLookupErrorClassifier.isTransient(wrapped)).isTrue();
    }

    @Test
    void aFailureCarryingNoSpannerStatusIsNotTransient() {
        assertThat(SpannerLookupErrorClassifier.isTransient(new IllegalStateException("boom")))
                .isFalse();
    }

    private static RuntimeException failure(ErrorCode code) {
        return SpannerExceptionFactory.newSpannerException(code, "scripted " + code);
    }
}
