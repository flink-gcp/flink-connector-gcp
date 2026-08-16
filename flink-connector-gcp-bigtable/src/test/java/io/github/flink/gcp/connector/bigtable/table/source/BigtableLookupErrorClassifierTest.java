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

package io.github.flink.gcp.connector.bigtable.table.source;

import com.google.api.gax.grpc.GrpcStatusCode;
import com.google.api.gax.rpc.ApiExceptionFactory;
import com.google.api.gax.rpc.StatusCode;
import io.grpc.Status;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link BigtableLookupErrorClassifier}. */
class BigtableLookupErrorClassifierTest {

    @ParameterizedTest
    @EnumSource(
            value = StatusCode.Code.class,
            names = {"DEADLINE_EXCEEDED", "UNAVAILABLE", "ABORTED"})
    void classifiesOnlyThePointReadRetryStatusesAsTransient(StatusCode.Code code) {
        assertThat(BigtableLookupErrorClassifier.isTransient(failure(code))).isTrue();
    }

    @ParameterizedTest
    @EnumSource(
            value = StatusCode.Code.class,
            names = {"DEADLINE_EXCEEDED", "UNAVAILABLE", "ABORTED"},
            mode = EnumSource.Mode.EXCLUDE)
    void rejectsEveryOtherStatus(StatusCode.Code code) {
        assertThat(BigtableLookupErrorClassifier.isTransient(failure(code))).isFalse();
    }

    @Test
    void findsATransientStatusThroughClientWrappers() {
        Throwable wrapped =
                new IOException(
                        "outer", new RuntimeException(failure(StatusCode.Code.UNAVAILABLE)));

        assertThat(BigtableLookupErrorClassifier.isTransient(wrapped)).isTrue();
    }

    @Test
    void aFailureWithoutAStatusIsNotTransient() {
        assertThat(BigtableLookupErrorClassifier.isTransient(new IllegalStateException("boom")))
                .isFalse();
    }

    private static RuntimeException failure(StatusCode.Code code) {
        return ApiExceptionFactory.createException(
                new RuntimeException("scripted " + code),
                GrpcStatusCode.of(Status.Code.valueOf(code.name())),
                false);
    }
}
