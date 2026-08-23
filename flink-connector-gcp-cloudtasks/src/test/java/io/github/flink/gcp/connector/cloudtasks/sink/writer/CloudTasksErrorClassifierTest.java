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

package io.github.flink.gcp.connector.cloudtasks.sink.writer;

import com.google.api.gax.rpc.StatusCode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link CloudTasksErrorClassifier}. */
class CloudTasksErrorClassifierTest {

    @ParameterizedTest
    @EnumSource(
            value = StatusCode.Code.class,
            names = {"UNAVAILABLE", "DEADLINE_EXCEEDED", "RESOURCE_EXHAUSTED"})
    void findsEveryTransientStatusThroughUnclassifiableWrappers(StatusCode.Code code) {
        Throwable failure =
                new IOException(
                        "outer", new IllegalStateException(FakeTaskCreator.apiException(code)));

        assertThat(CloudTasksErrorClassifier.transientCode(failure)).isEqualTo(code);
    }

    @Test
    void reportsTheOutermostClassifiableStatus() {
        Throwable failure =
                new IOException(
                        "outer",
                        FakeTaskCreator.apiException(
                                StatusCode.Code.INTERNAL,
                                FakeTaskCreator.apiException(StatusCode.Code.INVALID_ARGUMENT)));

        assertThat(CloudTasksErrorClassifier.statusCode(failure))
                .isEqualTo(StatusCode.Code.INTERNAL);
    }

    @Test
    void keepsTheTransientScanAndReportedStatusAsymmetric() {
        Throwable failure =
                FakeTaskCreator.apiException(
                        StatusCode.Code.INVALID_ARGUMENT,
                        FakeTaskCreator.apiException(StatusCode.Code.UNAVAILABLE));

        assertThat(CloudTasksErrorClassifier.statusCode(failure))
                .isEqualTo(StatusCode.Code.INVALID_ARGUMENT);
        assertThat(CloudTasksErrorClassifier.transientCode(failure))
                .isEqualTo(StatusCode.Code.UNAVAILABLE);
    }

    @Test
    void doesNotScanForAnInvalidArgument() {
        Throwable failure =
                FakeTaskCreator.apiException(
                        StatusCode.Code.INTERNAL,
                        FakeTaskCreator.apiException(StatusCode.Code.INVALID_ARGUMENT));

        assertThat(CloudTasksErrorClassifier.statusCode(failure))
                .isEqualTo(StatusCode.Code.INTERNAL);
        assertThat(CloudTasksErrorClassifier.transientCode(failure)).isNull();
    }

    @Test
    void answersNullWhenTheChainCarriesNoStatus() {
        Throwable failure = new IOException("outer", new IllegalStateException("boom"));

        assertThat(CloudTasksErrorClassifier.statusCode(failure)).isNull();
        assertThat(CloudTasksErrorClassifier.transientCode(failure)).isNull();
    }
}
