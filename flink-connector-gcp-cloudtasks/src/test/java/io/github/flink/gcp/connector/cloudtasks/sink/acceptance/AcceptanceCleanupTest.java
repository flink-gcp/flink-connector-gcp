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

package io.github.flink.gcp.connector.cloudtasks.sink.acceptance;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AcceptanceCleanupTest {
    @Test
    void aFailedJobStopStillStopsOtherJobsAndClosesTheProxyAndEvidence() {
        List<String> closed = new ArrayList<>();
        Exception first = new java.util.concurrent.TimeoutException("first job did not stop");
        Exception later = new IllegalStateException("proxy close failed");

        assertThatThrownBy(
                        () ->
                                AcceptanceCleanup.closeAll(
                                        () -> {
                                            closed.add("first job");
                                            throw first;
                                        },
                                        () -> closed.add("second job"),
                                        () -> {
                                            closed.add("proxy");
                                            throw later;
                                        },
                                        () -> closed.add("evidence")))
                .isSameAs(first)
                .hasSuppressedException(later);
        assertThat(closed).containsExactly("first job", "second job", "proxy", "evidence");
    }

    @Test
    void anErrorStillClosesLaterResourcesAndPropagatesAsTheSameError() {
        List<String> closed = new ArrayList<>();
        Error failure = new NoClassDefFoundError("transport close failed");

        assertThatThrownBy(
                        () ->
                                AcceptanceCleanup.closeAll(
                                        null,
                                        () -> {
                                            throw failure;
                                        },
                                        () -> closed.add("evidence")))
                .isSameAs(failure);
        assertThat(closed).containsExactly("evidence");
    }
}
