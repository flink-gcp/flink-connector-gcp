/*
 * Copyright 2026 laughingman7743
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

package io.github.flink.gcp.connector.testutils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static io.github.flink.gcp.connector.testutils.Awaits.await;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Covers the diagnosis half of {@link Awaits}, which nothing else can: it runs only when an await
 * has already timed out, so a green build never executes it and a broken diagnosis would first be
 * discovered by the CI failure it exists to explain.
 */
@Timeout(30)
class AwaitsTest {

    /** Short enough that the timing-out cases stay quick, long enough to poll more than once. */
    private static final Duration TIMEOUT = Duration.ofMillis(300);

    @Test
    void theFailureMessageCarriesTheDiagnosis() {
        assertThatThrownBy(() -> await("nothing", TIMEOUT, () -> false, () -> "state: stalled"))
                .isInstanceOf(AssertionError.class)
                .hasMessage("Timed out waiting for nothing (waited PT0.3S). state: stalled");
    }

    @Test
    void aThrowingDiagnosisIsReportedInPlaceOfItsText() {
        assertThatThrownBy(
                        () ->
                                await(
                                        "nothing",
                                        TIMEOUT,
                                        () -> false,
                                        () -> {
                                            throw new IllegalStateException("no job");
                                        }))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("Timed out waiting for nothing")
                .hasMessageContaining("The diagnosis itself threw")
                .hasMessageContaining("no job");
    }

    @Test
    void anEmptyDiagnosisAddsNothingToTheMessage() {
        assertThatThrownBy(() -> await("nothing", TIMEOUT, () -> false, () -> ""))
                .isInstanceOf(AssertionError.class)
                .hasMessage("Timed out waiting for nothing (waited PT0.3S).");
    }

    /** A supplier composing its text from something absent is the way null arrives here. */
    @Test
    void aNullDiagnosisAddsNothingToTheMessage() {
        assertThatThrownBy(() -> await("nothing", TIMEOUT, () -> false, () -> null))
                .isInstanceOf(AssertionError.class)
                .hasMessage("Timed out waiting for nothing (waited PT0.3S).");
    }

    @Test
    void aSatisfiedConditionNeverConsultsTheDiagnosis() {
        AtomicInteger consulted = new AtomicInteger();
        assertThatCode(
                        () ->
                                await(
                                        "nothing",
                                        TIMEOUT,
                                        () -> true,
                                        () -> {
                                            consulted.incrementAndGet();
                                            return "should not be read";
                                        }))
                .doesNotThrowAnyException();
        assertThat(consulted).hasValue(0);
    }

    @Test
    void aConditionThatBecomesTrueBeforeTheDeadlineReturns() throws Exception {
        AtomicInteger polls = new AtomicInteger();
        await("the condition to flip", TIMEOUT, () -> polls.incrementAndGet() > 1);
        assertThat(polls).hasValueGreaterThan(1);
    }
}
