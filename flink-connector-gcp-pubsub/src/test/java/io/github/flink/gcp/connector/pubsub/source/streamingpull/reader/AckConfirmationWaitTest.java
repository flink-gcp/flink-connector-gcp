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

import com.google.api.core.ApiFuture;
import com.google.api.core.SettableApiFuture;
import com.google.cloud.pubsub.v1.AckResponse;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link AckConfirmationWait}, which decides whether a completed checkpoint's
 * acknowledgements reached Pub/Sub, and says what an operator has to decide when it cannot tell.
 */
class AckConfirmationWaitTest {

    private final AckConfirmationWait wait = new AckConfirmationWait(Duration.ofSeconds(30));

    @Test
    void aCheckpointWhoseAcknowledgementsAreAllConfirmedReturns() {
        assertThatCode(
                        () ->
                                wait.await(
                                        Arrays.asList(
                                                confirmed(AckResponse.SUCCESSFUL),
                                                confirmed(AckResponse.SUCCESSFUL)),
                                        7L))
                .doesNotThrowAnyException();
    }

    @Test
    void aRejectionAnywhereInTheCheckpointFailsIt() {
        // Every response is read, not only the last: a checkpoint whose first acknowledgement was
        // refused and whose second succeeded must not be reported as durable.
        assertThatThrownBy(
                        () ->
                                wait.await(
                                        Arrays.asList(
                                                confirmed(AckResponse.PERMISSION_DENIED),
                                                confirmed(AckResponse.SUCCESSFUL)),
                                        7L))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("rejected an acknowledgement of checkpoint 7")
                .hasMessageContaining("PERMISSION_DENIED");

        assertThatThrownBy(
                        () ->
                                wait.await(
                                        Arrays.asList(
                                                confirmed(AckResponse.SUCCESSFUL),
                                                confirmed(AckResponse.FAILED_PRECONDITION)),
                                        7L))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("FAILED_PRECONDITION");
    }

    @Test
    void anUnconfirmedAcknowledgementIsReportedAsAnAmbiguityRatherThanAnError() {
        // A failed acknowledgement never completes its future on a subscription without
        // exactly-once delivery, so the timeout must not claim the acknowledgements failed.
        AckConfirmationWait prompt = new AckConfirmationWait(Duration.ofMillis(50));

        assertThatThrownBy(
                        () ->
                                prompt.await(
                                        Arrays.asList(
                                                SettableApiFuture.create(),
                                                SettableApiFuture.create(),
                                                confirmed(AckResponse.SUCCESSFUL)),
                                        7L))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("did not confirm the acknowledgements of checkpoint 7")
                // How many messages are in doubt is what decides whether an operator waits or
                // intervenes, so it counts the checkpoint's acknowledgements rather than the
                // unconfirmed ones.
                .hasMessageContaining("(3 messages)")
                .hasMessageContaining("never completes its future")
                .hasMessageContaining("may have failed, or merely be slow")
                .hasCauseInstanceOf(TimeoutException.class);
    }

    @Test
    void theConfiguredTimeoutIsTheOneWaited() {
        // Both halves of the duration contract: the wait lasts about as long as it was configured
        // to, and the message names that same duration rather than a hardcoded one.
        AckConfirmationWait halfASecond = new AckConfirmationWait(Duration.ofMillis(500));

        long startedNanos = System.nanoTime();
        assertThatThrownBy(
                        () ->
                                halfASecond.await(
                                        Collections.singletonList(SettableApiFuture.create()), 7L))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("within PT0.5S");
        long elapsedMillis = (System.nanoTime() - startedNanos) / 1_000_000;

        // One-sided: a slow machine only makes the wait longer, so the lower bound is what a
        // shorter hardcoded timeout would fail.
        assertThat(elapsedMillis).isGreaterThanOrEqualTo(400L);
    }

    @Test
    void aFailedRoundTripKeepsItsCauseRatherThanTheExecutionWrapper() {
        SettableApiFuture<AckResponse> failed = SettableApiFuture.create();
        IllegalStateException cause = new IllegalStateException("the channel went away");
        failed.setException(cause);

        assertThatThrownBy(() -> wait.await(Collections.singletonList(failed), 7L))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Failed to acknowledge the messages of checkpoint 7")
                .hasCause(cause);
    }

    @Test
    void anInterruptedWaitRestoresTheInterruptForTheCallerToSee() {
        Thread.currentThread().interrupt();
        try {
            assertThatThrownBy(
                            () ->
                                    wait.await(
                                            Collections.singletonList(SettableApiFuture.create()),
                                            7L))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("Interrupted while confirming the acknowledgements")
                    .hasCauseInstanceOf(InterruptedException.class);
            // The reader's task thread reacts to the interrupt, not to this exception, so a wait
            // that swallowed the flag would strand the shutdown it belongs to.
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
        } finally {
            Thread.interrupted();
        }
    }

    private static ApiFuture<AckResponse> confirmed(AckResponse response) {
        SettableApiFuture<AckResponse> future = SettableApiFuture.create();
        future.set(response);
        return future;
    }
}
