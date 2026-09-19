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

package io.github.flink.gcp.connector.testutils.pubsub;

import com.google.api.core.SettableApiFuture;
import com.google.pubsub.v1.PullResponse;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PubSubTestClientsTest {
    @Test
    void expiredPullIsCancelledAndCannotEstablishAnEmptySubscription() {
        SettableApiFuture<PullResponse> pending = SettableApiFuture.create();
        assertThatThrownBy(() -> PubSubTestClients.awaitPull(pending, 0))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("without a service response")
                .hasCauseInstanceOf(java.util.concurrent.TimeoutException.class);
        assertThat(pending.isCancelled()).isTrue();
    }

    @Test
    void completedPullIsReturned() throws Exception {
        SettableApiFuture<PullResponse> pending = SettableApiFuture.create();
        PullResponse response = PullResponse.newBuilder().build();
        pending.set(response);
        assertThat(PubSubTestClients.awaitPull(pending, 1)).isSameAs(response);
    }

    @Test
    void serviceFailureIsNotMistakenForAnEmptySubscription() {
        SettableApiFuture<PullResponse> pending = SettableApiFuture.create();
        IllegalStateException denied = new IllegalStateException("denied");
        pending.setException(denied);
        assertThatThrownBy(() -> PubSubTestClients.awaitPull(pending, 1))
                .isInstanceOf(IllegalStateException.class)
                .hasCause(denied);
    }

    @Test
    void interruptionCancelsThePendingPull() {
        SettableApiFuture<PullResponse> pending = SettableApiFuture.create();
        Thread.currentThread().interrupt();
        try {
            assertThatThrownBy(() -> PubSubTestClients.awaitPull(pending, Long.MAX_VALUE))
                    .isInstanceOf(InterruptedException.class);
            assertThat(pending.isCancelled()).isTrue();
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    void everyClientIsClosedAndTheFirstFailureKeepsLaterFailures() {
        List<String> closed = new ArrayList<>();
        RuntimeException first = new RuntimeException("subscription");
        RuntimeException second = new RuntimeException("topic");
        assertThatThrownBy(
                        () ->
                                PubSubTestClients.closeAll(
                                        () -> {
                                            closed.add("subscription");
                                            throw first;
                                        },
                                        () -> {
                                            closed.add("topic");
                                            throw second;
                                        },
                                        () -> closed.add("channel")))
                .isSameAs(first)
                .hasSuppressedException(second);
        assertThat(closed).containsExactly("subscription", "topic", "channel");
    }
}
