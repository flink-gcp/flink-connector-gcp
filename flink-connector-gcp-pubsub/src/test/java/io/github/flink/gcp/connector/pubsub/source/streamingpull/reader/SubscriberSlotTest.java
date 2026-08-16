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

import io.github.flink.gcp.connector.pubsub.source.SubscriptionDestination;
import io.github.flink.gcp.connector.pubsub.source.streamingpull.SubscriptionSplit;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link SubscriberSlot}. */
class SubscriberSlotTest {

    private final FakePullSubscriber subscriber = new FakePullSubscriber(() -> {});
    private final SubscriberSlot slot =
            new SubscriberSlot(
                    new SubscriptionSplit(SubscriptionDestination.of("project", "sub"), "0"),
                    subscriber);

    @Test
    void parkingAnUnpausedSlotThrows() {
        // "Parked implies paused" is the invariant the drain relies on: it skips a paused split
        // and dereferences the subscriber of every other one, so a park without a pause would
        // hand the drain a null.
        assertThatThrownBy(slot::park).isInstanceOf(IllegalStateException.class);

        assertThat(slot.isParked()).isFalse();
        assertThat(slot.subscriber()).isSameAs(subscriber);
    }

    @Test
    void parkingDropsTheSubscriberAndKeepsThePause() {
        slot.pause();
        slot.park();

        assertThat(slot.isParked()).isTrue();
        assertThat(slot.isPaused()).isTrue();
        assertThat(slot.subscriber()).isNull();
    }

    @Test
    void resumingWhileStillParkedThrows() {
        // The other end of "parked implies paused": the roster reopens a parked slot before it
        // lifts the pause, and the precondition is what makes that ordering structural — a resume
        // arriving first fails instead of exposing a null subscriber to the drain.
        slot.pause();
        slot.park();

        assertThatThrownBy(slot::resume).isInstanceOf(IllegalStateException.class);
        assertThat(slot.isPaused()).isTrue();

        FakePullSubscriber reopened = new FakePullSubscriber(() -> {});
        slot.reopen(reopened);
        slot.resume();
        assertThat(slot.isParked()).isFalse();
        assertThat(slot.isPaused()).isFalse();
        assertThat(slot.subscriber()).isSameAs(reopened);
    }
}
