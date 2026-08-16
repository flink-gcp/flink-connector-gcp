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

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

import io.github.flink.gcp.connector.pubsub.source.streamingpull.SubscriptionSplit;

import javax.annotation.Nullable;

/**
 * One assigned split, the subscriber serving it, and whether Flink has paused it.
 *
 * <p>The split itself is retained because reopening a parked one needs it, and a {@code null}
 * subscriber <em>is</em> the parked state: {@link SubscriberRoster#addSplit} either opens one or
 * throws, so nothing else can produce one, and a second flag could disagree with it. {@code paused}
 * is a second <em>axis</em> rather than a second parked flag — a paused split usually still has its
 * subscriber — and the one invariant tying the axes together, parked implies paused, is enforced at
 * both ends: {@link #park()} requires the pause, and {@link #resume()} requires the subscriber.
 *
 * <p>Deliberately dumb: state and its invariant only. The metrics and logging of every transition
 * stay at the call sites that make it, because the registry give-back loops at close are
 * roster-level and splitting the metric sites across two classes would hide the pairing.
 */
@Internal
final class SubscriberSlot {

    private final SubscriptionSplit split;
    @Nullable private PullSubscriber subscriber;
    private boolean paused;

    SubscriberSlot(SubscriptionSplit split, PullSubscriber subscriber) {
        this.split = split;
        this.subscriber = subscriber;
    }

    SubscriptionSplit split() {
        return split;
    }

    /** The subscriber serving the split, or {@code null} while the slot is parked. */
    @Nullable
    PullSubscriber subscriber() {
        return subscriber;
    }

    boolean isParked() {
        return subscriber == null;
    }

    boolean isPaused() {
        return paused;
    }

    void pause() {
        paused = true;
    }

    /**
     * Lifts the pause. The roster reopens a parked slot before lifting its pause, and the
     * precondition is what makes that ordering structural rather than a calling convention: a
     * resume arriving first fails here instead of exposing a {@code null} subscriber to the drain.
     */
    void resume() {
        Preconditions.checkState(
                subscriber != null, "A parked split must be reopened before its pause lifts.");
        paused = false;
    }

    /**
     * Drops the subscriber, making the slot parked.
     *
     * <p>Only a paused slot may be parked: the drain skips a paused split and dereferences the
     * subscriber of every other one, so "parked implies paused" is what keeps it off a subscriber
     * that is gone.
     */
    void park() {
        Preconditions.checkState(paused, "Only a paused split may be parked.");
        subscriber = null;
    }

    /** Installs the fresh subscriber a resume opened for a parked slot. */
    void reopen(PullSubscriber subscriber) {
        this.subscriber = subscriber;
    }
}
