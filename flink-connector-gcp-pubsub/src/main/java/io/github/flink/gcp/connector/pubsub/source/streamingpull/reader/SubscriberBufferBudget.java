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
import org.apache.flink.annotation.VisibleForTesting;

import io.github.flink.gcp.connector.pubsub.source.streamingpull.SubscriberBufferLimitExceededEvent;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Consumer;

/**
 * The hard aggregate bound on messages still held in one source reader's subscriber deques.
 *
 * <p>Reservations happen on SDK callback threads while releases and pause transitions happen on
 * Flink's fetcher thread, so every state transition is guarded by this object's monitor. The
 * response is chosen at the first rejected reservation: an entirely paused reader parks its
 * subscribers, while any unpaused split makes the coordinator fail the job. A resume or active
 * split registration racing that park promotes it to failure, because a stopped or newly opened
 * subscriber can no longer be left active.
 */
@Internal
final class SubscriberBufferBudget {

    enum Response {
        ADMIT,
        PARK,
        FAIL
    }

    private static final Consumer<SubscriberBufferLimitExceededEvent> NO_EVENT = event -> {};
    private static final Admission ADMITTED =
            new Admission(Response.ADMIT, null, null, false, NO_EVENT);

    private final long maxMessages;
    private final long maxBytes;
    private final Consumer<SubscriberBufferLimitExceededEvent> failureReporter;

    private final Map<String, SubscriberState> subscribers = new LinkedHashMap<>();

    private long messages;
    private long bytes;
    private long pauseTransitions;
    private Response response = Response.ADMIT;
    @Nullable private SubscriberBufferLimitExceededEvent overflowEvent;

    SubscriberBufferBudget(
            long maxMessages,
            long maxBytes,
            Consumer<SubscriberBufferLimitExceededEvent> failureReporter) {
        this.maxMessages = maxMessages;
        this.maxBytes = maxBytes;
        this.failureReporter = failureReporter;
    }

    static SubscriberBufferBudget unbounded() {
        return new SubscriberBufferBudget(Long.MAX_VALUE, Long.MAX_VALUE, NO_EVENT);
    }

    synchronized Admission register(String splitId, Runnable stopAsync) {
        subscribers.put(splitId, new SubscriberState(stopAsync));
        if (response == Response.ADMIT) {
            return ADMITTED;
        }
        SubscriberBufferLimitExceededEvent event = null;
        if (response == Response.PARK) {
            // The park was selected only because every subscriber registered at the crossing was
            // paused. A newly assigned subscriber is active, so leaving PARK in place would skip it
            // during the park and keep every later callback in the rejected PARK state forever.
            response = Response.FAIL;
            event = overflowEvent;
        }
        // A subscriber opened after either terminal response must not keep receiving while the
        // coordinator failure is in flight. The caller responds after attaching it to the roster,
        // so ordinary reader cleanup still owns it if stopping or reporting throws.
        return new Admission(
                Response.FAIL, new Runnable[] {stopAsync}, event, true, failureReporter);
    }

    synchronized void unregister(String splitId) {
        subscribers.remove(splitId);
        resetAfterParkIfEmpty();
    }

    void setPaused(String splitId, boolean paused) {
        setPaused(Collections.singletonList(splitId), paused);
    }

    void setPaused(Collection<String> splitIds, boolean paused) {
        SubscriberBufferLimitExceededEvent event = null;
        synchronized (this) {
            if (!splitIds.isEmpty()) {
                pauseTransitions++;
            }
            for (String splitId : splitIds) {
                SubscriberState subscriber = subscribers.get(splitId);
                if (subscriber != null) {
                    subscriber.paused = paused;
                }
            }
            if (!paused && !splitIds.isEmpty() && response == Response.PARK) {
                // The crossing callback already asked every subscriber to stop. If a resume lands
                // before the fetcher turns that request into the ordinary park lifecycle, leaving
                // this now-unpaused slot active would strand a stopped subscriber forever.
                response = Response.FAIL;
                event = overflowEvent;
            }
        }
        if (event != null) {
            failureReporter.accept(event);
        }
    }

    Admission tryReserve(String splitId, long messageBytes) {
        SubscriberBufferLimitExceededEvent event = null;
        Runnable[] stops = null;
        Response result;
        boolean triggeringSubscriberIncluded;
        synchronized (this) {
            if (response != Response.ADMIT) {
                return new Admission(response, null, null, false, NO_EVENT);
            }
            long attemptedMessages = messages == Long.MAX_VALUE ? Long.MAX_VALUE : messages + 1;
            long attemptedBytes =
                    messageBytes > Long.MAX_VALUE - bytes ? Long.MAX_VALUE : bytes + messageBytes;
            if (messages < maxMessages && messageBytes <= maxBytes - bytes) {
                messages = attemptedMessages;
                bytes = attemptedBytes;
                return ADMITTED;
            }

            boolean allPaused =
                    !subscribers.isEmpty()
                            && subscribers.values().stream().allMatch(state -> state.paused);
            response = allPaused ? Response.PARK : Response.FAIL;
            result = response;
            triggeringSubscriberIncluded = subscribers.containsKey(splitId);
            stops =
                    subscribers.values().stream()
                            .map(state -> state.stopAsync)
                            .toArray(Runnable[]::new);
            overflowEvent =
                    new SubscriberBufferLimitExceededEvent(
                            splitId, attemptedMessages, attemptedBytes, maxMessages, maxBytes);
            if (result == Response.FAIL) {
                event = overflowEvent;
            }
        }
        return new Admission(result, stops, event, triggeringSubscriberIncluded, failureReporter);
    }

    synchronized void release(long releasedMessages, long releasedBytes) {
        messages -= releasedMessages;
        bytes -= releasedBytes;
        if (messages < 0 || bytes < 0) {
            throw new IllegalStateException(
                    "Subscriber buffer accounting became negative: "
                            + messages
                            + " messages, "
                            + bytes
                            + " bytes.");
        }
        resetAfterParkIfEmpty();
    }

    synchronized boolean parkingRequested() {
        return response == Response.PARK;
    }

    @VisibleForTesting
    synchronized boolean isPaused(String splitId) {
        SubscriberState subscriber = subscribers.get(splitId);
        return subscriber != null && subscriber.paused;
    }

    @VisibleForTesting
    synchronized long pauseTransitions() {
        return pauseTransitions;
    }

    synchronized BufferUsage usage() {
        return BufferUsage.of(Math.toIntExact(messages), bytes);
    }

    private void resetAfterParkIfEmpty() {
        if (response == Response.PARK && subscribers.isEmpty() && messages == 0 && bytes == 0) {
            response = Response.ADMIT;
        }
    }

    static final class Admission {

        private final Response response;
        private final Runnable[] stops;
        private final SubscriberBufferLimitExceededEvent event;
        private final boolean triggeringSubscriberIncluded;
        private final Consumer<SubscriberBufferLimitExceededEvent> failureReporter;

        private Admission(
                Response response,
                Runnable[] stops,
                SubscriberBufferLimitExceededEvent event,
                boolean triggeringSubscriberIncluded,
                Consumer<SubscriberBufferLimitExceededEvent> failureReporter) {
            this.response = response;
            this.stops = stops;
            this.event = event;
            this.triggeringSubscriberIncluded = triggeringSubscriberIncluded;
            this.failureReporter = failureReporter;
        }

        boolean isAdmitted() {
            return response == Response.ADMIT;
        }

        void respond() {
            respond(() -> {});
        }

        void respond(Runnable triggeringSubscriberStop) {
            if (stops == null) {
                if (response != Response.ADMIT) {
                    triggeringSubscriberStop.run();
                }
                return;
            }
            RuntimeException firstFailure = null;
            for (Runnable stop : stops) {
                try {
                    stop.run();
                } catch (RuntimeException failure) {
                    if (firstFailure == null) {
                        firstFailure = failure;
                    } else {
                        firstFailure.addSuppressed(failure);
                    }
                }
            }
            if (!triggeringSubscriberIncluded) {
                try {
                    triggeringSubscriberStop.run();
                } catch (RuntimeException failure) {
                    if (firstFailure == null) {
                        firstFailure = failure;
                    } else {
                        firstFailure.addSuppressed(failure);
                    }
                }
            }
            if (event != null) {
                try {
                    failureReporter.accept(event);
                } catch (RuntimeException failure) {
                    if (firstFailure == null) {
                        firstFailure = failure;
                    } else {
                        firstFailure.addSuppressed(failure);
                    }
                }
            }
            if (firstFailure != null) {
                throw firstFailure;
            }
        }
    }

    private static final class SubscriberState {

        private final Runnable stopAsync;
        private boolean paused;

        private SubscriberState(Runnable stopAsync) {
            this.stopAsync = stopAsync;
        }
    }
}
