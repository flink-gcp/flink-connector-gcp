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

package io.github.flink.gcp.connector.pubsub.sink.writer;

import com.google.api.core.ApiFuture;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import io.github.flink.gcp.connector.base.retry.RetrySchedule;
import io.github.flink.gcp.connector.pubsub.sink.TopicCreateOptions;
import io.github.flink.gcp.connector.pubsub.sink.TopicDestination;
import io.github.flink.gcp.connector.testutils.TestSinkWriterMetricGroup;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Direct tests for {@link TopicRepairer}, driving it through a scripted {@link
 * TopicRepairer.RepairContext} instead of a whole writer: the fake's drains mutate the {@link
 * DestinationState} exactly as the writer's mail path would — re-parking messages, re-marking the
 * topic missing, registering dropped keys — so each behavior is pinned where it lives.
 */
class TopicRepairerTest {

    private static final String PROJECT = "test-project";
    private static final TopicDestination TOPIC = TopicDestination.of(PROJECT, "repair-topic");
    private static final TopicCreateOptions CREATE_OPTIONS =
            TopicCreateOptions.builder().messageRetention(Duration.ofDays(7)).build();

    private final FakeTopicAdmin admin = new FakeTopicAdmin();
    private final PubSubWriterMetrics metrics =
            new PubSubWriterMetrics(TestSinkWriterMetricGroup.create(), false);
    private final Logger log = LoggerFactory.getLogger(TopicRepairerTest.class);

    /** One ordered log of everything the repair touches, shared by the context and publisher. */
    private final List<String> events = new ArrayList<>();

    private final FakeRepairContext context = new FakeRepairContext();

    /** A fast schedule keeping repair backoffs out of the test wall clock. */
    private static RetrySchedule schedule(int maxAttempts) {
        return new RetrySchedule(1, 1, maxAttempts, 0);
    }

    private TopicRepairer newRepairer(int maxAttempts, boolean orderingEnabled) {
        return new TopicRepairer(
                admin,
                CREATE_OPTIONS,
                schedule(maxAttempts),
                metrics,
                orderingEnabled,
                log,
                context);
    }

    private DestinationState newState() {
        return new DestinationState(TOPIC, new RecordingPublisher(events), metrics.forTopic(TOPIC));
    }

    private static PubsubMessage message(String payload) {
        return PubsubMessage.newBuilder().setData(ByteString.copyFromUtf8(payload)).build();
    }

    private static PubsubMessage keyedMessage(String payload, String orderingKey) {
        return message(payload).toBuilder().setOrderingKey(orderingKey).build();
    }

    @Test
    void createsTheTopicAtMostOncePerRepair() throws Exception {
        DestinationState state = newState();
        PubsubMessage first = message("m1");
        state.pendingRetries.put(1L, first);
        state.pendingRetries.put(2L, message("m2"));
        state.topicMissing = true;
        // The republish meets the topic still missing: the drain re-parks one message and
        // re-marks the topic, as the writer's NOT_FOUND failure mail would.
        context.onDrain(
                1,
                () -> {
                    state.pendingRetries.put(1L, first);
                    state.topicMissing = true;
                });

        newRepairer(5, false).repair(state);

        assertThat(admin.created).containsExactly(TOPIC);
        assertThat(admin.createOptions).containsExactly(CREATE_OPTIONS);
        assertThat(context.republishedPayloads()).containsExactly("m1", "m2", "m1");
        assertThat(context.soloVerdicts).containsOnly(false);
        assertThat(state.topicMissing).isFalse();
    }

    @Test
    void createsNoTopicWhenNothingMarkedItMissing() throws Exception {
        DestinationState state = newState();
        state.pendingRetries.put(1L, message("cascade"));

        newRepairer(5, false).repair(state);

        assertThat(admin.created).isEmpty();
        assertThat(context.republishedPayloads()).containsExactly("cascade");
        assertThat(context.soloVerdicts).containsExactly(false);
    }

    @Test
    void batchAttemptsReleaseTheWholeBatchAndDrainOnce() throws Exception {
        DestinationState state = newState();
        state.pendingRetries.put(1L, message("m1"));
        state.pendingRetries.put(2L, message("m2"));

        newRepairer(5, false).repair(state);

        assertThat(context.releases).containsExactly(2);
        assertThat(events)
                .containsExactly(
                        "release:2", "republish:m1:batch", "republish:m2:batch", "flush", "drain");
    }

    @Test
    void exhaustionWithoutCreationSaysKeptFailing() {
        DestinationState state = newState();
        PubsubMessage stuck = message("stuck");
        state.pendingRetries.put(1L, stuck);
        IOException cause = new IOException("parked for a cascade");
        state.repairCause = cause;
        context.onEveryDrain(() -> state.pendingRetries.put(1L, stuck));

        assertThatThrownBy(() -> newRepairer(2, false).repair(state))
                .isInstanceOf(IOException.class)
                .hasMessage(
                        "Republishing to Pub/Sub topic " + TOPIC + " kept failing (2 attempt(s)).")
                .hasCause(cause);
        assertThat(admin.created).isEmpty();
    }

    @Test
    void exhaustionAfterCreationSaysAfterCreatingTheTopic() {
        DestinationState state = newState();
        PubsubMessage stuck = message("stuck");
        state.pendingRetries.put(1L, stuck);
        state.topicMissing = true;
        IOException cause = new IOException("NOT_FOUND");
        state.repairCause = cause;
        context.onEveryDrain(() -> state.pendingRetries.put(1L, stuck));

        assertThatThrownBy(() -> newRepairer(2, false).repair(state))
                .isInstanceOf(IOException.class)
                .hasMessage(
                        "Republishing to Pub/Sub topic "
                                + TOPIC
                                + " kept failing after creating the topic (2 attempt(s)).")
                .hasCause(cause);
        assertThat(admin.created).containsExactly(TOPIC);
    }

    @Test
    void exhaustionAfterRoutingReportsTheHandedMessages() {
        DestinationState state = newState();
        PubsubMessage root = message("root");
        PubsubMessage follower = message("follower");
        state.pendingRetries.put(1L, root);
        state.pendingRetries.put(2L, follower);
        // Parked by a batched request-level rejection, so the first attempt isolates — routing can
        // only happen from an isolation pass, since a solo verdict exists nowhere else.
        state.isolationNeeded = true;
        IOException cause = new IOException("INVALID_ARGUMENT");
        state.repairCause = cause;
        // The pass's first drain routes the poison root, as the writer's solo-verdict mail would;
        // the follower then keeps failing for a reason of its own and is re-parked by every later
        // drain, so the budget runs out with a drop to report.
        context.onDrain(1, () -> state.routedDuringRepair++);
        context.onDrain(2, () -> state.pendingRetries.put(2L, follower));
        context.onDrain(3, () -> state.pendingRetries.put(2L, follower));

        assertThatThrownBy(() -> newRepairer(2, false).repair(state))
                .isInstanceOf(IOException.class)
                .hasMessage(
                        "Republishing to Pub/Sub topic "
                                + TOPIC
                                + " could not drain its parked messages within the recovery budget"
                                + " (2 attempt(s)); 1 message(s) were handed to the failure handler"
                                + " during the repair.")
                .hasCause(cause);
        // The isolation pass republished both solo; the second attempt fell back to batched.
        assertThat(context.soloVerdicts).containsExactly(true, true, false);
    }

    @Test
    void clearsTheRepairCauseOnSuccess() throws Exception {
        DestinationState state = newState();
        state.pendingRetries.put(1L, message("m1"));
        state.repairCause = new IOException("NOT_FOUND");

        newRepairer(5, false).repair(state);

        assertThat(state.repairCause).isNull();
    }

    @Test
    void resumesOrderingKeysBeforeRepublishingOnEveryAttempt() throws Exception {
        DestinationState state = newState();
        PubsubMessage first = keyedMessage("m1", "a");
        state.pendingRetries.put(1L, first);
        state.pendingRetries.put(2L, keyedMessage("m2", "b"));
        // A key a dropped message left paused, with nothing parked for it.
        state.keysToResume.add("dropped");
        context.onDrain(1, () -> state.pendingRetries.put(1L, first));

        newRepairer(5, true).repair(state);

        assertThat(events)
                .containsExactly(
                        "resume:dropped",
                        "resume:a",
                        "resume:b",
                        "release:2",
                        "republish:m1:batch",
                        "republish:m2:batch",
                        "flush",
                        "drain",
                        "resume:a",
                        "release:1",
                        "republish:m1:batch",
                        "flush",
                        "drain");
        assertThat(state.keysToResume).isEmpty();
    }

    @Test
    void theIsolationPassPublishesSoloAndDrainsOncePerMessage() throws Exception {
        DestinationState state = newState();
        state.pendingRetries.put(1L, message("m1"));
        state.pendingRetries.put(2L, message("m2"));
        state.pendingRetries.put(3L, message("m3"));
        state.isolationNeeded = true;
        // The second solo verdict is a drop whose key must be handed back before the pass's next
        // republish, as routeFailedMessage registers it.
        context.onDrain(2, () -> state.keysToResume.add("k"));

        newRepairer(5, true).repair(state);

        assertThat(context.releases).containsExactly(1, 1, 1);
        assertThat(context.soloVerdicts).containsExactly(true, true, true);
        assertThat(events)
                .containsExactly(
                        "release:1",
                        "republish:m1:solo",
                        "flush",
                        "drain",
                        "release:1",
                        "republish:m2:solo",
                        "flush",
                        "drain",
                        "resume:k",
                        "release:1",
                        "republish:m3:solo",
                        "flush",
                        "drain");
        assertThat(state.isolationNeeded).isFalse();
    }

    /**
     * A scripted {@link TopicRepairer.RepairContext} recording republishes with their solo
     * verdicts, drains and releases into {@link #events}; a drain can be scripted to mutate the
     * destination state the way the writer's mail path would.
     */
    private final class FakeRepairContext implements TopicRepairer.RepairContext {

        final List<Boolean> soloVerdicts = new ArrayList<>();
        final List<PubsubMessage> republished = new ArrayList<>();
        final List<Integer> releases = new ArrayList<>();
        private final Map<Integer, Runnable> drainScripts = new HashMap<>();
        private Runnable everyDrainScript;
        private int drains;

        void onDrain(int drainNumber, Runnable script) {
            drainScripts.put(drainNumber, script);
        }

        void onEveryDrain(Runnable script) {
            everyDrainScript = script;
        }

        @Override
        public void republish(DestinationState state, PubsubMessage message, boolean soloVerdict) {
            republished.add(message);
            soloVerdicts.add(soloVerdict);
            events.add(
                    "republish:"
                            + message.getData().toStringUtf8()
                            + (soloVerdict ? ":solo" : ":batch"));
        }

        @Override
        public void drainInFlight() {
            drains++;
            events.add("drain");
            Runnable script = drainScripts.remove(drains);
            if (script != null) {
                script.run();
            }
            if (everyDrainScript != null) {
                everyDrainScript.run();
            }
        }

        @Override
        public void releaseParked(int count) {
            releases.add(count);
            events.add("release:" + count);
        }

        List<String> republishedPayloads() {
            List<String> payloads = new ArrayList<>();
            for (PubsubMessage message : republished) {
                payloads.add(message.getData().toStringUtf8());
            }
            return payloads;
        }
    }

    /**
     * A publisher recording its resumes and flushes into the shared {@link #events} log, so their
     * order relative to the context's republishes is assertable. The repairer never publishes
     * directly — only through the context — so {@link #publish} rejects.
     */
    private static final class RecordingPublisher implements TopicPublisher {

        private final List<String> events;

        RecordingPublisher(List<String> events) {
            this.events = events;
        }

        @Override
        public ApiFuture<String> publish(PubsubMessage message) {
            throw new UnsupportedOperationException(
                    "The repairer republishes only through its RepairContext.");
        }

        @Override
        public void resumePublish(String orderingKey) {
            events.add("resume:" + orderingKey);
        }

        @Override
        public void flushOutstanding() {
            events.add("flush");
        }

        @Override
        public void shutdown() {}

        @Override
        public void close() {}
    }
}
