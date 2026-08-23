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

package io.github.flink.gcp.connector.bigtable.source.changestream.reader;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.metrics.groups.SourceReaderMetricGroup;
import org.apache.flink.metrics.testutils.MetricListener;
import org.apache.flink.runtime.metrics.groups.InternalSourceReaderMetricGroup;
import org.apache.flink.util.Collector;

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.rpc.ResponseObserver;
import com.google.api.gax.rpc.StreamController;
import com.google.cloud.bigtable.data.v2.models.ChangeStreamRecord;
import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import io.github.flink.gcp.connector.base.source.StartPosition;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.source.changestream.BigtableChangeStreamMutation;
import io.github.flink.gcp.connector.bigtable.source.changestream.ChangeStreamPartitionSplit;
import io.github.flink.gcp.connector.bigtable.source.changestream.PartitionTransitionEvent;
import io.github.flink.gcp.connector.bigtable.source.changestream.ReaderCapacityEvent;
import io.github.flink.gcp.connector.bigtable.source.serializer.BigtableChangeStreamDeserializationSchema;
import io.github.flink.gcp.connector.testutils.CollectingReaderOutput;
import io.github.flink.gcp.connector.testutils.FakeSourceReaderContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CancellationException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BigtableChangeStreamReaderTest {

    private final MetricListener listener = new MetricListener();
    private final SourceReaderMetricGroup metricGroup =
            InternalSourceReaderMetricGroup.mock(listener.getMetricGroup());
    private final FakeSourceReaderContext context = new FakeSourceReaderContext(metricGroup);
    private final ScriptedOpener opener = new ScriptedOpener();
    @Nullable private BigtableChangeStreamReader<String> reader;

    @AfterEach
    void closeReader() throws Exception {
        if (reader != null) {
            reader.close();
        }
    }

    @Test
    void boundsActiveReadsAndAdvertisesAbsoluteCapacity() {
        reader = reader(2);
        reader.start();

        assertThat(capacities()).containsExactly(2);

        reader.addSplits(Arrays.asList(split("first"), split("second"), split("queued")));

        assertThat(opener.openedSplitIds()).containsExactly("first", "second");
        assertThat(opener.controllers())
                .allSatisfy(controller -> assertThat(controller.requests).isEqualTo(1));
        assertThat(counter("changeStreamReadsStarted")).isEqualTo(2);
        assertThat(gauge("activeChangeStreamReads")).isEqualTo(2);
        assertThat(gauge("queuedChangeStreamPartitions")).isEqualTo(1);
        assertThat(capacities()).containsExactly(2, 0);
        assertThat(reader.snapshotState(1L))
                .extracting(ChangeStreamPartitionSplit::splitId)
                .containsExactly("first", "second", "queued");
    }

    @Test
    void checkpointsOnlyTaskThreadEmittedProgress() throws Exception {
        Instant watermark = Instant.parse("2026-08-13T01:00:00Z");
        reader = reader(1);
        reader.addSplits(Collections.singletonList(split("partition")));
        reader.start();

        opener.deliver(
                0,
                TestChangeStreamRecords.mutation(
                        watermark.plusSeconds(1), watermark, "delivered-token"));

        assertThat(reader.snapshotState(1L).get(0).getContinuationTokens()).isEmpty();

        CollectingReaderOutput<String> output = new CollectingReaderOutput<>();
        assertThat(reader.pollNext(output)).isEqualTo(InputStatus.NOTHING_AVAILABLE);

        assertThat(output.records()).containsExactly("row");
        assertThat(reader.snapshotState(2L).get(0).getContinuationTokens())
                .singleElement()
                .extracting(token -> token.getToken())
                .isEqualTo("delivered-token");
        assertThat(gauge("partitionLowWatermarkMillis")).isEqualTo(watermark.toEpochMilli());
        assertThat(opener.controllers().get(0).requests).isEqualTo(2);
    }

    @Test
    void rotatesPartitionsAtHeartbeatsInFifoOrder() throws Exception {
        reader = reader(1);
        reader.addSplits(Arrays.asList(split("first"), split("second")));
        reader.start();
        CollectingReaderOutput<String> output = new CollectingReaderOutput<>();

        opener.deliver(
                0,
                TestChangeStreamRecords.heartbeat(
                        Instant.parse("2026-08-13T01:00:00Z"), "first-heartbeat"));
        reader.pollNext(output);
        assertThat(opener.controllers().get(0).cancelCalls).isEqualTo(1);
        reader.pollNext(output);

        assertThat(opener.openedSplitIds()).containsExactly("first", "second");

        opener.deliver(
                1,
                TestChangeStreamRecords.heartbeat(
                        Instant.parse("2026-08-13T01:01:00Z"), "second-heartbeat"));
        reader.pollNext(output);
        reader.pollNext(output);

        assertThat(opener.openedSplitIds()).containsExactly("first", "second", "first");
        assertThat(opener.opened.get(2).getContinuationTokens())
                .singleElement()
                .extracting(token -> token.getToken())
                .isEqualTo("first-heartbeat");
    }

    @Test
    void drainsConcurrentHeartbeatsBeforeReusingTheirHandoverCapacity() throws Exception {
        reader = reader(2);
        reader.addSplits(Arrays.asList(split("first"), split("second"), split("third")));
        reader.start();
        CollectingReaderOutput<String> output = new CollectingReaderOutput<>();

        opener.deliver(
                0,
                TestChangeStreamRecords.heartbeat(
                        Instant.parse("2026-08-13T01:00:00Z"), "first-heartbeat"));
        opener.deliver(
                1,
                TestChangeStreamRecords.heartbeat(
                        Instant.parse("2026-08-13T01:00:01Z"), "second-heartbeat"));

        for (int poll = 0; poll < 4; poll++) {
            reader.pollNext(output);
        }

        assertThat(opener.openedSplitIds()).containsExactly("first", "second", "third", "first");
        assertThat(opener.controllers().get(0).cancelCalls).isEqualTo(1);
        assertThat(opener.controllers().get(1).cancelCalls).isEqualTo(1);
    }

    @Test
    void requestsAnotherResponseForEveryConcurrentHeartbeatWithoutAQueue() throws Exception {
        reader = reader(2);
        reader.addSplits(Arrays.asList(split("first"), split("second")));
        reader.start();
        CollectingReaderOutput<String> output = new CollectingReaderOutput<>();

        opener.deliver(
                0,
                TestChangeStreamRecords.heartbeat(
                        Instant.parse("2026-08-13T01:00:00Z"), "first-heartbeat"));
        opener.deliver(
                1,
                TestChangeStreamRecords.heartbeat(
                        Instant.parse("2026-08-13T01:00:01Z"), "second-heartbeat"));

        reader.pollNext(output);
        reader.pollNext(output);

        assertThat(opener.controllers())
                .allSatisfy(
                        controller -> {
                            assertThat(controller.requests).isEqualTo(2);
                            assertThat(controller.pendingRequests).isEqualTo(1);
                        });
    }

    @Test
    void boundedReaderEndsOnlyAfterTheCancelledRpcTerminates() throws Exception {
        opener.completeCancellation = false;
        reader = reader(1);
        reader.addSplits(Collections.singletonList(split("closing")));
        reader.start();
        reader.notifyNoMoreSplits();
        CollectingReaderOutput<String> output = new CollectingReaderOutput<>();

        opener.deliver(0, TestChangeStreamRecords.close("successor"));

        assertThat(reader.pollNext(output)).isEqualTo(InputStatus.NOTHING_AVAILABLE);
        assertThat(reader.pollNext(output)).isEqualTo(InputStatus.NOTHING_AVAILABLE);

        opener.complete(0);

        assertThat(reader.pollNext(output)).isEqualTo(InputStatus.END_OF_INPUT);
    }

    @Test
    void acceptsNormalCompletionAfterADeliveredCloseStream() throws Exception {
        reader = reader(1);
        reader.addSplits(Collections.singletonList(split("closing")));
        reader.start();
        reader.notifyNoMoreSplits();
        opener.deliver(0, TestChangeStreamRecords.close("successor"));
        opener.complete(0);

        CollectingReaderOutput<String> output = new CollectingReaderOutput<>();
        assertThat(reader.pollNext(output)).isEqualTo(InputStatus.MORE_AVAILABLE);
        assertThat(reader.pollNext(output)).isEqualTo(InputStatus.END_OF_INPUT);
        assertThat(context.sourceEvents()).anyMatch(PartitionTransitionEvent.class::isInstance);
    }

    @Test
    void doesNotFinishAPartitionWhenRpcFailsAfterDeliveringCloseStream() throws Exception {
        reader = reader(1);
        reader.addSplits(Collections.singletonList(split("closing")));
        reader.start();
        opener.deliver(0, TestChangeStreamRecords.close("successor"));
        opener.fail(0, new IllegalStateException("permission denied"));

        CollectingReaderOutput<String> output = new CollectingReaderOutput<>();
        assertThat(reader.pollNext(output)).isEqualTo(InputStatus.MORE_AVAILABLE);
        assertThatThrownBy(() -> reader.pollNext(output))
                .isInstanceOf(IOException.class)
                .hasRootCauseMessage("permission denied");
        assertThat(context.sourceEvents()).noneMatch(PartitionTransitionEvent.class::isInstance);
        assertThat(reader.snapshotState(1L))
                .extracting(ChangeStreamPartitionSplit::splitId)
                .containsExactly("closing");
    }

    @Test
    void retainsFailedSplitStateForRecovery() {
        reader = reader(1);
        reader.addSplits(Collections.singletonList(split("failed")));
        reader.start();
        opener.fail(0, new IllegalStateException("scripted failure"));

        CollectingReaderOutput<String> output = new CollectingReaderOutput<>();
        assertThatThrownBy(() -> reader.pollNext(output))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("failed")
                .hasRootCauseMessage("scripted failure");
        assertThat(reader.snapshotState(1L))
                .extracting(ChangeStreamPartitionSplit::splitId)
                .containsExactly("failed");
        assertThat(gauge("activeChangeStreamReads")).isZero();
    }

    @Test
    void failsUnexpectedRpcCompletionWithoutLosingTheSplit() {
        reader = reader(1);
        reader.addSplits(Collections.singletonList(split("ended")));
        reader.start();
        opener.complete(0);

        CollectingReaderOutput<String> output = new CollectingReaderOutput<>();
        assertThatThrownBy(() -> reader.pollNext(output))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("ended without a CloseStream")
                .hasMessageContaining("ended");
        assertThat(reader.snapshotState(1L))
                .extracting(ChangeStreamPartitionSplit::splitId)
                .containsExactly("ended");
        assertThat(gauge("activeChangeStreamReads")).isZero();
    }

    @Test
    void doesNotClassifyAnRpcFailureAfterAHeartbeatAsRotation() throws Exception {
        reader = reader(1);
        reader.addSplits(Arrays.asList(split("failed"), split("queued")));
        reader.start();
        opener.deliver(
                0,
                TestChangeStreamRecords.heartbeat(
                        Instant.parse("2026-08-13T01:00:00Z"), "heartbeat"));
        opener.fail(0, new IllegalStateException("permission denied"));

        reader.pollNext(new CollectingReaderOutput<>());

        assertThatThrownBy(() -> reader.pollNext(new CollectingReaderOutput<>()))
                .isInstanceOf(IOException.class)
                .hasRootCauseMessage("permission denied");
        assertThat(opener.controllers().get(0).cancelCalls).isZero();
        assertThat(reader.snapshotState(1L))
                .extracting(ChangeStreamPartitionSplit::splitId)
                .containsExactly("failed", "queued");
    }

    @Test
    void doesNotSuppressAnRpcFailureThatRacesWithHeartbeatCancellation() throws Exception {
        opener.completeCancellation = false;
        reader = reader(1);
        reader.addSplits(Arrays.asList(split("failed"), split("queued")));
        reader.start();
        opener.deliver(
                0,
                TestChangeStreamRecords.heartbeat(
                        Instant.parse("2026-08-13T01:00:00Z"), "heartbeat"));

        reader.pollNext(new CollectingReaderOutput<>());
        assertThat(opener.controllers().get(0).cancelCalls).isOne();
        opener.fail(0, new IllegalStateException("permission denied"));

        assertThatThrownBy(() -> reader.pollNext(new CollectingReaderOutput<>()))
                .isInstanceOf(IOException.class)
                .hasRootCauseMessage("permission denied");
        assertThat(reader.snapshotState(1L))
                .extracting(ChangeStreamPartitionSplit::splitId)
                .containsExactly("failed", "queued");
    }

    @Test
    void failsAnUnrequestedResponseAfterDrainingTheRequestedOne() throws Exception {
        reader = reader(1);
        reader.addSplits(Collections.singletonList(split("partition")));
        reader.start();
        opener.deliver(
                0,
                TestChangeStreamRecords.mutation(
                        Instant.parse("2026-08-13T01:00:01Z"),
                        Instant.parse("2026-08-13T01:00:00Z"),
                        "requested"));
        opener.deliverUnrequested(
                0,
                TestChangeStreamRecords.mutation(
                        Instant.parse("2026-08-13T01:00:02Z"),
                        Instant.parse("2026-08-13T01:00:01Z"),
                        "unrequested"));

        CollectingReaderOutput<String> output = new CollectingReaderOutput<>();
        reader.pollNext(output);

        assertThatThrownBy(() -> reader.pollNext(output))
                .isInstanceOf(IOException.class)
                .hasRootCauseInstanceOf(IllegalStateException.class)
                .hasRootCauseMessage(
                        "Bigtable Change Streams delivered an unrequested response for"
                                + " partition.");
        assertThat(output.records()).containsExactly("row");
    }

    @Test
    void replacesACompletedAvailabilityFutureOnlyAfterTheDeliveryIsDrained() throws Exception {
        reader = reader(1);
        reader.addSplits(Collections.singletonList(split("partition")));
        reader.start();
        java.util.concurrent.CompletableFuture<Void> first = reader.isAvailable();

        opener.deliver(
                0,
                TestChangeStreamRecords.mutation(
                        Instant.parse("2026-08-13T01:00:01Z"),
                        Instant.parse("2026-08-13T01:00:00Z"),
                        "available"));

        assertThat(first).isCompleted();
        assertThat(reader.isAvailable()).isCompleted();
        reader.pollNext(new CollectingReaderOutput<>());
        java.util.concurrent.CompletableFuture<Void> next = reader.isAvailable();

        assertThat(next).isNotSameAs(first).isNotDone();
    }

    @Test
    void doesNotReusePhysicalSlotUntilCancelledRpcTerminates() throws Exception {
        opener.completeCancellation = false;
        reader = reader(1);
        reader.addSplits(Arrays.asList(split("closing"), split("next")));
        reader.start();

        opener.deliver(0, TestChangeStreamRecords.close("successor"));
        reader.pollNext(new CollectingReaderOutput<>());

        assertThat(opener.openedSplitIds()).containsExactly("closing");
        assertThat(reader.snapshotState(1L))
                .extracting(ChangeStreamPartitionSplit::splitId)
                .containsExactly("next");

        opener.complete(0);
        reader.pollNext(new CollectingReaderOutput<>());

        assertThat(opener.openedSplitIds()).containsExactly("closing", "next");
    }

    @Test
    void cancelsAReadWhoseOpenFailsAfterInstallingItsController() {
        opener.throwAfterStart = true;
        reader = reader(1);
        reader.addSplits(Collections.singletonList(split("opening")));

        assertThatThrownBy(reader::start)
                .isInstanceOf(org.apache.flink.util.FlinkRuntimeException.class)
                .hasRootCauseMessage("scripted open failure");

        assertThat(opener.controllers().get(0).cancelCalls).isEqualTo(1);
        assertThat(reader.snapshotState(1L))
                .extracting(ChangeStreamPartitionSplit::splitId)
                .containsExactly("opening");
    }

    @Test
    void ignoresAResponseThatRacesWithReaderClose() throws Exception {
        opener.completeCancellation = false;
        reader = reader(1);
        reader.addSplits(Collections.singletonList(split("closing")));
        reader.start();

        reader.close();
        reader = null;
        opener.deliver(
                0,
                TestChangeStreamRecords.mutation(
                        Instant.parse("2026-08-13T01:00:01Z"),
                        Instant.parse("2026-08-13T01:00:00Z"),
                        "late"));

        assertThat(opener.closeCalls).isEqualTo(1);
    }

    @Test
    void cancelsAControllerThatStartsAfterReaderClose() throws Exception {
        opener.delayStart = true;
        reader = reader(1);
        reader.addSplits(Collections.singletonList(split("closing")));
        reader.start();

        reader.close();
        reader = null;
        opener.start(0);

        assertThat(opener.controllers().get(0).cancelCalls).isEqualTo(1);
        assertThat(opener.controllers().get(0).requests).isZero();
    }

    @Test
    void handsTheConfiguredResumeFallbackToTheRestoreResolver() {
        // The resolver is the only thing that sees the configured fallback, and the reader's own
        // behaviour is identical whichever fallback it holds, so this asserts on the argument
        // itself: with nothing recording it, passing null instead would look the same.
        List<StartPosition> seen = new ArrayList<>();
        StartPosition fallback = StartPosition.at(Instant.parse("2026-08-13T00:00:00Z"));
        reader =
                new BigtableChangeStreamReader<>(
                        context,
                        TableDestination.of("project", "instance", "table"),
                        schema(),
                        opener,
                        (split, configured) -> {
                            seen.add(configured);
                            return split;
                        },
                        fallback,
                        null,
                        1,
                        new BigtableChangeStreamReaderMetrics(metricGroup));

        reader.addSplits(Collections.singletonList(split("restored")));

        assertThat(seen).containsExactly(fallback);
    }

    private BigtableChangeStreamReader<String> reader(int maximumStreams) {
        return new BigtableChangeStreamReader<>(
                context,
                TableDestination.of("project", "instance", "table"),
                schema(),
                opener,
                (split, ignored) -> split,
                null,
                null,
                maximumStreams,
                new BigtableChangeStreamReaderMetrics(metricGroup));
    }

    private List<Integer> capacities() {
        List<Integer> capacities = new ArrayList<>();
        context.sourceEvents().stream()
                .filter(ReaderCapacityEvent.class::isInstance)
                .map(ReaderCapacityEvent.class::cast)
                .map(ReaderCapacityEvent::getFreeSlots)
                .forEach(capacities::add);
        return capacities;
    }

    private long counter(String name) {
        return listener.getCounter(name).orElseThrow(AssertionError::new).getCount();
    }

    private long gauge(String name) {
        Object value = listener.getGauge(name).orElseThrow(AssertionError::new).getValue();
        return ((Number) value).longValue();
    }

    private static ChangeStreamPartitionSplit split(String id) {
        return new ChangeStreamPartitionSplit(
                id, ByteStringRange.create("a", "z"), Collections.emptyList(), Instant.EPOCH);
    }

    private static BigtableChangeStreamDeserializationSchema<String> schema() {
        return new BigtableChangeStreamDeserializationSchema<String>() {
            @Override
            public void deserialize(BigtableChangeStreamMutation mutation, Collector<String> out) {
                out.collect(mutation.getRowKey().toStringUtf8());
            }

            @Override
            public TypeInformation<String> getProducedType() {
                return TypeInformation.of(String.class);
            }
        };
    }

    private static final class ScriptedOpener implements ChangeStreamOpener {
        private static final long serialVersionUID = 1L;
        private final List<ChangeStreamPartitionSplit> opened = new ArrayList<>();
        private final List<ResponseObserver<ChangeStreamRecord>> observers = new ArrayList<>();
        private final List<ScriptedController> controllers = new ArrayList<>();
        private boolean completeCancellation = true;
        private boolean throwAfterStart;
        private boolean delayStart;
        private int closeCalls;

        @Override
        public void open(
                TableDestination table,
                ChangeStreamPartitionSplit split,
                @Nullable Instant boundedTimestamp,
                ResponseObserver<ChangeStreamRecord> observer) {
            ScriptedController controller = new ScriptedController(observer, this);
            opened.add(split);
            observers.add(observer);
            controllers.add(controller);
            if (!delayStart) {
                observer.onStart(controller);
            }
            if (throwAfterStart) {
                throw new IllegalStateException("scripted open failure");
            }
        }

        private void start(int index) {
            observers.get(index).onStart(controllers.get(index));
        }

        private List<String> openedSplitIds() {
            List<String> ids = new ArrayList<>();
            for (ChangeStreamPartitionSplit split : opened) {
                ids.add(split.splitId());
            }
            return ids;
        }

        private List<ScriptedController> controllers() {
            return controllers;
        }

        private void deliver(int index, ChangeStreamRecord record) {
            ScriptedController controller = controllers.get(index);
            assertThat(controller.pendingRequests).isPositive();
            controller.pendingRequests--;
            observers.get(index).onResponse(record);
        }

        private void deliverUnrequested(int index, ChangeStreamRecord record) {
            observers.get(index).onResponse(record);
        }

        private void fail(int index, Throwable error) {
            observers.get(index).onError(error);
        }

        private void complete(int index) {
            observers.get(index).onComplete();
        }

        /** Answers from a script rather than a client, so there is nothing to authenticate. */
        @Override
        public void useCredentials(@Nullable CredentialsProvider credentials) {}

        @Override
        public void close() {
            closeCalls++;
        }
    }

    private static final class ScriptedController implements StreamController {
        private final ResponseObserver<ChangeStreamRecord> observer;
        private final ScriptedOpener owner;
        private int requests;
        private int pendingRequests;
        private int cancelCalls;
        private boolean autoFlowDisabled;

        private ScriptedController(
                ResponseObserver<ChangeStreamRecord> observer, ScriptedOpener owner) {
            this.observer = observer;
            this.owner = owner;
        }

        @Override
        public void cancel() {
            cancelCalls++;
            if (owner.completeCancellation) {
                observer.onError(new CancellationException("scripted cancellation"));
            }
        }

        @Override
        public void disableAutoInboundFlowControl() {
            autoFlowDisabled = true;
        }

        @Override
        public void request(int count) {
            assertThat(autoFlowDisabled).isTrue();
            requests += count;
            pendingRequests += count;
        }
    }
}
