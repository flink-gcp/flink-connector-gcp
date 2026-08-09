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

package io.github.flink.gcp.connector.bigquery.source.enumerator;

import org.apache.flink.util.FlinkRuntimeException;

import io.github.flink.gcp.connector.bigquery.source.TestRows;
import io.github.flink.gcp.connector.bigquery.source.TestSources;
import io.github.flink.gcp.connector.bigquery.source.split.BigQueryReadStreamSplit;
import io.github.flink.gcp.connector.testutils.FakeSplitEnumeratorContext;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The enumerator's assignment protocol.
 *
 * <p>The reference implementation this design was drawn from reports a data-loss bug in its
 * no-more-splits and reader-failure bookkeeping, so the cases here are the invariants that
 * bookkeeping would break, one test each — not the happy path with a few extras.
 */
class BigQueryReadSplitEnumeratorTest {

    @Test
    void createsTheSessionExactlyOnceOnAFreshStart() throws Exception {
        ScriptedReadSessionCreator creator = ScriptedReadSessionCreator.withStreams(2);
        FakeSplitEnumeratorContext<BigQueryReadStreamSplit> context =
                new FakeSplitEnumeratorContext<BigQueryReadStreamSplit>(2);

        try (BigQueryReadSplitEnumerator enumerator = enumerator(context, creator, null)) {
            enumerator.start();
            context.runAsyncCalls();

            assertThat(creator.creations()).isEqualTo(1);
            assertThat(context.counter("readSessionsCreated")).isEqualTo(1);
        }
    }

    @Test
    void createsNoSessionWhenItRestoresAnInitializedState() throws Exception {
        // The guard the whole design rests on: a second session would pin a second snapshot of the
        // table, so a failed-over job would read it as of two different instants.
        ScriptedReadSessionCreator creator = ScriptedReadSessionCreator.withStreams(2);
        FakeSplitEnumeratorContext<BigQueryReadStreamSplit> context =
                new FakeSplitEnumeratorContext<BigQueryReadStreamSplit>(2);
        BigQueryReadEnumeratorState restored =
                new BigQueryReadEnumeratorState(
                        true,
                        ScriptedReadSessionCreator.SESSION,
                        null,
                        Collections.singletonList(split(0, 7)));

        try (BigQueryReadSplitEnumerator enumerator = enumerator(context, creator, restored)) {
            enumerator.start();
            context.runAsyncCalls();

            assertThat(creator.creations()).isZero();
            assertThat(context.counter("readSessionsCreated")).isZero();
            assertThat(enumerator.pendingSplitCount()).isEqualTo(1);
        }
    }

    @Test
    void checkpointsNoSplitBeforeTheSessionExists() throws Exception {
        ScriptedReadSessionCreator creator = ScriptedReadSessionCreator.withStreams(2);
        FakeSplitEnumeratorContext<BigQueryReadStreamSplit> context =
                new FakeSplitEnumeratorContext<BigQueryReadStreamSplit>(2);

        try (BigQueryReadSplitEnumerator enumerator = enumerator(context, creator, null)) {
            enumerator.start();
            BigQueryReadEnumeratorState beforeCreation = enumerator.snapshotState(1L);

            assertThat(beforeCreation.isInitialized()).isFalse();
            assertThat(beforeCreation.getPendingSplits()).isEmpty();
            assertThat(context.events()).isEmpty();

            context.runAsyncCalls();
            assertThat(enumerator.snapshotState(2L).isInitialized()).isTrue();
        }
    }

    @Test
    void handsOutOneSplitPerRequest() throws Exception {
        FakeSplitEnumeratorContext<BigQueryReadStreamSplit> context =
                new FakeSplitEnumeratorContext<BigQueryReadStreamSplit>(2);
        try (BigQueryReadSplitEnumerator enumerator = started(context, 3)) {
            context.registerReader(0);

            enumerator.handleSplitRequest(0, "localhost");

            assertThat(context.assignedSplits(0)).hasSize(1);
            assertThat(enumerator.pendingSplitCount()).isEqualTo(2);
            assertThat(context.counter("splitsAssigned")).isEqualTo(1);
        }
    }

    @Test
    void servesTheRequestsThatArrivedBeforeTheSessionExisted() throws Exception {
        ScriptedReadSessionCreator creator = ScriptedReadSessionCreator.withStreams(2);
        FakeSplitEnumeratorContext<BigQueryReadStreamSplit> context =
                new FakeSplitEnumeratorContext<BigQueryReadStreamSplit>(2);

        try (BigQueryReadSplitEnumerator enumerator = enumerator(context, creator, null)) {
            enumerator.start();
            context.registerReader(0);
            context.registerReader(1);
            enumerator.handleSplitRequest(0, "localhost");
            enumerator.handleSplitRequest(1, "localhost");
            assertThat(context.events()).isEmpty();

            context.runAsyncCalls();

            assertThat(context.assignedSplits(0)).hasSize(1);
            assertThat(context.assignedSplits(1)).hasSize(1);
        }
    }

    @Test
    void skipsARequesterThatFailedWhileItWaited() throws Exception {
        ScriptedReadSessionCreator creator = ScriptedReadSessionCreator.withStreams(2);
        FakeSplitEnumeratorContext<BigQueryReadStreamSplit> context =
                new FakeSplitEnumeratorContext<BigQueryReadStreamSplit>(2);

        try (BigQueryReadSplitEnumerator enumerator = enumerator(context, creator, null)) {
            enumerator.start();
            context.registerReader(0);
            context.registerReader(1);
            enumerator.handleSplitRequest(0, "localhost");
            enumerator.handleSplitRequest(1, "localhost");
            context.unregisterReader(0);

            context.runAsyncCalls();

            assertThat(context.assignedSplits(0)).isEmpty();
            assertThat(context.assignedSplits(1)).hasSize(1);
        }
    }

    @Test
    void tellsARequesterThereAreNoMoreSplitsOnceTheQueueIsEmpty() throws Exception {
        FakeSplitEnumeratorContext<BigQueryReadStreamSplit> context =
                new FakeSplitEnumeratorContext<BigQueryReadStreamSplit>(2);
        try (BigQueryReadSplitEnumerator enumerator = started(context, 1)) {
            context.registerReader(0);
            context.registerReader(1);

            enumerator.handleSplitRequest(0, "localhost");
            enumerator.handleSplitRequest(1, "localhost");

            assertThat(context.readersToldNoMoreSplits()).containsExactly(1);
            assertThat(context.events()).containsExactly("assign:0", "noMoreSplits:1");
        }
    }

    @Test
    void servesAReaderThatWasAlreadyToldThereAreNoMoreSplits() throws Exception {
        // Nothing here records that a reader was told, which is the point: a returned split must be
        // assignable to whoever asks next. (Flink's coordinator does keep such a flag and clears it
        // when the subtask is reset — the reset that also returns the splits — so this enumerator's
        // job is only to not add a second, staler copy of the same fact.)
        FakeSplitEnumeratorContext<BigQueryReadStreamSplit> context =
                new FakeSplitEnumeratorContext<BigQueryReadStreamSplit>(2);
        try (BigQueryReadSplitEnumerator enumerator = started(context, 1)) {
            context.registerReader(0);
            context.registerReader(1);
            enumerator.handleSplitRequest(0, "localhost");
            enumerator.handleSplitRequest(1, "localhost");
            assertThat(context.readersToldNoMoreSplits()).containsExactly(1);

            enumerator.addSplitsBack(Collections.singletonList(split(0, 3)), 0);
            enumerator.handleSplitRequest(1, "localhost");

            assertThat(context.assignedSplits(1)).hasSize(1);
            assertThat(context.assignedSplits(1).get(0).getOffset()).isEqualTo(3);
            assertThat(context.events()).containsExactly("assign:0", "noMoreSplits:1", "assign:1");
        }
    }

    @Test
    void returnsSplitsToTheQueueAndAssignsNothingItself() throws Exception {
        FakeSplitEnumeratorContext<BigQueryReadStreamSplit> context =
                new FakeSplitEnumeratorContext<BigQueryReadStreamSplit>(2);
        try (BigQueryReadSplitEnumerator enumerator = started(context, 2)) {
            context.registerReader(0);
            enumerator.handleSplitRequest(0, "localhost");
            BigQueryReadStreamSplit assigned = context.assignedSplits(0).get(0);

            enumerator.addSplitsBack(Collections.singletonList(assigned), 0);

            assertThat(enumerator.pendingSplitCount()).isEqualTo(2);
            assertThat(context.events()).containsExactly("assign:0");
            assertThat(context.counter("splitsReturned")).isEqualTo(1);
        }
    }

    @Test
    void keepsEveryStreamInExactlyOnePlaceAcrossFailoverAndReassignment() throws Exception {
        FakeSplitEnumeratorContext<BigQueryReadStreamSplit> context =
                new FakeSplitEnumeratorContext<BigQueryReadStreamSplit>(2);
        try (BigQueryReadSplitEnumerator enumerator = started(context, 4)) {
            context.registerReader(0);
            context.registerReader(1);
            Set<String> streams = new HashSet<>();

            enumerator.handleSplitRequest(0, "localhost");
            enumerator.handleSplitRequest(1, "localhost");
            List<BigQueryReadStreamSplit> held = new ArrayList<>(context.assignedSplits(0));
            held.addAll(context.assignedSplits(1));
            assertThat(held).hasSize(2);
            assertThat(enumerator.pendingSplitCount()).isEqualTo(2);

            enumerator.addSplitsBack(context.assignedSplits(0), 0);
            assertThat(enumerator.pendingSplitCount()).isEqualTo(3);

            enumerator.handleSplitRequest(0, "localhost");
            enumerator.handleSplitRequest(0, "localhost");
            enumerator.handleSplitRequest(1, "localhost");
            enumerator.handleSplitRequest(1, "localhost");

            context.assignedSplits(0).forEach(split -> streams.add(split.splitId()));
            context.assignedSplits(1).forEach(split -> streams.add(split.splitId()));
            assertThat(streams).hasSize(4);
            assertThat(enumerator.pendingSplitCount()).isZero();
            // Five assignments for four streams — the returned one and no other was handed out
            // twice. The distinct set alone would hide a stream assigned to two readers at once.
            assertThat(context.events())
                    .filteredOn(event -> event.startsWith("assign:"))
                    .hasSize(5);
            assertThat(context.counter("splitsAssigned")).isEqualTo(5);
        }
    }

    @Test
    void failsTheJobWhenTheSessionCannotBeCreated() throws Exception {
        ScriptedReadSessionCreator creator =
                ScriptedReadSessionCreator.failing(new IllegalStateException("denied"));
        FakeSplitEnumeratorContext<BigQueryReadStreamSplit> context =
                new FakeSplitEnumeratorContext<BigQueryReadStreamSplit>(1);

        try (BigQueryReadSplitEnumerator enumerator = enumerator(context, creator, null)) {
            enumerator.start();

            assertThatThrownBy(context::runAsyncCalls)
                    .isInstanceOf(FlinkRuntimeException.class)
                    .hasMessageContaining("read session")
                    .hasRootCauseMessage("denied");
        }
    }

    @Test
    void staysQuietWhenTheSessionArrivesAfterItWasClosed() throws Exception {
        // Failing the job during our own teardown would turn a clean cancellation into a failure.
        ScriptedReadSessionCreator creator =
                ScriptedReadSessionCreator.failing(new IllegalStateException("denied"));
        FakeSplitEnumeratorContext<BigQueryReadStreamSplit> context =
                new FakeSplitEnumeratorContext<BigQueryReadStreamSplit>(1);
        BigQueryReadSplitEnumerator enumerator = enumerator(context, creator, null);
        enumerator.start();

        enumerator.close();
        context.runAsyncCalls();

        assertThat(creator.closes()).isEqualTo(1);
    }

    @Test
    void reportsTheUnassignedSplitsToFlinksOwnGauge() throws Exception {
        FakeSplitEnumeratorContext<BigQueryReadStreamSplit> context =
                new FakeSplitEnumeratorContext<BigQueryReadStreamSplit>(2);
        try (BigQueryReadSplitEnumerator enumerator = started(context, 3)) {
            context.registerReader(0);
            enumerator.handleSplitRequest(0, "localhost");

            assertThat(context.<Long>gauge("unassignedSplits")).isEqualTo(2L);
        }
    }

    private static BigQueryReadStreamSplit split(int index, long offset) {
        return new BigQueryReadStreamSplit(
                ScriptedReadSessionCreator.streamName(index), offset, TestRows.SCHEMA_JSON);
    }

    private static BigQueryReadSplitEnumerator enumerator(
            FakeSplitEnumeratorContext<BigQueryReadStreamSplit> context,
            ScriptedReadSessionCreator creator,
            BigQueryReadEnumeratorState restored) {
        return new BigQueryReadSplitEnumerator(context, TestSources.config(), creator, restored);
    }

    /** An enumerator whose session has been created, with the given number of streams. */
    private static BigQueryReadSplitEnumerator started(
            FakeSplitEnumeratorContext<BigQueryReadStreamSplit> context, int streamCount) {
        BigQueryReadSplitEnumerator enumerator =
                enumerator(context, ScriptedReadSessionCreator.withStreams(streamCount), null);
        enumerator.start();
        context.runAsyncCalls();
        return enumerator;
    }
}
