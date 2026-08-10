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

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * What this enumerator adds to the shared protocol: the read session.
 *
 * <p>The assignment protocol itself — parking, serving, no-more-splits, a returned split — is
 * {@code PullAssignmentSplitEnumeratorTest}'s, in {@code flink-connector-gcp-base}. The cases here
 * are the ones that would still pass there and be wrong here: a second session, a session that is
 * not closed, a failure reported without naming the table, a metric registered under the wrong
 * name.
 */
class BigQueryReadSplitEnumeratorTest {

    @Test
    void createsTheSessionExactlyOnceOnAFreshStart() throws Exception {
        ScriptedReadSessionCreator creator = ScriptedReadSessionCreator.withStreams(2);
        FakeSplitEnumeratorContext<BigQueryReadStreamSplit> context =
                new FakeSplitEnumeratorContext<>(2);

        try (BigQueryReadSplitEnumerator enumerator = enumerator(context, creator, null)) {
            enumerator.start();
            context.runAsyncCalls();

            assertThat(creator.creations()).isEqualTo(1);
            assertThat(context.counter("readSessionsCreated")).isEqualTo(1);
            assertThat(enumerator.snapshotState(1L).getPendingSplits()).hasSize(2);
        }
    }

    @Test
    void createsNoSessionWhenItRestoresAnInitializedState() throws Exception {
        // The guard the whole design rests on: a second session would pin a second snapshot of the
        // table, so a failed-over job would read it as of two different instants.
        ScriptedReadSessionCreator creator = ScriptedReadSessionCreator.withStreams(2);
        FakeSplitEnumeratorContext<BigQueryReadStreamSplit> context =
                new FakeSplitEnumeratorContext<>(2);
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
            assertThat(enumerator.snapshotState(1L).getPendingSplits()).hasSize(1);
        }
    }

    @Test
    void checkpointsTheSessionItCreated() throws Exception {
        ScriptedReadSessionCreator creator = ScriptedReadSessionCreator.withStreams(2);
        FakeSplitEnumeratorContext<BigQueryReadStreamSplit> context =
                new FakeSplitEnumeratorContext<>(2);

        try (BigQueryReadSplitEnumerator enumerator = enumerator(context, creator, null)) {
            enumerator.start();
            BigQueryReadEnumeratorState beforeCreation = enumerator.snapshotState(1L);

            assertThat(beforeCreation.isInitialized()).isFalse();
            assertThat(beforeCreation.getSessionName()).isNull();
            assertThat(beforeCreation.getPendingSplits()).isEmpty();

            context.runAsyncCalls();
            BigQueryReadEnumeratorState afterCreation = enumerator.snapshotState(2L);

            assertThat(afterCreation.isInitialized()).isTrue();
            assertThat(afterCreation.getSessionName())
                    .isEqualTo(ScriptedReadSessionCreator.SESSION);
            assertThat(afterCreation.getSessionExpireTime()).isNotNull();
        }
    }

    @Test
    void failsTheJobWhenTheSessionCannotBeCreated() throws Exception {
        ScriptedReadSessionCreator creator =
                ScriptedReadSessionCreator.failing(new IllegalStateException("denied"));
        FakeSplitEnumeratorContext<BigQueryReadStreamSplit> context =
                new FakeSplitEnumeratorContext<>(1);

        try (BigQueryReadSplitEnumerator enumerator = enumerator(context, creator, null)) {
            enumerator.start();

            assertThatThrownBy(context::runAsyncCalls)
                    .isInstanceOf(FlinkRuntimeException.class)
                    .hasMessageContaining("read session")
                    .hasMessageContaining(TestSources.config().getTable().toString())
                    .hasRootCauseMessage("denied");
        }
    }

    @Test
    void closesTheSessionCreatorItOwns() throws Exception {
        ScriptedReadSessionCreator creator = ScriptedReadSessionCreator.withStreams(2);
        FakeSplitEnumeratorContext<BigQueryReadStreamSplit> context =
                new FakeSplitEnumeratorContext<>(1);
        BigQueryReadSplitEnumerator enumerator = enumerator(context, creator, null);

        enumerator.start();
        enumerator.close();

        assertThat(creator.closes()).isEqualTo(1);
    }

    @Test
    void reportsWhatItAssignedReturnedAndCreated() throws Exception {
        FakeSplitEnumeratorContext<BigQueryReadStreamSplit> context =
                new FakeSplitEnumeratorContext<>(2);
        try (BigQueryReadSplitEnumerator enumerator = started(context, 3)) {
            context.registerReader(0);
            enumerator.handleSplitRequest(0, "localhost");

            assertThat(context.counter("splitsAssigned")).isEqualTo(1);
            assertThat(context.counter("readSessionsCreated")).isEqualTo(1);
            assertThat(context.<Long>gauge("unassignedSplits")).isEqualTo(2L);

            enumerator.addSplitsBack(context.assignedSplits(0), 0);

            assertThat(context.counter("splitsReturned")).isEqualTo(1);
            assertThat(context.<Long>gauge("unassignedSplits")).isEqualTo(3L);
        }
    }

    @Test
    void handsOutOneStreamPerRequest() throws Exception {
        // The session's streams reach the readers one at a time, and each carries two things off
        // the session that a reader is given no other way: the Avro schema, without which it
        // cannot decode a row, and the expiry, without which a failure past it is a bare stream
        // error. Both would fail in the reader rather than here.
        FakeSplitEnumeratorContext<BigQueryReadStreamSplit> context =
                new FakeSplitEnumeratorContext<>(2);
        try (BigQueryReadSplitEnumerator enumerator = started(context, 3)) {
            context.registerReader(0);

            enumerator.handleSplitRequest(0, "localhost");

            assertThat(context.assignedSplits(0))
                    .singleElement()
                    .satisfies(
                            split -> {
                                assertThat(split.splitId())
                                        .isEqualTo(ScriptedReadSessionCreator.streamName(0));
                                assertThat(split.getOffset()).isZero();
                                assertThat(split.getAvroSchemaJson())
                                        .isEqualTo(TestRows.SCHEMA_JSON);
                                assertThat(split.getSessionExpireTime())
                                        .isEqualTo(ScriptedReadSessionCreator.EXPIRE_TIME);
                            });
            assertThat(enumerator.snapshotState(1L).getPendingSplits()).hasSize(2);
        }
    }

    private static BigQueryReadStreamSplit split(int index, long offset) {
        return new BigQueryReadStreamSplit(
                ScriptedReadSessionCreator.streamName(index),
                offset,
                TestRows.SCHEMA_JSON,
                ScriptedReadSessionCreator.EXPIRE_TIME);
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
