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

import io.github.flink.gcp.connector.bigquery.source.BigQuerySourceConfig;
import io.github.flink.gcp.connector.bigquery.source.TestRows;
import io.github.flink.gcp.connector.bigquery.source.TestSources;
import io.github.flink.gcp.connector.bigquery.source.query.ScriptedQueryRunner;
import io.github.flink.gcp.connector.bigquery.source.split.BigQueryReadStreamSplit;
import io.github.flink.gcp.connector.testutils.FakeSplitEnumeratorContext;
import org.junit.jupiter.api.Test;

import java.io.IOException;
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
                    .hasMessageContaining("plan the BigQuery read")
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

    @Test
    void runsTheQueryOnceAndCreatesTheSessionAgainstWhereItLanded() throws Exception {
        ScriptedReadSessionCreator creator = ScriptedReadSessionCreator.withStreams(1);
        ScriptedQueryRunner runner = ScriptedQueryRunner.answering(TestSources.QUERY_RESULT);
        FakeSplitEnumeratorContext<BigQueryReadStreamSplit> context =
                new FakeSplitEnumeratorContext<>(1);

        try (BigQueryReadSplitEnumerator enumerator = queryEnumerator(context, creator, runner)) {
            enumerator.start();
            context.runAsyncCalls();

            assertThat(runner.runs()).isEqualTo(1);
            assertThat(context.counter("queryJobsSubmitted")).isEqualTo(1);
            // The whole point of the query path: the session reads the result table, not the query.
            assertThat(creator.lastRequest())
                    .as("the session creator was handed a request")
                    .isNotNull();
            assertThat(creator.lastRequest().getReadSession().getTable())
                    .isEqualTo(TestSources.QUERY_RESULT.toTablePath());
        }
    }

    @Test
    void handsTheRunnerTheQueryAndWhereItsResultShouldGo() throws Exception {
        ScriptedQueryRunner runner = ScriptedQueryRunner.answering(TestSources.QUERY_RESULT);
        FakeSplitEnumeratorContext<BigQueryReadStreamSplit> context =
                new FakeSplitEnumeratorContext<>(1);
        BigQuerySourceConfig<?> config =
                TestSources.queryConfig(
                        builder ->
                                builder.queryLocation("asia-northeast1")
                                        .queryResultDataset("scratch"));

        try (BigQueryReadSplitEnumerator enumerator =
                new BigQueryReadSplitEnumerator(
                        context, config, ScriptedReadSessionCreator.withStreams(1), runner, null)) {
            enumerator.start();
            context.runAsyncCalls();

            assertThat(runner.lastSpec().getSql()).isEqualTo(TestSources.QUERY);
            assertThat(runner.lastSpec().getProject()).isEqualTo(TestSources.TABLE.getProject());
            assertThat(runner.lastSpec().getLocation()).isEqualTo("asia-northeast1");
            assertThat(runner.lastSpec().getResultDataset()).isEqualTo("scratch");
        }
    }

    @Test
    void runsNoQueryWhenItRestoresAnInitializedState() throws Exception {
        // The flag that stops a second read session stops a second query with it — and a second
        // query is the one that would be billed again, against a result the readers are not
        // reading.
        ScriptedQueryRunner runner = ScriptedQueryRunner.answering(TestSources.QUERY_RESULT);
        FakeSplitEnumeratorContext<BigQueryReadStreamSplit> context =
                new FakeSplitEnumeratorContext<>(1);
        BigQueryReadEnumeratorState restored =
                new BigQueryReadEnumeratorState(
                        true,
                        ScriptedReadSessionCreator.SESSION,
                        null,
                        Collections.singletonList(split(0, 7)));

        try (BigQueryReadSplitEnumerator enumerator =
                new BigQueryReadSplitEnumerator(
                        context,
                        TestSources.queryConfig(),
                        ScriptedReadSessionCreator.withStreams(1),
                        runner,
                        restored)) {
            enumerator.start();
            context.runAsyncCalls();

            assertThat(runner.runs()).isZero();
            assertThat(context.counter("queryJobsSubmitted")).isZero();
        }
    }

    @Test
    void runsNoQueryForASourceThatNamedATable() throws Exception {
        FakeSplitEnumeratorContext<BigQueryReadStreamSplit> context =
                new FakeSplitEnumeratorContext<>(1);

        try (BigQueryReadSplitEnumerator enumerator =
                enumerator(context, ScriptedReadSessionCreator.withStreams(1), null)) {
            enumerator.start();
            context.runAsyncCalls();

            // Registered by every source so a dashboard reads one set of metrics, so the zero here
            // is a fact about this source rather than about what was registered.
            assertThat(context.counter("queryJobsSubmitted")).isZero();
        }
    }

    @Test
    void failsTheJobWhenTheQueryFails() throws Exception {
        ScriptedQueryRunner runner =
                ScriptedQueryRunner.failing(new IOException("the query was invalid"));
        FakeSplitEnumeratorContext<BigQueryReadStreamSplit> context =
                new FakeSplitEnumeratorContext<>(1);

        try (BigQueryReadSplitEnumerator enumerator =
                queryEnumerator(context, ScriptedReadSessionCreator.withStreams(1), runner)) {
            enumerator.start();

            assertThatThrownBy(context::runAsyncCalls)
                    .isInstanceOf(FlinkRuntimeException.class)
                    .hasMessageContaining("plan the BigQuery read")
                    .hasMessageContaining("the result of the configured query")
                    .hasRootCauseMessage("the query was invalid");
        }
    }

    @Test
    void materializesATableThatTurnsOutToBeAView() throws Exception {
        ScriptedReadSessionCreator creator = ScriptedReadSessionCreator.withStreams(1);
        ScriptedQueryRunner runner = ScriptedQueryRunner.answering(TestSources.QUERY_RESULT);
        FakeSplitEnumeratorContext<BigQueryReadStreamSplit> context =
                new FakeSplitEnumeratorContext<>(1);

        try (BigQueryReadSplitEnumerator enumerator =
                new BigQueryReadSplitEnumerator(
                        context,
                        TestSources.config(builder -> builder.materializeViews()),
                        creator,
                        runner,
                        null)) {
            enumerator.start();
            context.runAsyncCalls();

            assertThat(runner.viewChecks()).isEqualTo(1);
            assertThat(runner.runs()).isEqualTo(1);
            assertThat(runner.lastSpec().getSql())
                    .isEqualTo("SELECT * FROM `" + TestSources.TABLE + "`");
            // What the whole feature is for: the session reads the materialized result.
            assertThat(creator.lastRequest().getReadSession().getTable())
                    .isEqualTo(TestSources.QUERY_RESULT.toTablePath());
        }
    }

    @Test
    void readsATableDirectlyEvenWhenViewsAreMaterialized() throws Exception {
        // The opt-in costs one metadata call and nothing else: an ordinary table is still read
        // straight, with no query job and nothing billed for one.
        ScriptedReadSessionCreator creator = ScriptedReadSessionCreator.withStreams(1);
        ScriptedQueryRunner runner =
                ScriptedQueryRunner.answering(TestSources.QUERY_RESULT).answeringNotAView();
        FakeSplitEnumeratorContext<BigQueryReadStreamSplit> context =
                new FakeSplitEnumeratorContext<>(1);

        try (BigQueryReadSplitEnumerator enumerator =
                new BigQueryReadSplitEnumerator(
                        context,
                        TestSources.config(builder -> builder.materializeViews()),
                        creator,
                        runner,
                        null)) {
            enumerator.start();
            context.runAsyncCalls();

            assertThat(runner.viewChecks()).isEqualTo(1);
            assertThat(runner.runs()).isZero();
            assertThat(context.counter("queryJobsSubmitted")).isZero();
            assertThat(creator.lastRequest().getReadSession().getTable())
                    .isEqualTo(TestSources.TABLE.toTablePath());
        }
    }

    @Test
    void asksNothingAboutATableWhenViewsAreNotMaterialized() throws Exception {
        // The default: no metadata call at all, which is the property the opt-in exists to protect.
        ScriptedQueryRunner runner = ScriptedQueryRunner.answering(TestSources.QUERY_RESULT);
        FakeSplitEnumeratorContext<BigQueryReadStreamSplit> context =
                new FakeSplitEnumeratorContext<>(1);

        try (BigQueryReadSplitEnumerator enumerator =
                new BigQueryReadSplitEnumerator(
                        context,
                        TestSources.config(),
                        ScriptedReadSessionCreator.withStreams(1),
                        runner,
                        null)) {
            enumerator.start();
            context.runAsyncCalls();

            assertThat(runner.viewChecks()).isZero();
            assertThat(runner.runs()).isZero();
        }
    }

    @Test
    void foldsTheProjectionIntoTheQueryItWritesForAView() throws Exception {
        ScriptedQueryRunner runner = ScriptedQueryRunner.answering(TestSources.QUERY_RESULT);
        FakeSplitEnumeratorContext<BigQueryReadStreamSplit> context =
                new FakeSplitEnumeratorContext<>(1);

        try (BigQueryReadSplitEnumerator enumerator =
                new BigQueryReadSplitEnumerator(
                        context,
                        TestSources.config(
                                builder ->
                                        builder.materializeViews()
                                                .selectedFields("id")
                                                .rowRestriction("id > 3")),
                        ScriptedReadSessionCreator.withStreams(1),
                        runner,
                        null)) {
            enumerator.start();
            context.runAsyncCalls();

            // The projection is folded, so the view is not scanned for columns nobody reads. The
            // restriction is not: it is BigQuery's restriction syntax, not a SQL WHERE, and it
            // stays on the read session where a table source applies it too.
            assertThat(runner.lastSpec().getSql())
                    .isEqualTo("SELECT `id` FROM `" + TestSources.TABLE + "`");
        }
    }

    @Test
    void failsTheJobWhenTheViewCheckFails() throws Exception {
        ScriptedQueryRunner runner =
                ScriptedQueryRunner.answering(TestSources.QUERY_RESULT)
                        .failingTheViewCheck(new IOException("denied by IAM"));
        FakeSplitEnumeratorContext<BigQueryReadStreamSplit> context =
                new FakeSplitEnumeratorContext<>(1);

        try (BigQueryReadSplitEnumerator enumerator =
                new BigQueryReadSplitEnumerator(
                        context,
                        TestSources.config(builder -> builder.materializeViews()),
                        ScriptedReadSessionCreator.withStreams(1),
                        runner,
                        null)) {
            enumerator.start();

            assertThatThrownBy(context::runAsyncCalls)
                    .isInstanceOf(FlinkRuntimeException.class)
                    .hasRootCauseMessage("denied by IAM");
        }
    }

    private static BigQueryReadSplitEnumerator queryEnumerator(
            FakeSplitEnumeratorContext<BigQueryReadStreamSplit> context,
            ScriptedReadSessionCreator creator,
            ScriptedQueryRunner runner) {
        return new BigQueryReadSplitEnumerator(
                context, TestSources.queryConfig(), creator, runner, null);
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
        return new BigQueryReadSplitEnumerator(
                context, TestSources.config(), creator, null, restored);
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
