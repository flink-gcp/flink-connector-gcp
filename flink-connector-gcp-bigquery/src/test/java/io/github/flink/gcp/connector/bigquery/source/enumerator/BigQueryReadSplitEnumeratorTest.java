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

package io.github.flink.gcp.connector.bigquery.source.enumerator;

import org.apache.flink.api.connector.source.ReaderInfo;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.apache.flink.metrics.groups.SplitEnumeratorMetricGroup;
import org.apache.flink.util.FlinkRuntimeException;

import io.github.flink.gcp.connector.bigquery.source.BigQuerySourceConfig;
import io.github.flink.gcp.connector.bigquery.source.TestRows;
import io.github.flink.gcp.connector.bigquery.source.TestSources;
import io.github.flink.gcp.connector.bigquery.source.query.ScriptedQueryRunner;
import io.github.flink.gcp.connector.bigquery.source.split.BigQueryReadStreamSplit;
import io.github.flink.gcp.connector.testutils.FakeSplitEnumeratorContext;
import io.github.flink.gcp.connector.testutils.LogCapture;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.function.BiConsumer;

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
    void derivesTheReuseIdentityFromTheJobNameTheMetricGroupCarries() throws Exception {
        ScriptedQueryRunner runner = ScriptedQueryRunner.answering(TestSources.QUERY_RESULT);
        FakeSplitEnumeratorContext<BigQueryReadStreamSplit> context =
                new FakeSplitEnumeratorContext<>(1);
        context.putMetricVariable("<job_name>", "my pipeline");

        try (BigQueryReadSplitEnumerator enumerator =
                new BigQueryReadSplitEnumerator(
                        context,
                        TestSources.queryConfig(
                                builder ->
                                        builder.queryLocation("asia-northeast1")
                                                .reuseQueryResultWithin(Duration.ofHours(1))),
                        ScriptedReadSessionCreator.withStreams(1),
                        runner,
                        null)) {
            enumerator.start();
            context.runAsyncCalls();

            assertThat(runner.lastSpec().getJobIdentity()).isNotNull();
            assertThat(runner.lastSpec().getJobIdentity().getCurrentJobId())
                    .contains("_my_pipeline_");
        }
    }

    @Test
    void fallsBackToARandomIdWhereTheMetricGroupCarriesNoJobName() throws Exception {
        // The fake context's variables are empty unless a test injects them, which is exactly the
        // production fallback under test: a runtime that did not fill the variable in must get
        // today's behaviour, not a failed job.
        ScriptedQueryRunner runner = ScriptedQueryRunner.answering(TestSources.QUERY_RESULT);
        FakeSplitEnumeratorContext<BigQueryReadStreamSplit> context =
                new FakeSplitEnumeratorContext<>(1);

        try (BigQueryReadSplitEnumerator enumerator =
                new BigQueryReadSplitEnumerator(
                        context,
                        TestSources.queryConfig(
                                builder ->
                                        builder.queryLocation("asia-northeast1")
                                                .reuseQueryResultWithin(Duration.ofHours(1))),
                        ScriptedReadSessionCreator.withStreams(1),
                        runner,
                        null)) {
            enumerator.start();
            context.runAsyncCalls();

            assertThat(runner.runs()).isEqualTo(1);
            assertThat(runner.lastSpec().getJobIdentity()).isNull();
        }
    }

    @Test
    void withoutTheReuseKnobNoIdentityIsDerivedEvenWithAJobName() throws Exception {
        ScriptedQueryRunner runner = ScriptedQueryRunner.answering(TestSources.QUERY_RESULT);
        FakeSplitEnumeratorContext<BigQueryReadStreamSplit> context =
                new FakeSplitEnumeratorContext<>(1);
        context.putMetricVariable("<job_name>", "my pipeline");

        try (BigQueryReadSplitEnumerator enumerator =
                queryEnumerator(context, ScriptedReadSessionCreator.withStreams(1), runner)) {
            enumerator.start();
            context.runAsyncCalls();

            assertThat(runner.lastSpec().getJobIdentity()).isNull();
        }
    }

    @Test
    void aMaterializedViewsQueryCarriesTheIdentityToo() throws Exception {
        // The identity wiring sits after the forView branch, so this pins that a view's generated
        // SELECT is reused under the same rules as a hand-written query.
        ScriptedQueryRunner runner = ScriptedQueryRunner.answering(TestSources.QUERY_RESULT);
        FakeSplitEnumeratorContext<BigQueryReadStreamSplit> context =
                new FakeSplitEnumeratorContext<>(1);
        context.putMetricVariable("<job_name>", "my pipeline");

        try (BigQueryReadSplitEnumerator enumerator =
                new BigQueryReadSplitEnumerator(
                        context,
                        TestSources.config(
                                builder ->
                                        builder.materializeViews()
                                                .queryLocation("asia-northeast1")
                                                .reuseQueryResultWithin(Duration.ofHours(1))),
                        ScriptedReadSessionCreator.withStreams(1),
                        runner,
                        null)) {
            enumerator.start();
            context.runAsyncCalls();

            assertThat(runner.viewChecks()).isEqualTo(1);
            assertThat(runner.lastSpec().getJobIdentity()).isNotNull();
        }
    }

    @Test
    void countsAReuseApartFromASubmission() throws Exception {
        ScriptedQueryRunner runner =
                ScriptedQueryRunner.answering(TestSources.QUERY_RESULT).reattaching();
        FakeSplitEnumeratorContext<BigQueryReadStreamSplit> context =
                new FakeSplitEnumeratorContext<>(1);

        try (BigQueryReadSplitEnumerator enumerator =
                queryEnumerator(context, ScriptedReadSessionCreator.withStreams(1), runner)) {
            enumerator.start();
            context.runAsyncCalls();

            // queryJobsSubmitted means "the query was billed"; a reuse is exactly a billing that
            // did not happen, so it lands on its own counter.
            assertThat(context.counter("queryJobsSubmitted")).isZero();
            assertThat(context.counter("queryJobsReattached")).isEqualTo(1);
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

    @Test
    void warnsWhenTheRestoredSessionHasAlreadyExpired() throws Exception {
        BigQueryReadEnumeratorState restored =
                new BigQueryReadEnumeratorState(
                        true,
                        ScriptedReadSessionCreator.SESSION,
                        Instant.now().minusSeconds(60),
                        Collections.singletonList(split(0, 7)));
        FakeSplitEnumeratorContext<BigQueryReadStreamSplit> context =
                new FakeSplitEnumeratorContext<>(1);

        try (LogCapture capture = LogCapture.of(BigQueryReadSplitEnumerator.class);
                BigQueryReadSplitEnumerator enumerator =
                        enumerator(context, ScriptedReadSessionCreator.withStreams(1), restored)) {
            enumerator.start();
            context.runAsyncCalls();

            // The warning is the whole report: nothing can recover the session — creating a second
            // one is exactly what the restore guard forbids — so the reads fail later, and this
            // line in the JobManager log is what says the job has to be started over.
            assertThat(capture.getMessages())
                    .singleElement()
                    .asString()
                    .contains(ScriptedReadSessionCreator.SESSION)
                    .contains("expired");
        }
    }

    @Test
    void staysQuietWhenTheRestoredSessionIsStillAlive() throws Exception {
        // The control arm: the warning is about the expiry having passed, not about restoring.
        BigQueryReadEnumeratorState restored =
                new BigQueryReadEnumeratorState(
                        true,
                        ScriptedReadSessionCreator.SESSION,
                        Instant.now().plusSeconds(3_600),
                        Collections.singletonList(split(0, 7)));
        FakeSplitEnumeratorContext<BigQueryReadStreamSplit> context =
                new FakeSplitEnumeratorContext<>(1);

        try (LogCapture capture = LogCapture.of(BigQueryReadSplitEnumerator.class);
                BigQueryReadSplitEnumerator enumerator =
                        enumerator(context, ScriptedReadSessionCreator.withStreams(1), restored)) {
            enumerator.start();
            context.runAsyncCalls();

            assertThat(capture.getMessages()).isEmpty();
        }
    }

    @Test
    void warnsWhenBigQueryAnswersFewerStreamsThanTheParallelism() throws Exception {
        FakeSplitEnumeratorContext<BigQueryReadStreamSplit> context =
                new FakeSplitEnumeratorContext<>(2);

        try (LogCapture capture = LogCapture.of(BigQueryReadSplitEnumerator.class);
                BigQueryReadSplitEnumerator enumerator =
                        enumerator(context, ScriptedReadSessionCreator.withStreams(1), null)) {
            enumerator.start();
            context.runAsyncCalls();

            // The warning is the whole report: the subtasks left without a stream finish
            // immediately and nothing else says why, because the stream count is BigQuery's
            // decision and maxStreamCount only caps it.
            assertThat(capture.getMessages())
                    .singleElement()
                    .asString()
                    .contains("1 stream(s)")
                    .contains("parallelism 2");
        }
    }

    @Test
    void staysQuietWhenEverySubtaskHasAStream() throws Exception {
        FakeSplitEnumeratorContext<BigQueryReadStreamSplit> context =
                new FakeSplitEnumeratorContext<>(2);

        try (LogCapture capture = LogCapture.of(BigQueryReadSplitEnumerator.class);
                BigQueryReadSplitEnumerator enumerator =
                        enumerator(context, ScriptedReadSessionCreator.withStreams(2), null)) {
            enumerator.start();
            context.runAsyncCalls();

            assertThat(capture.getMessages()).isEmpty();
        }
    }

    @Test
    void plansAQuerySourceWithoutAMetricGroup() throws Exception {
        // Flink's own contexts always offer a metric group, but the API does not promise one, and
        // a context answering with nothing must not fail the planning call — there is simply
        // nothing to count into.
        FakeSplitEnumeratorContext<BigQueryReadStreamSplit> submitDelegate =
                new FakeSplitEnumeratorContext<>(1);
        ScriptedQueryRunner submitted = ScriptedQueryRunner.answering(TestSources.QUERY_RESULT);
        try (BigQueryReadSplitEnumerator enumerator =
                queryEnumerator(
                        new WithoutMetrics(submitDelegate),
                        ScriptedReadSessionCreator.withStreams(1),
                        submitted)) {
            enumerator.start();
            submitDelegate.runAsyncCalls();

            assertThat(submitted.runs()).isEqualTo(1);
        }

        // Both counter arms: a reuse is counted apart from a submission, so each has its own guard.
        FakeSplitEnumeratorContext<BigQueryReadStreamSplit> reuseDelegate =
                new FakeSplitEnumeratorContext<>(1);
        ScriptedQueryRunner reused =
                ScriptedQueryRunner.answering(TestSources.QUERY_RESULT).reattaching();
        try (BigQueryReadSplitEnumerator enumerator =
                queryEnumerator(
                        new WithoutMetrics(reuseDelegate),
                        ScriptedReadSessionCreator.withStreams(1),
                        reused)) {
            enumerator.start();
            reuseDelegate.runAsyncCalls();

            assertThat(reused.runs()).isEqualTo(1);
        }
    }

    /**
     * A context that offers no metric group, which the shared fake cannot express and Flink's own
     * contexts never do.
     */
    private static final class WithoutMetrics
            implements SplitEnumeratorContext<BigQueryReadStreamSplit> {

        private final FakeSplitEnumeratorContext<BigQueryReadStreamSplit> delegate;

        private WithoutMetrics(FakeSplitEnumeratorContext<BigQueryReadStreamSplit> delegate) {
            this.delegate = delegate;
        }

        @Override
        @Nullable
        public SplitEnumeratorMetricGroup metricGroup() {
            return null;
        }

        @Override
        public void sendEventToSourceReader(int subtaskId, SourceEvent event) {
            delegate.sendEventToSourceReader(subtaskId, event);
        }

        @Override
        public int currentParallelism() {
            return delegate.currentParallelism();
        }

        @Override
        public Map<Integer, ReaderInfo> registeredReaders() {
            return delegate.registeredReaders();
        }

        @Override
        public void assignSplits(SplitsAssignment<BigQueryReadStreamSplit> newSplitAssignments) {
            delegate.assignSplits(newSplitAssignments);
        }

        @Override
        public void signalNoMoreSplits(int subtask) {
            delegate.signalNoMoreSplits(subtask);
        }

        @Override
        public <T> void callAsync(Callable<T> callable, BiConsumer<T, Throwable> handler) {
            delegate.callAsync(callable, handler);
        }

        @Override
        public <T> void callAsync(
                Callable<T> callable,
                BiConsumer<T, Throwable> handler,
                long initialDelayMillis,
                long periodMillis) {
            delegate.callAsync(callable, handler, initialDelayMillis, periodMillis);
        }

        @Override
        public void runInCoordinatorThread(Runnable runnable) {
            delegate.runInCoordinatorThread(runnable);
        }
    }

    private static BigQueryReadSplitEnumerator queryEnumerator(
            SplitEnumeratorContext<BigQueryReadStreamSplit> context,
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
