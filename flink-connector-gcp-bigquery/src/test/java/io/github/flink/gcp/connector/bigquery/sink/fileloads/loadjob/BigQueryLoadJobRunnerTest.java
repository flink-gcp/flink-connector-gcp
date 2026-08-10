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

package io.github.flink.gcp.connector.bigquery.sink.fileloads.loadjob;

import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.CopyJobConfiguration;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FormatOptions;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.JobStatus;
import com.google.cloud.bigquery.LoadJobConfiguration;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TestJobs;
import io.github.flink.gcp.connector.base.retry.RetrySchedule;
import io.github.flink.gcp.connector.bigquery.StubBigQuery;
import io.github.flink.gcp.connector.bigquery.StubBigQuery.JobAnswer;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.StagingFormat;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link BigQueryLoadJobRunner} against a scripted {@link StubBigQuery}.
 *
 * <p>What is being pinned is the exactly-once mechanics the {@link LoadJobRunner} contract puts on
 * an implementation: a deterministic job id doubles as an idempotency key, so which id a restarted
 * attempt submits — and which existing job it attaches to instead of submitting — decides whether a
 * restart loads the same staged files twice, once, or not at all. Until this class existed the only
 * coverage was the gated real-GCP FILE_LOADS suites, which run weekly and only along the happy
 * path.
 */
class BigQueryLoadJobRunnerTest {

    private static final String JOB_ID = "bq-1";
    private static final String LOCATION = "asia-northeast1";

    private static final TableDestination DESTINATION = TableDestination.of("p", "d", "t");
    private static final TableDestination TEMP = TableDestination.of("p", "d", "t_tmp");
    private static final DatasetId DATASET = DatasetId.of("p", "d");

    private static final Schema SCHEMA = Schema.of(Field.of("f1", StandardSQLTypeName.STRING));

    /**
     * One millisecond is the fastest a {@link RetrySchedule} may be built (its initial backoff must
     * be positive), and no jitter keeps a polling test deterministic. The attempt cap is never read
     * by {@code awaitJob}, which polls without one on purpose.
     */
    private static final RetrySchedule FAST = new RetrySchedule(1, 1, Integer.MAX_VALUE, 0);

    private final StubBigQuery client = new StubBigQuery();

    private BigQueryLoadJobRunner runner() {
        return new BigQueryLoadJobRunner(client, LOCATION, FAST);
    }

    // ---------------------------------------------------------------------------------------
    // submitOrAttach: probing a previous attempt's deterministic ids
    // ---------------------------------------------------------------------------------------

    @Test
    void submitsTheBaseIdWhenNoPreviousJobExists() throws Exception {
        client.answering(JobAnswer.absent());

        BigQueryLoadJobRunner runner = runner();
        runner.submitLoad(JOB_ID, loadSpec(List.of()));
        runner.awaitJob(JOB_ID);

        assertThat(client.getJobCalls).extracting(id -> id.getJob()).containsExactly(JOB_ID);
        assertThat(client.created)
                .extracting(info -> info.getJobId().getJob())
                .containsExactly(JOB_ID);
    }

    @Test
    void reAttachesToARunningJobFromAPreviousAttempt() throws Exception {
        client.answering(
                JobAnswer.withStatus(TestJobs.status(JobStatus.State.RUNNING)),
                JobAnswer.withStatus(TestJobs.status(JobStatus.State.DONE)));

        BigQueryLoadJobRunner runner = runner();
        runner.submitLoad(JOB_ID, loadSpec(List.of()));
        runner.awaitJob(JOB_ID);

        assertThat(client.created).isEmpty();
        assertThat(client.getJobCalls)
                .extracting(id -> id.getJob())
                .containsExactly(JOB_ID, JOB_ID);
    }

    @Test
    void reAttachesToACompletedJobWithoutResubmittingIt() throws Exception {
        client.answering(JobAnswer.withStatus(TestJobs.status(JobStatus.State.DONE)));

        BigQueryLoadJobRunner runner = runner();
        runner.submitLoad(JOB_ID, loadSpec(List.of()));
        runner.awaitJob(JOB_ID);

        assertThat(client.created).isEmpty();
        // One lookup and nothing else: no retry id probed, and no poll, because the job is done.
        assertThat(client.getJobCalls).hasSize(1);
    }

    @Test
    void reAttachesToAJobWhoseStatusTheResponseDidNotCarry() throws Exception {
        client.answering(
                JobAnswer.withoutStatus(),
                JobAnswer.withStatus(TestJobs.status(JobStatus.State.DONE)));

        BigQueryLoadJobRunner runner = runner();
        runner.submitLoad(JOB_ID, loadSpec(List.of()));
        runner.awaitJob(JOB_ID);

        assertThat(client.created).isEmpty();
        assertThat(client.getJobCalls).hasSize(2);
    }

    @Test
    void probesTheNextIdWhenThePreviousAttemptFailed() throws Exception {
        client.answering(JobAnswer.withStatus(failed("r1")), JobAnswer.absent());

        BigQueryLoadJobRunner runner = runner();
        runner.submitLoad(JOB_ID, loadSpec(List.of()));
        // The handle is filed under the base id, not under the id that was finally submitted.
        runner.awaitJob(JOB_ID);

        assertThat(client.getJobCalls)
                .extracting(id -> id.getJob())
                .containsExactly(JOB_ID, JOB_ID + "-r1");
        assertThat(client.created)
                .extracting(info -> info.getJobId().getJob())
                .containsExactly(JOB_ID + "-r1");
    }

    @Test
    void probesPastEveryFailedIdUpToTheCap() throws Exception {
        client.answering(
                JobAnswer.withStatus(failed("r1")),
                JobAnswer.withStatus(failed("r2")),
                JobAnswer.withStatus(failed("r3")),
                JobAnswer.withStatus(failed("r4")),
                JobAnswer.withStatus(failed("r5")),
                JobAnswer.absent());

        runner().submitLoad(JOB_ID, loadSpec(List.of()));

        assertThat(client.getJobCalls)
                .extracting(id -> id.getJob())
                .containsExactly(
                        JOB_ID,
                        JOB_ID + "-r1",
                        JOB_ID + "-r2",
                        JOB_ID + "-r3",
                        JOB_ID + "-r4",
                        JOB_ID + "-r5");
        assertThat(client.created)
                .extracting(info -> info.getJobId().getJob())
                .containsExactly(JOB_ID + "-r5");
    }

    @Test
    void reAttachesToARetryIdWhenTheBaseIdFailed() throws Exception {
        client.answering(
                JobAnswer.withStatus(failed("r1")),
                JobAnswer.withStatus(TestJobs.status(JobStatus.State.RUNNING)),
                JobAnswer.withStatus(TestJobs.status(JobStatus.State.DONE)));

        BigQueryLoadJobRunner runner = runner();
        runner.submitLoad(JOB_ID, loadSpec(List.of()));
        // Filed under the base id even though the job that was found is the retry id's, and
        // polled by the id it actually has.
        runner.awaitJob(JOB_ID);

        assertThat(client.created).isEmpty();
        assertThat(client.getJobCalls)
                .extracting(id -> id.getJob())
                .containsExactly(JOB_ID, JOB_ID + "-r1", JOB_ID + "-r1");
    }

    @ParameterizedTest
    @ValueSource(strings = {"PENDING", "RUNNING"})
    void doesNotProbePastAnUnfinishedJobThatReportsAnError(String state) throws Exception {
        // Only a job that has finished releases its id. One that has not, but already carries an
        // error, must be waited for rather than probed past: probing would submit a second load of
        // the same files while the first is still free to write them.
        client.answering(
                JobAnswer.withStatus(
                        TestJobs.status(
                                JobStatus.State.valueOf(state),
                                new BigQueryError("invalid", null, "row 7"),
                                null)),
                JobAnswer.withStatus(TestJobs.status(JobStatus.State.DONE)));

        BigQueryLoadJobRunner runner = runner();
        runner.submitLoad(JOB_ID, loadSpec(List.of()));
        runner.awaitJob(JOB_ID);

        assertThat(client.created).isEmpty();
        assertThat(client.getJobCalls)
                .extracting(id -> id.getJob())
                .containsExactly(JOB_ID, JOB_ID);
    }

    @Test
    void givesUpWhenEveryRetryIdAlsoFailed() {
        client.answering(
                JobAnswer.withStatus(failed("first")),
                JobAnswer.withStatus(failed("second")),
                JobAnswer.withStatus(failed("third")),
                JobAnswer.withStatus(failed("fourth")),
                JobAnswer.withStatus(failed("fifth")),
                JobAnswer.withStatus(failed("sixth")));

        BigQueryLoadJobRunner runner = runner();

        assertThatThrownBy(() -> runner.submitLoad(JOB_ID, loadSpec(List.of())))
                .isInstanceOf(IOException.class)
                .hasMessageContaining(JOB_ID)
                .hasMessageContaining("all its retry ids failed")
                // The error reported is the last one probed, not the first.
                .hasMessageContaining("sixth");
        assertThat(client.created).isEmpty();
        assertThat(client.getJobCalls)
                .extracting(id -> id.getJob())
                .containsExactly(
                        JOB_ID,
                        JOB_ID + "-r1",
                        JOB_ID + "-r2",
                        JOB_ID + "-r3",
                        JOB_ID + "-r4",
                        JOB_ID + "-r5");
    }

    // ---------------------------------------------------------------------------------------
    // create: the conflict race against a previous attempt's zombie
    // ---------------------------------------------------------------------------------------

    @Test
    void attachesToTheExistingJobWhenTheSubmitLosesAConflict() throws Exception {
        client.answering(
                JobAnswer.absent(), JobAnswer.withStatus(TestJobs.status(JobStatus.State.DONE)));
        client.createFailure = conflict();

        BigQueryLoadJobRunner runner = runner();
        runner.submitLoad(JOB_ID, loadSpec(List.of()));
        // Reaching this without throwing is the assertion: the conflicting job is what was filed.
        runner.awaitJob(JOB_ID);

        assertThat(client.getJobCalls)
                .extracting(id -> id.getJob())
                .containsExactly(JOB_ID, JOB_ID);
        // The conflicting attempt is the only submit: attaching must not create a second job,
        // which the real service would answer with another 409 (the stub's one-shot would not).
        assertThat(client.created)
                .extracting(info -> info.getJobId().getJob())
                .containsExactly(JOB_ID);
    }

    @Test
    void aConflictWithARunningJobBehindItIsAttachedToAndPolled() throws Exception {
        // The common real outcome of the race: the zombie is still loading. It is attached to and
        // awaited like any re-attached running job.
        client.answering(
                JobAnswer.absent(),
                JobAnswer.withStatus(TestJobs.status(JobStatus.State.RUNNING)),
                JobAnswer.withStatus(TestJobs.status(JobStatus.State.DONE)));
        client.createFailure = conflict();

        BigQueryLoadJobRunner runner = runner();
        runner.submitLoad(JOB_ID, loadSpec(List.of()));
        runner.awaitJob(JOB_ID);

        assertThat(client.created)
                .extracting(info -> info.getJobId().getJob())
                .containsExactly(JOB_ID);
        // The probe, the conflict lookup, then the poll that saw the job finish.
        assertThat(client.getJobCalls)
                .extracting(id -> id.getJob())
                .containsExactly(JOB_ID, JOB_ID, JOB_ID);
    }

    @Test
    void aConflictWithAFailedJobBehindItProbesTheNextIdInsteadOfAttaching() throws Exception {
        // The probe said absent, the create lost the race anyway, and the job it lost to had
        // already failed. Attaching would fail the commit with that stored failure; a fresh -rN
        // id can still load the data, which is the whole point of the probes.
        client.answering(
                JobAnswer.absent(),
                JobAnswer.withStatus(failed("zombie had failed")),
                JobAnswer.absent());
        client.createFailure = conflict();

        BigQueryLoadJobRunner runner = runner();
        runner.submitLoad(JOB_ID, loadSpec(List.of()));
        runner.awaitJob(JOB_ID);

        // The base id's probe, the conflict lookup, then the retry id's probe.
        assertThat(client.getJobCalls)
                .extracting(id -> id.getJob())
                .containsExactly(JOB_ID, JOB_ID, JOB_ID + "-r1");
        assertThat(client.created)
                .extracting(info -> info.getJobId().getJob())
                .containsExactly(JOB_ID, JOB_ID + "-r1");
    }

    @Test
    void aConflictOnTheLastRetryIdFeedsTheGiveUpMessage() {
        // The failed job the conflict lookup finds reports its error the same way one the probe
        // finds does: as the "last error" of the give-up message, since it was met last.
        client.answering(
                JobAnswer.withStatus(failed("first")),
                JobAnswer.withStatus(failed("second")),
                JobAnswer.withStatus(failed("third")),
                JobAnswer.withStatus(failed("fourth")),
                JobAnswer.withStatus(failed("fifth")),
                JobAnswer.absent(),
                JobAnswer.withStatus(failed("behind the conflict")));
        client.createFailure = conflict();

        BigQueryLoadJobRunner runner = runner();

        assertThatThrownBy(() -> runner.submitLoad(JOB_ID, loadSpec(List.of())))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("all its retry ids failed")
                .hasMessageContaining("behind the conflict");
        assertThat(client.created)
                .extracting(info -> info.getJobId().getJob())
                .containsExactly(JOB_ID + "-r5");
    }

    @Test
    void aStatuslessJobFromTheCreateIsAttachedToAndPolled() throws Exception {
        // What the SDK's own already-exists absorber hands back: it re-fetches the conflicting
        // job with fields that exclude the status (ADR-0018), so no failed-job verdict can be
        // read from it. It must be attached to — polling resolves what it is — never probed
        // past, or the absorber's every answer would burn a retry id.
        client.createdStatus = null;
        client.answering(
                JobAnswer.absent(), JobAnswer.withStatus(TestJobs.status(JobStatus.State.DONE)));

        BigQueryLoadJobRunner runner = runner();
        runner.submitLoad(JOB_ID, loadSpec(List.of()));
        runner.awaitJob(JOB_ID);

        assertThat(client.created)
                .extracting(info -> info.getJobId().getJob())
                .containsExactly(JOB_ID);
        // The probe, then the poll that resolved the missing status.
        assertThat(client.getJobCalls)
                .extracting(id -> id.getJob())
                .containsExactly(JOB_ID, JOB_ID);
    }

    @Test
    void aConflictWithNoJobBehindItIsStillASubmitFailure() {
        client.answering(JobAnswer.absent(), JobAnswer.absent());
        BigQueryException conflict = conflict();
        client.createFailure = conflict;

        BigQueryLoadJobRunner runner = runner();

        assertThatThrownBy(() -> runner.submitLoad(JOB_ID, loadSpec(List.of())))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Failed to submit BigQuery job " + JOB_ID)
                .hasCause(conflict);
    }

    @Test
    void aSubmitFailureThatIsNotAConflictLooksForNoExistingJob() {
        client.answering(JobAnswer.absent());
        BigQueryException unavailable = unavailable();
        client.createFailure = unavailable;

        BigQueryLoadJobRunner runner = runner();

        assertThatThrownBy(() -> runner.submitLoad(JOB_ID, loadSpec(List.of())))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Failed to submit BigQuery job " + JOB_ID)
                .hasCause(unavailable);
        // Only the probe: a non-conflict failure must not look the job up a second time.
        assertThat(client.getJobCalls).hasSize(1);
    }

    // ---------------------------------------------------------------------------------------
    // lookups that fail: the client's unchecked type must not cross the SPI
    // ---------------------------------------------------------------------------------------

    @Test
    void aLookupThatFailsWhileProbingFailsTheSubmitAsAnIOException() {
        BigQueryException unavailable = unavailable();
        client.answering(JobAnswer.failing(unavailable));

        BigQueryLoadJobRunner runner = runner();

        assertThatThrownBy(() -> runner.submitLoad(JOB_ID, loadSpec(List.of())))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Failed to look up BigQuery job " + JOB_ID)
                .hasMessageContaining("previous attempt")
                .hasCause(unavailable);
    }

    @Test
    void aLookupThatFailsWhilePollingFailsTheAwaitAsAnIOException() throws Exception {
        BigQueryException unavailable = unavailable();
        client.createdStatus = TestJobs.status(JobStatus.State.RUNNING);
        client.answering(JobAnswer.absent(), JobAnswer.failing(unavailable));

        BigQueryLoadJobRunner runner = runner();
        runner.submitLoad(JOB_ID, loadSpec(List.of()));

        assertThatThrownBy(() -> runner.awaitJob(JOB_ID))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Failed to look up BigQuery job " + JOB_ID)
                .hasMessageContaining("polling")
                .hasCause(unavailable);
    }

    @Test
    void aLookupThatFailsAfterAConflictKeepsTheConflict() {
        BigQueryException conflict = conflict();
        BigQueryException unavailable = unavailable();
        client.answering(JobAnswer.absent(), JobAnswer.failing(unavailable));
        client.createFailure = conflict;

        BigQueryLoadJobRunner runner = runner();

        // The lookup failure is what went wrong last, but the conflict is the one that says the id
        // is already taken — which is what a reader of this failure needs in order to act on it.
        assertThatThrownBy(() -> runner.submitLoad(JOB_ID, loadSpec(List.of())))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Failed to submit BigQuery job " + JOB_ID)
                .hasCause(unavailable)
                .cause()
                .satisfies(cause -> assertThat(cause.getSuppressed()).containsExactly(conflict));
    }

    // ---------------------------------------------------------------------------------------
    // awaitJob
    // ---------------------------------------------------------------------------------------

    @Test
    void awaitingAJobThatWasNeverSubmittedIsAProgrammingError() {
        BigQueryLoadJobRunner runner = runner();

        assertThatThrownBy(() -> runner.awaitJob(JOB_ID))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Job " + JOB_ID + " was never submitted");
    }

    @Test
    void aJobIsAwaitedOnceAndItsHandleIsThenReleased() throws Exception {
        client.answering(JobAnswer.absent());

        BigQueryLoadJobRunner runner = runner();
        runner.submitLoad(JOB_ID, loadSpec(List.of()));
        runner.awaitJob(JOB_ID);

        assertThatThrownBy(() -> runner.awaitJob(JOB_ID))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("was never submitted");
    }

    @Test
    void pollsUntilTheJobReportsDone() throws Exception {
        client.createdStatus = TestJobs.status(JobStatus.State.RUNNING);
        client.answering(
                JobAnswer.absent(),
                JobAnswer.withStatus(TestJobs.status(JobStatus.State.PENDING)),
                JobAnswer.withStatus(TestJobs.status(JobStatus.State.RUNNING)),
                JobAnswer.withStatus(TestJobs.status(JobStatus.State.DONE)));

        BigQueryLoadJobRunner runner = runner();
        runner.submitLoad(JOB_ID, loadSpec(List.of()));
        runner.awaitJob(JOB_ID);

        // The probe, then one poll per state read, all of the same job: only DONE ends the wait,
        // and polling stops there rather than one fetch later.
        assertThat(client.getJobCalls)
                .extracting(id -> id.getJob())
                .containsExactly(JOB_ID, JOB_ID, JOB_ID, JOB_ID);
        // The location travels with every poll: a lookup without it resolves to another region's
        // job namespace, where this job reads as gone.
        assertThat(client.getJobCalls)
                .allSatisfy(id -> assertThat(id.getLocation()).isEqualTo(LOCATION));
    }

    @Test
    void aJobThatDisappearsWhilePollingFailsTheCommit() throws Exception {
        client.createdStatus = TestJobs.status(JobStatus.State.RUNNING);
        client.answering(JobAnswer.absent(), JobAnswer.absent());

        BigQueryLoadJobRunner runner = runner();
        runner.submitLoad(JOB_ID, loadSpec(List.of()));

        assertThatThrownBy(() -> runner.awaitJob(JOB_ID))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("BigQuery job " + JOB_ID + " disappeared while polling");
    }

    @Test
    void aJobThatFailsWhilePollingFailsTheCommitWithItsErrors() throws Exception {
        client.createdStatus = TestJobs.status(JobStatus.State.RUNNING);
        client.answering(
                JobAnswer.absent(),
                JobAnswer.withStatus(
                        TestJobs.status(
                                JobStatus.State.DONE,
                                new BigQueryError("invalid", null, "bad schema"),
                                List.of(new BigQueryError("invalid", null, "row 7")))));

        BigQueryLoadJobRunner runner = runner();
        runner.submitLoad(JOB_ID, loadSpec(List.of()));

        assertThatThrownBy(() -> runner.awaitJob(JOB_ID))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("BigQuery job " + JOB_ID + " failed")
                .hasMessageContaining("bad schema")
                .hasMessageContaining("execution errors")
                .hasMessageContaining("row 7");
    }

    @Test
    void waitsForAnUnfinishedJobThatReportsAnErrorWhilePolling() throws Exception {
        // The same rule the submit side applies: an error only ends the wait once the job says it
        // has finished. BigQuery sets an error result together with DONE, so this is the
        // service's contract being relied on rather than a state anyone expects to meet.
        client.createdStatus = TestJobs.status(JobStatus.State.RUNNING);
        client.answering(
                JobAnswer.absent(),
                JobAnswer.withStatus(
                        TestJobs.status(
                                JobStatus.State.RUNNING,
                                new BigQueryError("invalid", null, "row 7"),
                                null)),
                JobAnswer.withStatus(TestJobs.status(JobStatus.State.DONE)));

        BigQueryLoadJobRunner runner = runner();
        runner.submitLoad(JOB_ID, loadSpec(List.of()));
        runner.awaitJob(JOB_ID);

        assertThat(client.getJobCalls).hasSize(3);
    }

    @Test
    void aFailureNamesTheJobIdThatRan() throws Exception {
        // The id that failed is the one the operator has to look up, and after a probe it is not
        // the id the caller passed.
        client.answering(
                JobAnswer.withStatus(failed("the previous attempt")),
                JobAnswer.absent(),
                JobAnswer.withStatus(
                        TestJobs.status(
                                JobStatus.State.DONE,
                                new BigQueryError("invalid", null, "bad schema"),
                                null)));
        client.createdStatus = TestJobs.status(JobStatus.State.RUNNING);

        BigQueryLoadJobRunner runner = runner();
        runner.submitLoad(JOB_ID, loadSpec(List.of()));

        assertThatThrownBy(() -> runner.awaitJob(JOB_ID))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("BigQuery job " + JOB_ID + "-r1 failed");
    }

    @Test
    void aFailureThatReportsNoExecutionErrorsAppendsNone() throws Exception {
        client.createdStatus = TestJobs.status(JobStatus.State.RUNNING);
        client.answering(
                JobAnswer.absent(),
                JobAnswer.withStatus(
                        TestJobs.status(
                                JobStatus.State.DONE,
                                new BigQueryError("invalid", null, "bad schema"),
                                null)));

        BigQueryLoadJobRunner runner = runner();
        runner.submitLoad(JOB_ID, loadSpec(List.of()));

        assertThatThrownBy(() -> runner.awaitJob(JOB_ID))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("BigQuery job " + JOB_ID + " failed")
                .hasMessageContaining("bad schema")
                // The service reported no execution errors, so none are appended.
                .hasMessageNotContaining("execution errors");
    }

    // ---------------------------------------------------------------------------------------
    // deleteTable
    // ---------------------------------------------------------------------------------------

    @Test
    void deletesTheTemporaryTable() {
        runner().deleteTable(TEMP);

        assertThat(client.deleted).containsExactly(TableId.of("p", "d", "t_tmp"));
    }

    @Test
    void aFailedTemporaryTableDeleteIsSwallowed() {
        client.deleteFailure = new BigQueryException(HttpURLConnection.HTTP_FORBIDDEN, "denied");

        BigQueryLoadJobRunner runner = runner();

        // Cleanup is best-effort: a commit that loaded its data must not fail on a leftover table.
        assertThatCode(() -> runner.deleteTable(TEMP)).doesNotThrowAnyException();
        assertThat(client.deleted).containsExactly(TableId.of("p", "d", "t_tmp"));
    }

    @Test
    void aTemporaryTableDeleteThatFailsOutsideTheClientIsSwallowedToo() {
        // Not every failure here is a BigQueryException: the delete is the first call to reach
        // the lazily built client, which answers a missing project with IllegalArgumentException,
        // and a closed or broken transport with IllegalStateException. Cleanup runs after every
        // load and copy has been awaited, so an escape fails a commit whose data is already
        // durable — which is why the catch is RuntimeException rather than the client's own type.
        client.deleteFailure = new IllegalStateException("client is closed");

        BigQueryLoadJobRunner runner = runner();

        assertThatCode(() -> runner.deleteTable(TEMP)).doesNotThrowAnyException();
        assertThat(client.deleted).containsExactly(TableId.of("p", "d", "t_tmp"));
    }

    // ---------------------------------------------------------------------------------------
    // job ids and job configurations
    // ---------------------------------------------------------------------------------------

    @Test
    void theJobIdCarriesTheConfiguredLocation() throws Exception {
        client.answering(JobAnswer.absent());

        runner().submitLoad(JOB_ID, loadSpec(List.of()));

        assertThat(client.getJobCalls).singleElement().returns(LOCATION, id -> id.getLocation());
        assertThat(client.created)
                .singleElement()
                .returns(LOCATION, info -> info.getJobId().getLocation());
        // The configured location wins outright: no metadata round trip happens at all.
        assertThat(client.getDatasetCalls).isEmpty();
    }

    @Test
    void noConfiguredLocationDerivesTheJobLocationFromTheDestinationDataset() throws Exception {
        // A location-less id must not be built at all: a jobs.get naming no location resolves
        // against the US multi-region only, so on any other dataset the re-attach probe would
        // never find a previous attempt's job and the resubmission would collide (measured
        // 2026-08-10, #491).
        client.locatedDataset(DATASET, "europe-west1");
        client.answering(JobAnswer.absent());

        new BigQueryLoadJobRunner(client, null, FAST).submitLoad(JOB_ID, loadSpec(List.of()));

        assertThat(client.getDatasetCalls).containsExactly(DATASET);
        assertThat(client.getJobCalls)
                .singleElement()
                .returns("europe-west1", id -> id.getLocation());
        assertThat(client.created)
                .singleElement()
                .returns("europe-west1", info -> info.getJobId().getLocation());
    }

    @Test
    void theDatasetLocationIsDerivedOncePerDataset() throws Exception {
        client.locatedDataset(DATASET, "europe-west1");
        client.answering(JobAnswer.absent(), JobAnswer.absent());

        BigQueryLoadJobRunner runner = new BigQueryLoadJobRunner(client, null, FAST);
        runner.submitLoad(JOB_ID, loadSpec(List.of()));
        runner.submitLoad("bq-2", loadSpec(List.of()));

        assertThat(client.getDatasetCalls).containsExactly(DATASET);
        assertThat(client.created)
                .extracting(info -> info.getJobId().getLocation())
                .containsExactly("europe-west1", "europe-west1");
    }

    @Test
    void eachDestinationDatasetDerivesItsOwnJobLocation() throws Exception {
        // Dynamic destinations may route one committer's jobs to datasets in different regions;
        // each job runs where its own destination dataset lives, which no single configured
        // location could express.
        client.locatedDataset(DATASET, "europe-west1");
        client.locatedDataset(DatasetId.of("p", "d2"), "asia-northeast1");
        client.answering(JobAnswer.absent(), JobAnswer.absent());

        BigQueryLoadJobRunner runner = new BigQueryLoadJobRunner(client, null, FAST);
        runner.submitLoad(JOB_ID, loadSpecInto(DESTINATION));
        runner.submitLoad("bq-2", loadSpecInto(TableDestination.of("p", "d2", "t")));

        assertThat(client.created)
                .extracting(info -> info.getJobId().getLocation())
                .containsExactly("europe-west1", "asia-northeast1");
    }

    @Test
    void aCopyJobDerivesItsLocationFromItsDestinationDatasetNotItsSources() throws Exception {
        // The sources sit in a different dataset (as they do under FileLoadsOptions.tempDataset),
        // so a derivation that consulted a source dataset would be caught by both assertions.
        CopyJobSpec spec =
                new CopyJobSpec(
                        List.of(TableDestination.of("p", "d_tmp", "t_tmp")),
                        DESTINATION,
                        JobInfo.WriteDisposition.WRITE_TRUNCATE);
        client.locatedDataset(DATASET, "europe-west1");
        client.answering(JobAnswer.absent());

        new BigQueryLoadJobRunner(client, null, FAST).submitCopy(JOB_ID, spec);

        assertThat(client.getDatasetCalls).containsExactly(DATASET);
        assertThat(client.created)
                .singleElement()
                .returns("europe-west1", info -> info.getJobId().getLocation());
    }

    @Test
    void aFailedDatasetLookupFailsTheSubmissionNamingTheDataset() {
        client.getDatasetFailure = unavailable();

        BigQueryLoadJobRunner runner = new BigQueryLoadJobRunner(client, null, FAST);

        assertThatThrownBy(() -> runner.submitLoad(JOB_ID, loadSpec(List.of())))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Failed to look up dataset p.d")
                .hasCauseInstanceOf(BigQueryException.class);
        assertThat(client.created).isEmpty();
    }

    @Test
    void aMissingDatasetFailsTheSubmissionNamingTheDatasetAndBothCauses() {
        // Nothing scripted: the client answers null on a 404 — which BigQuery gives for a dataset
        // that does not exist AND for one this principal may not see, so the message must offer
        // both readings rather than assert non-existence.
        BigQueryLoadJobRunner runner = new BigQueryLoadJobRunner(client, null, FAST);

        assertThatThrownBy(() -> runner.submitLoad(JOB_ID, loadSpec(List.of())))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Dataset p.d was not found")
                .hasMessageContaining("bigquery.datasets.get");
        assertThat(client.created).isEmpty();
    }

    @Test
    void aLoadJobStagesAvroWithLogicalTypesAndBothDispositions() throws Exception {
        client.answering(JobAnswer.absent());

        runner().submitLoad(JOB_ID, loadSpec(List.of()));

        LoadJobConfiguration load = client.created.get(0).getConfiguration();
        assertThat(load.getFormat()).isEqualTo(FormatOptions.avro().getType());
        assertThat(load.getUseAvroLogicalTypes()).isTrue();
        assertThat(load.getSourceUris()).containsExactly("gs://bucket/a.avro");
        assertThat(load.getSchema()).isEqualTo(SCHEMA);
        assertThat(load.getDestinationTable()).isEqualTo(TableId.of("p", "d", "t"));
        assertThat(load.getCreateDisposition())
                .isEqualTo(JobInfo.CreateDisposition.CREATE_IF_NEEDED);
        assertThat(load.getWriteDisposition()).isEqualTo(JobInfo.WriteDisposition.WRITE_APPEND);
    }

    @Test
    void schemaUpdateOptionsAreSetOnlyWhenThereAreAny() throws Exception {
        client.answering(JobAnswer.absent(), JobAnswer.absent());

        BigQueryLoadJobRunner runner = runner();
        runner.submitLoad(JOB_ID, loadSpec(List.of()));
        runner.submitLoad(
                "bq-2", loadSpec(List.of(JobInfo.SchemaUpdateOption.ALLOW_FIELD_ADDITION)));

        assertThat(
                        client.created
                                .get(0)
                                .<LoadJobConfiguration>getConfiguration()
                                .getSchemaUpdateOptions())
                .isNull();
        assertThat(
                        client.created
                                .get(1)
                                .<LoadJobConfiguration>getConfiguration()
                                .getSchemaUpdateOptions())
                .containsExactly(JobInfo.SchemaUpdateOption.ALLOW_FIELD_ADDITION);
    }

    @Test
    void aCopyJobIsSubmittedWithCreateNeverAndTheGivenWriteDisposition() throws Exception {
        client.answering(JobAnswer.absent());

        runner().submitCopy(JOB_ID, copySpec());

        CopyJobConfiguration copy = client.created.get(0).getConfiguration();
        assertThat(copy.getSourceTables())
                .containsExactly(TableId.of("p", "d", "t_tmp"), TableId.of("p", "d", "t_tmp2"));
        assertThat(copy.getDestinationTable()).isEqualTo(TableId.of("p", "d", "t"));
        // The temporary tables are the only source of truth for the data, so a copy job must never
        // create the destination: it is reconciled before the load runs.
        assertThat(copy.getCreateDisposition()).isEqualTo(JobInfo.CreateDisposition.CREATE_NEVER);
        assertThat(copy.getWriteDisposition()).isEqualTo(JobInfo.WriteDisposition.WRITE_TRUNCATE);
    }

    @Test
    void avroLoadsCarryTheAvroFormatAndLogicalTypes() throws Exception {
        client.answering(JobAnswer.absent());

        runner().submitLoad(JOB_ID, loadSpec(List.of(), StagingFormat.AVRO));

        LoadJobConfiguration submitted = client.created.get(0).getConfiguration();
        assertThat(submitted.getFormat()).isEqualTo(FormatOptions.avro().getType());
        assertThat(submitted.getUseAvroLogicalTypes()).isTrue();
    }

    @Test
    void parquetLoadsEnableListInference() throws Exception {
        client.answering(JobAnswer.absent());

        runner().submitLoad(JOB_ID, loadSpec(List.of(), StagingFormat.PARQUET));

        LoadJobConfiguration submitted = client.created.get(0).getConfiguration();
        assertThat(submitted.getFormat()).isEqualTo(FormatOptions.parquet().getType());
        // Not a style preference. Without it a REPEATED column loads as an empty array and the job
        // reports success — measured against a provided schema naming the column STRING REPEATED,
        // which returned every row with zero elements. Nothing downstream would report it.
        assertThat(submitted.getParquetOptions()).isNotNull();
        assertThat(submitted.getParquetOptions().getEnableListInference()).isTrue();
        // useAvroLogicalTypes is meaningless here and must not travel with a Parquet load.
        assertThat(submitted.getUseAvroLogicalTypes()).isNull();
    }

    private static LoadJobSpec loadSpec(List<JobInfo.SchemaUpdateOption> schemaUpdateOptions) {
        return loadSpec(schemaUpdateOptions, StagingFormat.AVRO);
    }

    private static LoadJobSpec loadSpec(
            List<JobInfo.SchemaUpdateOption> schemaUpdateOptions, StagingFormat format) {
        return new LoadJobSpec(
                DESTINATION,
                List.of("gs://bucket/a.avro"),
                SCHEMA,
                JobInfo.CreateDisposition.CREATE_IF_NEEDED,
                JobInfo.WriteDisposition.WRITE_APPEND,
                schemaUpdateOptions,
                format);
    }

    private static LoadJobSpec loadSpecInto(TableDestination destination) {
        return new LoadJobSpec(
                destination,
                List.of("gs://bucket/a.avro"),
                SCHEMA,
                JobInfo.CreateDisposition.CREATE_IF_NEEDED,
                JobInfo.WriteDisposition.WRITE_APPEND,
                List.of(),
                StagingFormat.AVRO);
    }

    private static CopyJobSpec copySpec() {
        return new CopyJobSpec(
                List.of(TEMP, TableDestination.of("p", "d", "t_tmp2")),
                DESTINATION,
                JobInfo.WriteDisposition.WRITE_TRUNCATE);
    }

    /** A {@code DONE} status carrying an error, as a failed previous attempt reports. */
    private static JobStatus failed(String message) {
        return TestJobs.status(
                JobStatus.State.DONE, new BigQueryError("invalid", null, message), null);
    }

    private static BigQueryException conflict() {
        return new BigQueryException(HttpURLConnection.HTTP_CONFLICT, "already exists");
    }

    /** What a lookup answers once the client's own retries are exhausted. */
    private static BigQueryException unavailable() {
        return new BigQueryException(HttpURLConnection.HTTP_UNAVAILABLE, "backend error");
    }
}
