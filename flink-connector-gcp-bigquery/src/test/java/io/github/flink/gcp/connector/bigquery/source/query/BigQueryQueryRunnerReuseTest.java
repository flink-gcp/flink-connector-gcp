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

package io.github.flink.gcp.connector.bigquery.source.query;

import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.JobStatus;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TestJobs;
import io.github.flink.gcp.connector.bigquery.StubBigQuery;
import io.github.flink.gcp.connector.bigquery.StubBigQuery.JobAnswer;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.time.Duration;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The deterministic path: what is looked up in which order, what is reused, and what is probed
 * past.
 *
 * <p>Driven through a scripted {@link StubBigQuery}, because the behaviours under test — a job
 * found under a previous attempt's id, a conflict lost to a racing attempt, a failed id refused for
 * six months — are the service's, and the emulator answers none of them.
 */
class BigQueryQueryRunnerReuseTest {

    private static final String SQL = "SELECT id FROM `p.d.v`";
    private static final Duration WINDOW = Duration.ofHours(1);
    private static final TableId ANONYMOUS = TableId.of("p", "_anon1", "anon1");

    private static final JobStatus DONE = TestJobs.status(JobStatus.State.DONE);
    private static final JobStatus FAILED =
            TestJobs.status(
                    JobStatus.State.DONE,
                    new BigQueryError("invalid", "location", "previous attempt failed"),
                    Collections.emptyList());

    /** A specification carrying the identity, exactly as the enumerator builds one. */
    private static QuerySpec reusableSpec(String resultDataset) {
        QuerySpec spec = new QuerySpec(SQL, "p", null, resultDataset);
        return spec.withJobIdentity(
                QueryJobIdentity.of("pipeline", spec, WINDOW, System.currentTimeMillis()));
    }

    private static String currentId(QuerySpec spec) {
        return spec.getJobIdentity().getCurrentJobId();
    }

    private static String previousId(QuerySpec spec) {
        return spec.getJobIdentity().getPreviousJobId();
    }

    @Test
    void submitsUnderTheDeterministicIdWhenNothingExists() throws Exception {
        StubBigQuery client = new StubBigQuery();
        // The current id has no job, and neither does the previous window's: a fresh start.
        client.answering(JobAnswer.absent(), JobAnswer.absent());
        QuerySpec spec = reusableSpec("scratch");

        QueryResult result = new BigQueryQueryRunner(client).run(spec);

        assertThat(result.isReattached()).isFalse();
        assertThat(client.created).hasSize(1);
        assertThat(client.created.get(0).getJobId().getJob()).isEqualTo(currentId(spec));
        // The id doubles as the result table's name, so the two are found from each other.
        QueryJobConfiguration sent =
                (QueryJobConfiguration) client.created.get(0).getConfiguration();
        assertThat(sent.getDestinationTable().getTable()).isEqualTo(currentId(spec));
    }

    @Test
    void reattachesToTheCurrentWindowsJobInsteadOfSubmitting() throws Exception {
        StubBigQuery client = new StubBigQuery();
        client.answering(JobAnswer.withStatus(DONE));
        client.completedConfiguration =
                QueryJobConfiguration.newBuilder(SQL).setDestinationTable(ANONYMOUS).build();
        QuerySpec spec = reusableSpec(null);

        QueryResult result = new BigQueryQueryRunner(client).run(spec);

        assertThat(result.isReattached()).isTrue();
        assertThat(result.getTable().getTable()).isEqualTo("anon1");
        assertThat(client.created).isEmpty();
    }

    @Test
    void waitsForACurrentJobStillRunningRatherThanRacingIt() throws Exception {
        StubBigQuery client = new StubBigQuery();
        // Found running — the concurrent-scan case, where a second submission would double the
        // bill — then polled to completion.
        client.answering(
                JobAnswer.withStatus(TestJobs.status(JobStatus.State.RUNNING)),
                JobAnswer.withStatus(DONE));
        client.completedConfiguration =
                QueryJobConfiguration.newBuilder(SQL).setDestinationTable(ANONYMOUS).build();
        QuerySpec spec = reusableSpec(null);

        QueryResult result = new BigQueryQueryRunner(client).run(spec);

        assertThat(result.isReattached()).isTrue();
        assertThat(client.created).isEmpty();
        assertThat(client.getJobCalls).hasSize(2);
    }

    @Test
    void probesPastAFailedPreviousAttemptToAFreshRetryId() throws Exception {
        StubBigQuery client = new StubBigQuery();
        // The current id holds a failed job — BigQuery keeps that id for six months, so the only
        // way forward is the next retry id, which is free.
        client.answering(JobAnswer.withStatus(FAILED), JobAnswer.absent());
        QuerySpec spec = reusableSpec("scratch");

        QueryResult result = new BigQueryQueryRunner(client).run(spec);

        assertThat(result.isReattached()).isFalse();
        assertThat(client.created).hasSize(1);
        assertThat(client.created.get(0).getJobId().getJob()).isEqualTo(currentId(spec) + "_r1");
    }

    @Test
    void givesUpWhenEveryRetryIdHoldsAFailedJob() {
        StubBigQuery client = new StubBigQuery();
        JobAnswer[] allFailed = new JobAnswer[BigQueryQueryRunner.MAX_RETRY_PROBES + 1];
        for (int i = 0; i < allFailed.length; i++) {
            allFailed[i] = JobAnswer.withStatus(FAILED);
        }
        client.answering(allFailed);
        QuerySpec spec = reusableSpec("scratch");

        assertThatThrownBy(() -> new BigQueryQueryRunner(client).run(spec))
                .isInstanceOf(IOException.class)
                .hasMessageContaining(currentId(spec))
                .hasMessageContaining("all its retry ids failed")
                .hasMessageContaining("previous attempt failed");
        assertThat(client.created).isEmpty();
    }

    @Test
    void adoptsTheJobARacingAttemptSubmittedFirst() throws Exception {
        StubBigQuery client = new StubBigQuery();
        // Absent at look-up, conflicting at create: the failing-over coordinator's predecessor
        // won the race between the two calls. The conflict's job is adopted, and counted as a
        // reuse — the query is billed once either way.
        client.answering(JobAnswer.absent(), JobAnswer.absent(), JobAnswer.withStatus(DONE));
        client.createFailure = new BigQueryException(HttpURLConnection.HTTP_CONFLICT, "exists");
        client.completedConfiguration =
                QueryJobConfiguration.newBuilder(SQL).setDestinationTable(ANONYMOUS).build();
        QuerySpec spec = reusableSpec(null);

        QueryResult result = new BigQueryQueryRunner(client).run(spec);

        assertThat(result.isReattached()).isTrue();
    }

    @Test
    void aFailoverStraddlingTheWindowRolloverStillReuses() throws Exception {
        StubBigQuery client = new StubBigQuery();
        // Nothing under the current id — the bucket rolled between the first attempt and this
        // re-plan — but the previous window's id holds a healthy job created minutes ago.
        client.answering(
                JobAnswer.absent(),
                JobAnswer.withStatusCreatedAt(
                        DONE, System.currentTimeMillis() - Duration.ofMinutes(10).toMillis()));
        client.completedConfiguration =
                QueryJobConfiguration.newBuilder(SQL).setDestinationTable(ANONYMOUS).build();
        QuerySpec spec = reusableSpec(null);

        QueryResult result = new BigQueryQueryRunner(client).run(spec);

        assertThat(result.isReattached()).isTrue();
        assertThat(client.created).isEmpty();
        assertThat(client.getJobCalls.get(1).getJob()).isEqualTo(previousId(spec));
    }

    @Test
    void aPreviousWindowsJobOlderThanTheWindowIsNotReused() throws Exception {
        StubBigQuery client = new StubBigQuery();
        client.answering(
                JobAnswer.absent(),
                JobAnswer.withStatusCreatedAt(
                        DONE, System.currentTimeMillis() - WINDOW.toMillis() * 2));
        QuerySpec spec = reusableSpec("scratch");

        QueryResult result = new BigQueryQueryRunner(client).run(spec);

        // The documented window is exact: the id alone would have allowed up to twice it.
        assertThat(result.isReattached()).isFalse();
        assertThat(client.created).hasSize(1);
        assertThat(client.created.get(0).getJobId().getJob()).isEqualTo(currentId(spec));
    }

    @Test
    void aPreviousWindowsJobWithoutACreationTimeIsNotReused() throws Exception {
        StubBigQuery client = new StubBigQuery();
        // No statistics on the answer: age unknown, so the reuse — a cost optimisation — yields
        // to running the query again, which is always correct.
        client.answering(JobAnswer.absent(), JobAnswer.withStatus(DONE));
        QuerySpec spec = reusableSpec("scratch");

        QueryResult result = new BigQueryQueryRunner(client).run(spec);

        assertThat(result.isReattached()).isFalse();
        assertThat(client.created).hasSize(1);
    }

    @Test
    void walksThePreviousWindowsRetryChainPastItsFailedLinks() throws Exception {
        StubBigQuery client = new StubBigQuery();
        // The previous attempt probed past its own failed base id, so its live job sits at _r1.
        client.answering(
                JobAnswer.absent(),
                JobAnswer.withStatus(FAILED),
                JobAnswer.withStatusCreatedAt(
                        DONE, System.currentTimeMillis() - Duration.ofMinutes(10).toMillis()));
        client.completedConfiguration =
                QueryJobConfiguration.newBuilder(SQL).setDestinationTable(ANONYMOUS).build();
        QuerySpec spec = reusableSpec(null);

        QueryResult result = new BigQueryQueryRunner(client).run(spec);

        assertThat(result.isReattached()).isTrue();
        assertThat(client.getJobCalls.get(2).getJob()).isEqualTo(previousId(spec) + "_r1");
    }

    @Test
    void aStatuslessCurrentJobIsAttachedToAndPolledNotProbedPast() throws Exception {
        StubBigQuery client = new StubBigQuery();
        // The SDK's already-exists absorber answers a job with no status at all; probing past it
        // would abandon a job that may be running fine.
        client.answering(JobAnswer.withoutStatus(), JobAnswer.withStatus(DONE));
        client.completedConfiguration =
                QueryJobConfiguration.newBuilder(SQL).setDestinationTable(ANONYMOUS).build();
        QuerySpec spec = reusableSpec(null);

        QueryResult result = new BigQueryQueryRunner(client).run(spec);

        assertThat(result.isReattached()).isTrue();
        assertThat(client.created).isEmpty();
    }
}
