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

package io.github.flink.gcp.connector.bigquery.source.query;

import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.JobStatus;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TestJobs;
import io.github.flink.gcp.connector.bigquery.StubBigQuery;
import io.github.flink.gcp.connector.bigquery.StubBigQuery.TableAnswer;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * What the runner sends and what it makes of the answer, driven through a scripted {@link
 * StubBigQuery} rather than through BigQuery.
 *
 * <p>The gated real-GCP case is the authority on the service's behaviour — that a destination-less
 * query lands somewhere readable at all is BigQuery's to decide, and no stub can say so. What is
 * here is everything on this side of that: which job is submitted, which table is read back out of
 * it, and what each failure says.
 */
class BigQueryQueryRunnerTest {

    private static final String SQL = "SELECT id FROM `p.d.v`";
    private static final TableId ANONYMOUS = TableId.of("p", "_anon1", "anon1");

    @Test
    void submitsTheQueryWithNoDestinationAndReadsWhereBigQueryPutIt() throws Exception {
        StubBigQuery client = new StubBigQuery();
        // The half only the service does: the job comes back naming a table it was not asked for.
        client.completedConfiguration =
                QueryJobConfiguration.newBuilder(SQL).setDestinationTable(ANONYMOUS).build();

        QueryResult result =
                new BigQueryQueryRunner(client).run(new QuerySpec(SQL, "p", null, null));
        TableDestination landed = result.getTable();

        assertThat(landed).isEqualTo(TableDestination.of("p", "_anon1", "anon1"));
        // The random-id path never reuses anything, so it must never report a reattach.
        assertThat(result.isReattached()).isFalse();
        QueryJobConfiguration sent =
                (QueryJobConfiguration) client.created.get(0).getConfiguration();
        assertThat(sent.getQuery()).isEqualTo(SQL);
        // Not asking for a destination is what puts the result in the anonymous dataset, which is
        // the whole of the default path's cost argument.
        assertThat(sent.getDestinationTable()).isNull();
        assertThat(sent.getWriteDisposition()).isNull();
        assertThat(client.getTableCalls).isEmpty();
    }

    @Test
    void writesToTheNamedDatasetAndAsksToExpireWhatItCreated() throws Exception {
        StubBigQuery client = new StubBigQuery();

        TableDestination landed =
                new BigQueryQueryRunner(client)
                        .run(new QuerySpec(SQL, "p", null, "scratch"))
                        .getTable();

        QueryJobConfiguration sent =
                (QueryJobConfiguration) client.created.get(0).getConfiguration();
        assertThat(sent.getDestinationTable().getDataset()).isEqualTo("scratch");
        assertThat(sent.getDestinationTable().getProject()).isEqualTo("p");
        assertThat(sent.getWriteDisposition()).isEqualTo(JobInfo.WriteDisposition.WRITE_TRUNCATE);
        assertThat(landed.getDataset()).isEqualTo("scratch");
        // The expiration is the only cleanup this path has, so the attempt to set it is not
        // optional. The stub answers "gone", which is the branch that must not fail the job.
        assertThat(client.getTableCalls).singleElement().isEqualTo(sent.getDestinationTable());
    }

    @Test
    void givesTheJobAndTheTableItWroteTheSameName() throws Exception {
        StubBigQuery client = new StubBigQuery();

        new BigQueryQueryRunner(client).run(new QuerySpec(SQL, "p", null, "scratch"));

        QueryJobConfiguration sent =
                (QueryJobConfiguration) client.created.get(0).getConfiguration();
        // One string for both names, so a log line naming the job also finds the table in the
        // console.
        assertThat(client.created.get(0).getJobId().getJob())
                .isEqualTo(sent.getDestinationTable().getTable());
    }

    @Test
    void passesTheLocationOnWhenOneIsConfiguredAndLeavesItToBigQueryOtherwise() throws Exception {
        StubBigQuery pinned = landingAnonymously();
        new BigQueryQueryRunner(pinned).run(new QuerySpec(SQL, "p", "asia-northeast1", null));
        assertThat(pinned.created.get(0).getJobId().getLocation()).isEqualTo("asia-northeast1");

        StubBigQuery inferred = landingAnonymously();
        new BigQueryQueryRunner(inferred).run(new QuerySpec(SQL, "p", null, null));
        assertThat(inferred.created.get(0).getJobId().getLocation()).isNull();
    }

    @Test
    void pollsUntilTheJobIsDone() throws Exception {
        StubBigQuery client = new StubBigQuery();
        client.createdStatus = TestJobs.status(JobStatus.State.RUNNING);
        client.answering(
                StubBigQuery.JobAnswer.withStatus(TestJobs.status(JobStatus.State.RUNNING)),
                StubBigQuery.JobAnswer.withStatus(TestJobs.status(JobStatus.State.DONE)));

        new BigQueryQueryRunner(client).run(new QuerySpec(SQL, "p", null, "scratch"));

        assertThat(client.getJobCalls).hasSize(2);
    }

    @Test
    void reportsAFailedQueryWithTheQueryItRan() {
        StubBigQuery client = new StubBigQuery();
        client.createdStatus =
                TestJobs.status(
                        JobStatus.State.DONE,
                        new BigQueryError("invalidQuery", null, "Syntax error"),
                        null);

        assertThatThrownBy(
                        () ->
                                new BigQueryQueryRunner(client)
                                        .run(new QuerySpec(SQL, "p", null, null)))
                .isInstanceOf(java.io.IOException.class)
                .hasMessageContaining("Syntax error")
                // Without the query in it, a failed job leaves the reader with a job id and
                // nothing to read it against.
                .hasMessageContaining(SQL);
    }

    @Test
    void refusesToGuessWhenTheCompletedJobNamesNoTable() {
        StubBigQuery client = new StubBigQuery();
        // A completed query job always names its destination; if one ever did not, guessing a
        // table here would read something nobody asked for.
        client.completedConfiguration = QueryJobConfiguration.newBuilder(SQL).build();

        assertThatThrownBy(
                        () ->
                                new BigQueryQueryRunner(client)
                                        .run(new QuerySpec(SQL, "p", null, null)))
                .isInstanceOf(java.io.IOException.class)
                .hasMessageContaining("reported no result table");
    }

    @Test
    void countsBothViewKindsAsAViewAndNothingElse() {
        // The two the Storage Read API refuses as "non-table entities" (measured 2026-08-10).
        assertThat(BigQueryQueryRunner.isViewType(TableDefinition.Type.VIEW)).isTrue();
        assertThat(BigQueryQueryRunner.isViewType(TableDefinition.Type.MATERIALIZED_VIEW)).isTrue();

        // Not views, so not this feature's business: materializing them would answer a question
        // nobody asked. A snapshot the API reads directly; an external table it refuses for its own
        // reason, which a query job would not fix.
        assertThat(BigQueryQueryRunner.isViewType(TableDefinition.Type.TABLE)).isFalse();
        assertThat(BigQueryQueryRunner.isViewType(TableDefinition.Type.SNAPSHOT)).isFalse();
        assertThat(BigQueryQueryRunner.isViewType(TableDefinition.Type.EXTERNAL)).isFalse();
        assertThat(BigQueryQueryRunner.isViewType(TableDefinition.Type.MODEL)).isFalse();
        assertThat(BigQueryQueryRunner.isViewType(null)).isFalse();
    }

    @Test
    void reportsANameThatExistsNowhereRatherThanCallingItATable() {
        // The stub answers "gone", which is what the client maps a 404 to. Left to return false,
        // the read would fail later at session creation with a message about the wrong thing.
        StubBigQuery client = new StubBigQuery();

        assertThatThrownBy(
                        () ->
                                new BigQueryQueryRunner(client)
                                        .isView(TableDestination.of("p", "d", "gone")))
                .isInstanceOf(java.io.IOException.class)
                .hasMessageContaining("does not exist");
        assertThat(client.getTableCalls).singleElement().isEqualTo(TableId.of("p", "d", "gone"));
    }

    /** A client whose completed job names an anonymous table, as BigQuery's does. */
    private static StubBigQuery landingAnonymously() {
        StubBigQuery client = new StubBigQuery();
        client.completedConfiguration =
                QueryJobConfiguration.newBuilder(SQL).setDestinationTable(ANONYMOUS).build();
        return client;
    }

    @Test
    void reappliesTheExpirationOnAResultTableThatStillExists() throws Exception {
        StubBigQuery client = new StubBigQuery();
        client.tablesAnswering(TableAnswer.existing());
        long before = System.currentTimeMillis();

        new BigQueryQueryRunner(client).run(new QuerySpec(SQL, "p", null, "scratch"));

        // The backstop pushes the expiration out to a day from now — it never removes one, and it
        // never leaves the table permanent in the user's own dataset. The update must also aim at
        // the table the job wrote, not merely carry the right expiration.
        TableId destination =
                ((QueryJobConfiguration) client.created.get(0).getConfiguration())
                        .getDestinationTable();
        assertThat(client.updatedTables)
                .singleElement()
                .satisfies(
                        updated -> {
                            assertThat(updated.getTableId()).isEqualTo(destination);
                            assertThat(updated.getExpirationTime())
                                    .isBetween(
                                            before
                                                    + BigQueryQueryRunner.RESULT_TABLE_EXPIRATION
                                                            .toMillis(),
                                            System.currentTimeMillis()
                                                    + BigQueryQueryRunner.RESULT_TABLE_EXPIRATION
                                                            .toMillis());
                        });
    }

    @Test
    void aFailedExpirationBackstopDoesNotFailTheQuery() throws Exception {
        StubBigQuery client = new StubBigQuery();
        client.tablesAnswering(TableAnswer.existing());
        client.updateTableFailure = new BigQueryException(403, "tables.update was denied");

        TableDestination landed =
                new BigQueryQueryRunner(client)
                        .run(new QuerySpec(SQL, "p", null, "scratch"))
                        .getTable();

        // The table exists and is readable; failing the job here would turn a missing cleanup
        // backstop into a missing read.
        assertThat(landed.getDataset()).isEqualTo("scratch");
    }

    @Test
    void reportsAViewLookupFailureWithItsCause() {
        StubBigQuery client = new StubBigQuery();
        client.tablesAnswering(TableAnswer.failing(new BigQueryException(500, "backend error")));

        assertThatThrownBy(
                        () ->
                                new BigQueryQueryRunner(client)
                                        .isView(TableDestination.of("p", "d", "t")))
                .isInstanceOf(java.io.IOException.class)
                .hasMessageContaining("to see whether it is a view")
                .hasCauseInstanceOf(BigQueryException.class)
                .hasRootCauseMessage("backend error");
    }

    @Test
    void reportsAVanishedJobRatherThanWaitingForIt() {
        StubBigQuery client = new StubBigQuery();
        client.createdStatus = TestJobs.status(JobStatus.State.RUNNING);
        client.answering(StubBigQuery.JobAnswer.absent());

        assertThatThrownBy(
                        () ->
                                new BigQueryQueryRunner(client)
                                        .run(new QuerySpec(SQL, "p", null, null)))
                .isInstanceOf(java.io.IOException.class)
                .hasMessageContaining("disappeared");
    }
}
