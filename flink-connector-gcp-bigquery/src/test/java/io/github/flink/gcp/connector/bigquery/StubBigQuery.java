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

package io.github.flink.gcp.connector.bigquery;

import com.google.api.gax.paging.Page;
import com.google.cloud.NoCredentials;
import com.google.cloud.Policy;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Connection;
import com.google.cloud.bigquery.ConnectionSettings;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.InsertAllResponse;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobConfiguration;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.JobStatus;
import com.google.cloud.bigquery.Model;
import com.google.cloud.bigquery.ModelId;
import com.google.cloud.bigquery.ModelInfo;
import com.google.cloud.bigquery.Project;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.QueryResponse;
import com.google.cloud.bigquery.Routine;
import com.google.cloud.bigquery.RoutineId;
import com.google.cloud.bigquery.RoutineInfo;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableDataWriteChannel;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.bigquery.TestJobs;
import com.google.cloud.bigquery.WriteChannelConfiguration;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.loadjob.BigQueryLoadJobRunner;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The parts of {@link BigQuery} this module's REST callers read, with everything else unsupported —
 * so a new dependency on the client shows up as a failing test rather than as a silent null.
 *
 * <p>Four methods are live ({@link #getJob}, {@link #create(JobInfo, JobOption...)}, {@link
 * #create(TableInfo, TableOption...)}, {@link #delete(TableId)}), plus {@link #getOptions()}, which
 * no caller invokes itself — {@link Job}'s constructor does, so a stub throwing there fails on the
 * first submitted job. The other 50 throw.
 *
 * <p>It lives here, beside {@link RealBigQuery}, rather than in one caller's package, because it
 * has two consumers: {@link BigQueryLoadJobRunner}'s tests, which it was written for, and {@code
 * BigQueryTableAdmin}'s, which need a failing {@code create(TableInfo)} to pin how a REST failure
 * is typed. A third consumer earns its methods here the same way.
 *
 * <p>{@code getJob} answers <em>positionally</em> — the first call takes the first scripted answer
 * — because the runner's calls are a sequence, not a lookup: a submit probes {@code base}, {@code
 * base-r1}, ... and an await polls the same id repeatedly. Running past the script is a failure
 * naming the call, which turns "polled once more than the test expected" into a named assertion
 * failure instead of an unscripted null. Each answered job is stamped with the id it was
 * <em>asked</em> for, as the service does.
 */
public final class StubBigQuery implements BigQuery {

    /** Every {@link JobId} {@code getJob} was called with, in order. */
    public final List<JobId> getJobCalls = new ArrayList<>();

    /** What {@code getJob} answers, one entry per call. */
    private final List<JobAnswer> getJobAnswers = new ArrayList<>();

    /** Every {@link JobInfo} {@code create} was called with, in order. */
    public final List<JobInfo> created = new ArrayList<>();

    /** Every {@link TableId} {@code delete} was called with, in order. */
    public final List<TableId> deleted = new ArrayList<>();

    /**
     * The status a created job reports; {@code null} for the statusless job the SDK's own
     * already-exists absorber hands back (it re-fetches with fields that exclude the status).
     */
    @Nullable public JobStatus createdStatus = TestJobs.status(JobStatus.State.DONE);

    /** Thrown by the next {@code create} call when set, and consumed by it: later calls succeed. */
    @Nullable public BigQueryException createFailure;

    /** Every {@link TableInfo} {@code create(TableInfo)} was called with, in order. */
    public final List<TableInfo> createdTables = new ArrayList<>();

    /** Thrown by the next {@code create(TableInfo)} call, and consumed by it. */
    @Nullable public BigQueryException createTableFailure;

    /** Thrown by {@code delete} when set. */
    @Nullable public RuntimeException deleteFailure;

    /** Every {@link TableId} {@code getTable} was called with, in order. */
    public final List<TableId> getTableCalls = new ArrayList<>();

    /**
     * The configuration a minted job reports, instead of the one it was submitted with.
     *
     * <p>This models the half of a query job only the service can do: a query submitted with no
     * destination table comes back reporting the anonymous table BigQuery chose for it. Left {@code
     * null}, a job reports what it was created with.
     */
    @Nullable public JobConfiguration completedConfiguration;

    /** The configuration each created job was submitted with, keyed by job name. */
    private final Map<String, JobConfiguration> submittedConfigurations = new HashMap<>();

    /**
     * The options {@link Job}'s constructor reads.
     *
     * <p>Both setters are load-bearing, for different reasons. Without the project id {@link
     * BigQueryOptions} refuses to build unless it can determine one — the reason already recorded
     * on {@code BigQueryTableAdmin.emulatorOptions}. Without the credentials the build runs an
     * application-default-credentials lookup; measured 2026-08-08 against google-cloud-core 2.72.0,
     * {@code ServiceOptions.defaultCredentials()} catches every exception and answers {@code null},
     * so that lookup would not fail the test — it would make it read the environment (a credentials
     * file, or a metadata-server probe with its timeout) for a value nothing here uses.
     */
    private final BigQueryOptions options =
            BigQueryOptions.newBuilder()
                    .setProjectId("stub-project")
                    .setCredentials(NoCredentials.getInstance())
                    .build();

    /** What a scripted {@code getJob} call answers. */
    public static final class JobAnswer {

        @Nullable private final JobStatus status;
        private final boolean present;
        @Nullable private final BigQueryException failure;
        @Nullable private final Long creationTimeMillis;

        private JobAnswer(
                @Nullable JobStatus status, boolean present, @Nullable BigQueryException failure) {
            this(status, present, failure, null);
        }

        private JobAnswer(
                @Nullable JobStatus status,
                boolean present,
                @Nullable BigQueryException failure,
                @Nullable Long creationTimeMillis) {
            this.status = status;
            this.present = present;
            this.failure = failure;
            this.creationTimeMillis = creationTimeMillis;
        }

        /** Answers that no job exists under the id asked for. */
        public static JobAnswer absent() {
            return new JobAnswer(null, false, null);
        }

        /** Answers with a job in the given status. */
        public static JobAnswer withStatus(JobStatus status) {
            return new JobAnswer(status, true, null);
        }

        /** Answers with a job whose status the response did not carry. */
        public static JobAnswer withoutStatus() {
            return new JobAnswer(null, true, null);
        }

        /**
         * Answers with a job that also reports when it was created, which is what the query
         * runner's previous-window reuse reads. The plain {@link #withStatus} answer models a
         * response with no statistics, which that path treats as "do not reuse".
         */
        public static JobAnswer withStatusCreatedAt(JobStatus status, long creationTimeMillis) {
            return new JobAnswer(status, true, null, creationTimeMillis);
        }

        /** Fails the lookup, as the client does once its own retries are exhausted. */
        public static JobAnswer failing(BigQueryException failure) {
            return new JobAnswer(null, false, failure);
        }
    }

    /** Scripts the answers {@code getJob} gives, in call order. */
    public void answering(JobAnswer... answers) {
        getJobAnswers.addAll(List.of(answers));
    }

    @Override
    public BigQueryOptions getOptions() {
        return options;
    }

    @Override
    @Nullable
    public Job getJob(JobId jobId, JobOption... options) {
        noOptions(options);
        getJobCalls.add(jobId);
        int call = getJobCalls.size() - 1;
        if (call >= getJobAnswers.size()) {
            throw new UnsupportedOperationException(
                    "getJob call "
                            + (call + 1)
                            + " (for "
                            + jobId.getJob()
                            + ") is past the end of the script.");
        }
        JobAnswer answer = getJobAnswers.get(call);
        if (answer.failure != null) {
            throw answer.failure;
        }
        return answer.present
                ? TestJobs.job(
                        this,
                        jobId,
                        answer.status,
                        configurationOf(jobId),
                        answer.creationTimeMillis)
                : null;
    }

    @Override
    public Job create(JobInfo jobInfo, JobOption... options) {
        noOptions(options);
        created.add(jobInfo);
        if (createFailure != null) {
            BigQueryException failure = createFailure;
            createFailure = null;
            throw failure;
        }
        submittedConfigurations.put(jobInfo.getJobId().getJob(), jobInfo.getConfiguration());
        return TestJobs.job(
                this, jobInfo.getJobId(), createdStatus, configurationOf(jobInfo.getJobId()));
    }

    /**
     * What a job under this id reports as its configuration, or {@code null} for the placeholder a
     * caller that never reads one back gets — which is every caller but the query runner.
     */
    @Nullable
    private JobConfiguration configurationOf(JobId jobId) {
        return completedConfiguration != null
                ? completedConfiguration
                : submittedConfigurations.get(jobId.getJob());
    }

    @Override
    public boolean delete(TableId tableId) {
        deleted.add(tableId);
        if (deleteFailure != null) {
            throw deleteFailure;
        }
        return true;
    }

    /**
     * Rejects a request narrowed by {@link JobOption}s, which the runner passes none of.
     *
     * <p>Not fussiness: {@code JobField.REQUIRED_FIELDS} is the job reference and the configuration
     * alone, so a poll narrowed with {@code JobOption.fields(...)} would come back with no status —
     * and {@code awaitJob}, which has no attempt bound by design, would wait for a job it can never
     * see finish. Scripting an answer for such a call would hide that.
     */
    private static void noOptions(JobOption... options) {
        if (options.length > 0) {
            throw new UnsupportedOperationException(
                    "BigQueryLoadJobRunner passes no JobOptions; got " + List.of(options) + ".");
        }
    }

    private static UnsupportedOperationException unsupported(String call) {
        return new UnsupportedOperationException(
                "BigQueryLoadJobRunner has no reason to call " + call + ".");
    }

    @Override
    public Dataset create(DatasetInfo datasetInfo, DatasetOption... options) {
        throw unsupported("create(DatasetInfo)");
    }

    @Override
    public Table create(TableInfo tableInfo, TableOption... options) {
        createdTables.add(tableInfo);
        if (createTableFailure == null) {
            // Deliberately not a return: a successful creation would have to hand back a Table,
            // which the SDK lets nobody construct (docs/adr/0067), and no caller reads the value.
            // Every test here scripts a failure, so reaching this is a test that forgot to.
            throw unsupported("a successful create(TableInfo)");
        }
        BigQueryException failure = createTableFailure;
        createTableFailure = null;
        throw failure;
    }

    @Override
    public Routine create(RoutineInfo routineInfo, RoutineOption... options) {
        throw unsupported("create(RoutineInfo)");
    }

    @Override
    public Connection createConnection(ConnectionSettings connectionSettings) {
        throw unsupported("createConnection(ConnectionSettings)");
    }

    @Override
    public Connection createConnection() {
        throw unsupported("createConnection()");
    }

    @Override
    public Dataset getDataset(String datasetId, DatasetOption... options) {
        throw unsupported("getDataset(String)");
    }

    @Override
    public Dataset getDataset(DatasetId datasetId, DatasetOption... options) {
        throw unsupported("getDataset(DatasetId)");
    }

    @Override
    public Page<Dataset> listDatasets(DatasetListOption... options) {
        throw unsupported("listDatasets()");
    }

    @Override
    public Page<Project> listProjects(ProjectListOption... options) {
        throw unsupported("listProjects()");
    }

    @Override
    public Page<Dataset> listDatasets(String projectId, DatasetListOption... options) {
        throw unsupported("listDatasets(String)");
    }

    @Override
    public boolean delete(String datasetId, DatasetDeleteOption... options) {
        throw unsupported("delete(String)");
    }

    @Override
    public boolean delete(DatasetId datasetId, DatasetDeleteOption... options) {
        throw unsupported("delete(DatasetId)");
    }

    @Override
    public boolean delete(String datasetId, String tableId) {
        throw unsupported("delete(String, String)");
    }

    @Override
    public boolean delete(ModelId modelId) {
        throw unsupported("delete(ModelId)");
    }

    @Override
    public boolean delete(RoutineId routineId) {
        throw unsupported("delete(RoutineId)");
    }

    @Override
    public boolean delete(JobId jobId) {
        throw unsupported("delete(JobId)");
    }

    @Override
    public Dataset update(DatasetInfo datasetInfo, DatasetOption... options) {
        throw unsupported("update(DatasetInfo)");
    }

    @Override
    public Table update(TableInfo tableInfo, TableOption... options) {
        throw unsupported("update(TableInfo)");
    }

    @Override
    public Model update(ModelInfo modelInfo, ModelOption... options) {
        throw unsupported("update(ModelInfo)");
    }

    @Override
    public Routine update(RoutineInfo routineInfo, RoutineOption... options) {
        throw unsupported("update(RoutineInfo)");
    }

    @Override
    public Table getTable(String datasetId, String tableId, TableOption... options) {
        throw unsupported("getTable(String, String)");
    }

    @Override
    @Nullable
    public Table getTable(TableId tableId, TableOption... options) {
        // Records the call and answers "gone", which is the one answer this stub can give: a Table
        // has no constructor reachable from here, and minting one would need a second helper in the
        // vendor's own package — a decision `docs/adr/0067` asks to be taken deliberately rather
        // than reached for. What that covers is the query runner asking to expire the table it
        // created, and the branch where the table is no longer there; the update itself is the
        // gated real-GCP case's.
        getTableCalls.add(tableId);
        return null;
    }

    @Override
    public Model getModel(String datasetId, String modelId, ModelOption... options) {
        throw unsupported("getModel(String, String)");
    }

    @Override
    public Model getModel(ModelId modelId, ModelOption... options) {
        throw unsupported("getModel(ModelId)");
    }

    @Override
    public Routine getRoutine(String datasetId, String routineId, RoutineOption... options) {
        throw unsupported("getRoutine(String, String)");
    }

    @Override
    public Routine getRoutine(RoutineId routineId, RoutineOption... options) {
        throw unsupported("getRoutine(RoutineId)");
    }

    @Override
    public Page<Routine> listRoutines(String datasetId, RoutineListOption... options) {
        throw unsupported("listRoutines(String)");
    }

    @Override
    public Page<Routine> listRoutines(DatasetId datasetId, RoutineListOption... options) {
        throw unsupported("listRoutines(DatasetId)");
    }

    @Override
    public Page<Table> listTables(String datasetId, TableListOption... options) {
        throw unsupported("listTables(String)");
    }

    @Override
    public Page<Table> listTables(DatasetId datasetId, TableListOption... options) {
        throw unsupported("listTables(DatasetId)");
    }

    @Override
    public Page<Model> listModels(String datasetId, ModelListOption... options) {
        throw unsupported("listModels(String)");
    }

    @Override
    public Page<Model> listModels(DatasetId datasetId, ModelListOption... options) {
        throw unsupported("listModels(DatasetId)");
    }

    @Override
    public List<String> listPartitions(TableId tableId) {
        throw unsupported("listPartitions(TableId)");
    }

    @Override
    public InsertAllResponse insertAll(InsertAllRequest request) {
        throw unsupported("insertAll(InsertAllRequest)");
    }

    @Override
    public TableResult listTableData(
            String datasetId, String tableId, TableDataListOption... options) {
        throw unsupported("listTableData(String, String)");
    }

    @Override
    public TableResult listTableData(TableId tableId, TableDataListOption... options) {
        throw unsupported("listTableData(TableId)");
    }

    @Override
    public TableResult listTableData(
            String datasetId, String tableId, Schema schema, TableDataListOption... options) {
        throw unsupported("listTableData(String, String, Schema)");
    }

    @Override
    public TableResult listTableData(
            TableId tableId, Schema schema, TableDataListOption... options) {
        throw unsupported("listTableData(TableId, Schema)");
    }

    @Override
    public Job getJob(String jobId, JobOption... options) {
        throw unsupported("getJob(String)");
    }

    @Override
    public Page<Job> listJobs(JobListOption... options) {
        throw unsupported("listJobs()");
    }

    @Override
    public boolean cancel(String jobId) {
        throw unsupported("cancel(String)");
    }

    @Override
    public boolean cancel(JobId jobId) {
        throw unsupported("cancel(JobId)");
    }

    @Override
    public TableResult query(QueryJobConfiguration configuration, JobOption... options) {
        throw unsupported("query(QueryJobConfiguration)");
    }

    @Override
    public TableResult query(
            QueryJobConfiguration configuration, JobId jobId, JobOption... options) {
        throw unsupported("query(QueryJobConfiguration, JobId)");
    }

    @Override
    public Object queryWithTimeout(
            QueryJobConfiguration configuration,
            JobId jobId,
            Long timeoutMs,
            JobOption... options) {
        throw unsupported("queryWithTimeout(QueryJobConfiguration, JobId, Long)");
    }

    @Override
    public QueryResponse getQueryResults(JobId jobId, QueryResultsOption... options) {
        throw unsupported("getQueryResults(JobId)");
    }

    @Override
    public TableDataWriteChannel writer(WriteChannelConfiguration writeChannelConfiguration) {
        throw unsupported("writer(WriteChannelConfiguration)");
    }

    @Override
    public TableDataWriteChannel writer(
            JobId jobId, WriteChannelConfiguration writeChannelConfiguration) {
        throw unsupported("writer(JobId, WriteChannelConfiguration)");
    }

    @Override
    public Policy getIamPolicy(TableId tableId, IAMOption... options) {
        throw unsupported("getIamPolicy(TableId)");
    }

    @Override
    public Policy setIamPolicy(TableId tableId, Policy policy, IAMOption... options) {
        throw unsupported("setIamPolicy(TableId, Policy)");
    }

    @Override
    public List<String> testIamPermissions(
            TableId table, List<String> permissions, IAMOption... options) {
        throw unsupported("testIamPermissions(TableId, List)");
    }
}
