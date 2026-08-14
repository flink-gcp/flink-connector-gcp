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

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.metrics.Counter;
import org.apache.flink.util.StringUtils;

import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import io.github.flink.gcp.connector.base.retry.Retries;
import io.github.flink.gcp.connector.base.retry.RetrySchedule;
import io.github.flink.gcp.connector.bigquery.sink.BigQuerySinkConfig;
import io.github.flink.gcp.connector.bigquery.sink.CreateDisposition;
import io.github.flink.gcp.connector.bigquery.sink.SchemaUpdateOptions;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.WriteDisposition;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.FileLoadsCommittable;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.FileLoadsOptions;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.StagingFormat;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.writer.StagingStorage;
import io.github.flink.gcp.connector.bigquery.sink.tables.SchemaUnifier;
import io.github.flink.gcp.connector.bigquery.sink.tables.StorageSchemaConverter;
import io.github.flink.gcp.connector.bigquery.sink.tables.TableAdmin;
import io.github.flink.gcp.connector.bigquery.sink.tables.TableSchemaSnapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Function;

/**
 * Turns the staged files of one run — a whole batch job, or one checkpoint of a streaming job —
 * into BigQuery load jobs: groups files per destination table, bin-packs each table's files against
 * the per-load-job limits, and executes the resulting jobs through a {@link LoadJobRunner}.
 *
 * <p><b>Every load consults the live table first.</b> The destination is reconciled through one
 * shared decision ({@link #ensureFinalTable}, memoized once per destination per run): a missing
 * table is created via {@link TableAdmin} with the configured partitioning/clustering, and — gated
 * by {@link SchemaUpdateOptions} — the live schema is unioned with the serializer's via {@link
 * SchemaUnifier}, which demotes a new {@code REQUIRED} column to {@code NULLABLE} because BigQuery
 * cannot add {@code REQUIRED} columns to an existing table. A load job supplying an unreconciled
 * schema would be rejected at submission for exactly that case, and whether a run fits one
 * partition must not decide whether its records load.
 *
 * <p><b>Common case — one partition in one format.</b> A table whose files share a format and fit
 * one load job is loaded directly: the job carries the reconciled schema, the configured
 * disposition, and — belt-and-braces against external mid-run schema changes — the native {@code
 * ALLOW_FIELD_ADDITION}/{@code ALLOW_FIELD_RELAXATION} schema update options on {@code
 * WRITE_APPEND} and {@code WRITE_TRUNCATE_DATA} jobs. Multiple fitting formats also stay direct for
 * append/empty dispositions, while replacement dispositions combine them as described below.
 *
 * <p><b>Temporary tables plus a copy hierarchy.</b> If any staging format for a table exceeds the
 * limits, or replacement rows span formats, every format for that table is loaded
 * partition-by-partition into temporary tables ({@code WRITE_TRUNCATE} + {@code CREATE_IF_NEEDED},
 * so a retried partition load is idempotent). At most 1,200 sources feed one copy job. A larger
 * source set is reduced through deterministic intermediate tables. An ordinary disposition then
 * uses one final copy. Because a copy job cannot use {@code WRITE_TRUNCATE_DATA}, that disposition
 * first copies into one aggregate temporary table and then atomically replaces the final table's
 * data with a terminal query job. Copy jobs support no schema update options and require matching
 * schemas, so every leaf is loaded with the reconciled schema and every intermediate inherits it.
 * Streaming temporary-table names include the checkpoint id so consecutive checkpoints do not
 * collide.
 *
 * <p><b>Concurrency.</b> Independent jobs are submitted first and awaited second in deterministic
 * waves of at most 50,000 submissions, within BigQuery's per-project, per-region pending-job limit.
 * Interactive terminal queries use waves of at most 1,000, within their narrower queued-query
 * limit. Copy levels are barriers: a level is complete before a job that reads its tables is
 * submitted. BigQuery runs each wave concurrently server-side, so no thread pool is needed.
 *
 * <p><b>Determinism and retries.</b> Files are sorted by URI before bin-packing, so a retried run
 * over the same committables produces identical partitions, temporary table names and job ids
 * (hashes over destination and source URIs; streaming ids additionally carry a visible {@code
 * -c<checkpointId>} segment for attribution), letting the {@link LoadJobRunner} re-attach instead
 * of double-loading. On success staged files are deleted best-effort; on failure everything is
 * deliberately left in place for the retry (temporary tables rely on the temp dataset's expiration,
 * staging objects on the bucket's lifecycle rule).
 */
@Internal
public final class LoadJobOrchestrator {

    // BigQuery admits at most 1,000 queued interactive queries per project and region.
    private static final int MAX_QUERY_SUBMISSIONS_PER_WAVE = 1_000;

    private static final Logger LOG = LoggerFactory.getLogger(LoadJobOrchestrator.class);

    /** BigQuery's per-load-job source URI limit. */
    @VisibleForTesting static final int MAX_FILES_PER_JOB = 10_000;

    /** Per-load-job byte budget: 11 TiB, a safety margin under BigQuery's 15 TB limit. */
    @VisibleForTesting static final long MAX_BYTES_PER_JOB = 11L * (1L << 40);

    /** BigQuery's maximum source-table count for one copy job. */
    @VisibleForTesting static final int MAX_SOURCE_TABLES_PER_COPY = 1_200;

    /** BigQuery's project-wide daily quota for each of load and copy jobs. */
    @VisibleForTesting static final int MAX_JOBS_PER_COMMIT = 100_000;

    /** BigQuery's maximum pending jobs per project and region. */
    @VisibleForTesting static final int MAX_SUBMISSIONS_PER_WAVE = 50_000;

    private final BigQuerySinkConfig<?> config;
    private final FileLoadsOptions options;
    private final RetrySchedule schemaReconcileSchedule;
    private final LoadJobRunner runner;
    private final TableAdmin tableAdmin;
    private final StagingStorage storage;
    private final String flinkJobId;
    @Nullable private final Long checkpointId;
    private final Counter loadJobsSubmitted;
    private final Limits limits;

    /** Per-run memo of {@link #ensureFinalTable}; see {@link #finalTableSchema}. */
    private final Map<TableDestination, Schema> finalTableSchemas = new HashMap<>();

    /**
     * Creates an orchestrator.
     *
     * @param config the sink configuration
     * @param options the FILE_LOADS options
     * @param runner the job runner
     * @param tableAdmin the table admin (pre-load table creation and schema reconciliation)
     * @param storage the staging storage (post-load cleanup)
     * @param flinkJobId the Flink job id (hex), scoping temporary table names and job ids
     * @param checkpointId the checkpoint whose files this run loads, or {@code null} for a batch
     *     run; a non-null id scopes streaming job ids and temporary-table names
     * @param loadJobsSubmitted the committer's load-job counter. Passed as the counter rather than
     *     as a metric group because this type is constructed once per commit, while the metric it
     *     feeds is registered once per committer
     */
    public LoadJobOrchestrator(
            BigQuerySinkConfig<?> config,
            FileLoadsOptions options,
            LoadJobRunner runner,
            TableAdmin tableAdmin,
            StagingStorage storage,
            String flinkJobId,
            @Nullable Long checkpointId,
            Counter loadJobsSubmitted) {
        this(
                config,
                options,
                runner,
                tableAdmin,
                storage,
                flinkJobId,
                checkpointId,
                loadJobsSubmitted,
                Limits.BIGQUERY);
    }

    @VisibleForTesting
    LoadJobOrchestrator(
            BigQuerySinkConfig<?> config,
            FileLoadsOptions options,
            LoadJobRunner runner,
            TableAdmin tableAdmin,
            StagingStorage storage,
            String flinkJobId,
            @Nullable Long checkpointId,
            Counter loadJobsSubmitted,
            Limits limits) {
        this.config = config;
        this.options = options;
        this.schemaReconcileSchedule = options.toSchemaReconcileSchedule();
        this.runner = runner;
        this.tableAdmin = tableAdmin;
        this.storage = storage;
        this.flinkJobId = flinkJobId;
        this.loadJobsSubmitted = loadJobsSubmitted;
        this.checkpointId = checkpointId;
        this.limits = limits;
    }

    /**
     * Loads all staged files of the run into their destination tables.
     *
     * @param committables the staged files
     * @throws IOException if any load, copy, or terminal query fails; staged files are left in
     *     place
     */
    public void run(List<FileLoadsCommittable> committables) throws IOException {
        if (committables.isEmpty()) {
            LOG.info("No staged files; nothing to load");
            return;
        }
        CommitPlan plan = planCommit(committables);

        runInWaves(plan.loads, this::submitLoad, load -> load.jobId);
        for (int level = 0; level < plan.intermediateLevelCount; level++) {
            List<PlannedCopy> jobs = new ArrayList<>();
            for (DestinationCopy copy : plan.copies) {
                if (level < copy.intermediateLevels.size()) {
                    jobs.addAll(copy.intermediateLevels.get(level));
                }
            }
            runInWaves(jobs, this::submitCopy, copy -> copy.jobId);
        }
        List<PlannedCopy> finalCopies = new ArrayList<>(plan.copies.size());
        for (DestinationCopy copy : plan.copies) {
            finalCopies.add(copy.finalCopy);
        }
        runInWaves(finalCopies, this::submitCopy, copy -> copy.jobId);

        List<PlannedQuery> terminalQueries = new ArrayList<>();
        for (DestinationCopy copy : plan.copies) {
            if (copy.terminalQuery != null) {
                terminalQueries.add(copy.terminalQuery);
            }
        }
        runInWaves(
                terminalQueries,
                this::submitQuery,
                query -> query.jobId,
                Math.min(limits.maxSubmissionsPerWave, MAX_QUERY_SUBMISSIONS_PER_WAVE));

        for (DestinationCopy copy : plan.copies) {
            for (TableDestination tempTable : copy.cleanupTables) {
                runner.deleteTable(tempTable);
            }
        }

        List<String> uris = new ArrayList<>(committables.size());
        long rows = 0;
        for (FileLoadsCommittable committable : committables) {
            uris.add(committable.getUri());
            rows += committable.getRowCount();
        }
        storage.deleteObjects(uris);
        LOG.info(
                "Loaded {} rows from {} staged files into {} tables{}",
                rows,
                committables.size(),
                plan.destinationCount,
                checkpointId != null ? " for checkpoint " + checkpointId : "");
    }

    /** Builds and validates every load, copy level, terminal query and cleanup target first. */
    private CommitPlan planCommit(List<FileLoadsCommittable> committables) throws IOException {
        List<DestinationLoad> destinationLoads = plan(committables);
        Map<TableDestination, Integer> formatCounts = new HashMap<>();
        Set<TableDestination> overflowingDestinations = new HashSet<>();
        for (DestinationLoad load : destinationLoads) {
            formatCounts.merge(load.destination, 1, Integer::sum);
            if (load.partitions.size() > 1) {
                overflowingDestinations.add(load.destination);
            }
        }

        List<PlannedLoad> loads = new ArrayList<>();
        Map<TableDestination, List<TableDestination>> copySources = new LinkedHashMap<>();
        for (DestinationLoad load : destinationLoads) {
            boolean replacementAcrossFormats =
                    isReplacementDisposition() && formatCounts.get(load.destination) > 1;
            boolean useTempTables =
                    overflowingDestinations.contains(load.destination) || replacementAcrossFormats;
            for (int partitionIndex = 0;
                    partitionIndex < load.partitions.size();
                    partitionIndex++) {
                List<FileLoadsCommittable> partition = load.partitions.get(partitionIndex);
                List<String> uris = urisOf(partition);
                if (useTempTables) {
                    TableDestination tempTable =
                            tempTable(
                                    load.destination,
                                    load.format,
                                    formatCounts.get(load.destination) > 1,
                                    partitionIndex);
                    loads.add(
                            new PlannedLoad(
                                    load.destination,
                                    tempTable,
                                    load.format,
                                    uris,
                                    jobId(
                                            "flink-bq-load",
                                            load.destination,
                                            uris,
                                            "p" + partitionIndex),
                                    false));
                    copySources
                            .computeIfAbsent(load.destination, unused -> new ArrayList<>())
                            .add(tempTable);
                } else {
                    loads.add(
                            new PlannedLoad(
                                    load.destination,
                                    load.destination,
                                    load.format,
                                    uris,
                                    jobId("flink-bq-load", load.destination, uris, null),
                                    true));
                }
            }
        }

        List<DestinationCopy> copies = new ArrayList<>(copySources.size());
        int intermediateLevelCount = 0;
        long copyJobCount = 0;
        for (Map.Entry<TableDestination, List<TableDestination>> entry : copySources.entrySet()) {
            DestinationCopy copy = planCopy(entry.getKey(), entry.getValue());
            copies.add(copy);
            intermediateLevelCount =
                    Math.max(intermediateLevelCount, copy.intermediateLevels.size());
            copyJobCount += copy.jobCount();
        }
        validateJobCounts(loads.size(), copyJobCount, limits);
        return new CommitPlan(loads, copies, intermediateLevelCount, formatCounts.size());
    }

    private DestinationCopy planCopy(
            TableDestination destination, List<TableDestination> leafTables) {
        List<TableDestination> cleanupTables = new ArrayList<>(leafTables);
        List<List<PlannedCopy>> intermediateLevels = new ArrayList<>();
        List<TableDestination> sources = new ArrayList<>(leafTables);
        int level = 1;
        while (sources.size() > limits.maxSourceTablesPerCopy) {
            List<PlannedCopy> jobs = new ArrayList<>();
            List<TableDestination> nextSources = new ArrayList<>();
            for (int start = 0, group = 0;
                    start < sources.size();
                    start += limits.maxSourceTablesPerCopy, group++) {
                int end = Math.min(start + limits.maxSourceTablesPerCopy, sources.size());
                List<TableDestination> groupSources = List.copyOf(sources.subList(start, end));
                if (groupSources.size() == 1) {
                    nextSources.add(groupSources.get(0));
                    continue;
                }
                TableDestination intermediate =
                        intermediateTable(destination, groupSources, level, group);
                cleanupTables.add(intermediate);
                nextSources.add(intermediate);
                jobs.add(
                        new PlannedCopy(
                                jobId(
                                        "flink-bq-copy",
                                        intermediate,
                                        tablePaths(groupSources),
                                        "l" + level + "g" + group),
                                new CopyJobSpec(
                                        groupSources,
                                        intermediate,
                                        JobInfo.CreateDisposition.CREATE_IF_NEEDED,
                                        JobInfo.WriteDisposition.WRITE_TRUNCATE)));
            }
            intermediateLevels.add(jobs);
            sources = nextSources;
            level++;
        }

        PlannedQuery terminalQuery = null;
        TableDestination copyDestination = destination;
        JobInfo.CreateDisposition copyCreateDisposition = JobInfo.CreateDisposition.CREATE_NEVER;
        JobInfo.WriteDisposition copyWriteDisposition =
                toWriteDisposition(options.getWriteDisposition());
        if (options.getWriteDisposition() == WriteDisposition.WRITE_TRUNCATE_DATA) {
            copyDestination = aggregateTable(destination, sources);
            cleanupTables.add(copyDestination);
            copyCreateDisposition = JobInfo.CreateDisposition.CREATE_IF_NEEDED;
            copyWriteDisposition = JobInfo.WriteDisposition.WRITE_TRUNCATE;
            terminalQuery =
                    new PlannedQuery(
                            jobId(
                                    "flink-bq-query",
                                    destination,
                                    List.of(copyDestination.toString()),
                                    null),
                            new QueryJobSpec(copyDestination, destination, schemaUpdateOptions()));
        }
        PlannedCopy finalCopy =
                new PlannedCopy(
                        jobId("flink-bq-copy", copyDestination, tablePaths(sources), null),
                        new CopyJobSpec(
                                sources,
                                copyDestination,
                                copyCreateDisposition,
                                copyWriteDisposition));
        return new DestinationCopy(intermediateLevels, finalCopy, terminalQuery, cleanupTables);
    }

    @VisibleForTesting
    static void validateJobCounts(long loadJobs, long copyJobs) throws IOException {
        validateJobCounts(loadJobs, copyJobs, Limits.BIGQUERY);
    }

    private static void validateJobCounts(long loadJobs, long copyJobs, Limits limits)
            throws IOException {
        if (loadJobs > limits.maxLoadJobsPerCommit) {
            throw new IOException(
                    "FILE_LOADS commit requires "
                            + loadJobs
                            + " load jobs, but one commit may plan at most "
                            + limits.maxLoadJobsPerCommit
                            + "; increase maxStagingFileBytes or reduce the volume per commit");
        }
        if (copyJobs > limits.maxCopyJobsPerCommit) {
            throw new IOException(
                    "FILE_LOADS commit requires "
                            + copyJobs
                            + " copy jobs, but one commit may plan at most "
                            + limits.maxCopyJobsPerCommit
                            + "; increase maxStagingFileBytes or reduce the volume per commit");
        }
    }

    private <T> void runInWaves(List<T> jobs, JobSubmitter<T> submitter, Function<T, String> jobId)
            throws IOException {
        runInWaves(jobs, submitter, jobId, limits.maxSubmissionsPerWave);
    }

    private <T> void runInWaves(
            List<T> jobs,
            JobSubmitter<T> submitter,
            Function<T, String> jobId,
            int maxSubmissionsPerWave)
            throws IOException {
        for (int start = 0; start < jobs.size(); start += maxSubmissionsPerWave) {
            int end = Math.min(start + maxSubmissionsPerWave, jobs.size());
            for (int i = start; i < end; i++) {
                submitter.submit(jobs.get(i));
            }
            for (int i = start; i < end; i++) {
                runner.awaitJob(jobId.apply(jobs.get(i)));
            }
        }
    }

    /**
     * Groups, sorts and bin-packs the committables into load plans, one per destination <em>and
     * staging format</em>.
     *
     * <p>The format is part of the key because a load job carries exactly one: a job is configured
     * {@code AVRO} or {@code PARQUET}, never both. Normally every committable of a destination
     * shares a format and this groups exactly as it did before. The case that needs it is
     * transitional — the commit that follows a change of staging format, or the upgrade that
     * introduces the format at all, where committables already in committer state were written as
     * Avro and new ones are not.
     *
     * <p>Append and empty dispositions retain two direct load jobs when both format groups fit.
     * Replacement dispositions instead route both groups through temporary tables and one final
     * action, so one direct truncate cannot erase the other format's rows. Draining the old format
     * first needs the writer to know what is still in committer state, which it cannot, and
     * rejecting the mix would wedge the restart of any job whose format changed, recoverable only
     * by discarding state.
     *
     * <p>The job ids need no help from this: {@link #jobId} hashes the source URI list, and the two
     * formats' files are different objects, so their ids already differ.
     */
    private List<DestinationLoad> plan(List<FileLoadsCommittable> committables) {
        Map<DestinationFormat, List<FileLoadsCommittable>> byDestination =
                new TreeMap<>(
                        Comparator.comparing((DestinationFormat k) -> k.destination.toTablePath())
                                .thenComparing(k -> k.format));
        for (FileLoadsCommittable committable : committables) {
            byDestination
                    .computeIfAbsent(
                            new DestinationFormat(
                                    committable.getDestination(), committable.getFormat()),
                            unused -> new ArrayList<>())
                    .add(committable);
        }
        List<DestinationLoad> loads = new ArrayList<>(byDestination.size());
        for (Map.Entry<DestinationFormat, List<FileLoadsCommittable>> entry :
                byDestination.entrySet()) {
            List<FileLoadsCommittable> files = entry.getValue();
            files.sort(Comparator.comparing(FileLoadsCommittable::getUri));
            loads.add(
                    new DestinationLoad(
                            entry.getKey().destination, entry.getKey().format, partition(files)));
        }
        return loads;
    }

    /**
     * The grouping key: one load job is one destination in one format.
     *
     * <p>No {@code equals}/{@code hashCode}: the map is a {@link TreeMap} and orders by the
     * comparator above, which is also what keeps the job order deterministic. Adding them would be
     * code no caller reaches.
     */
    private static final class DestinationFormat {

        private final TableDestination destination;
        private final StagingFormat format;

        DestinationFormat(TableDestination destination, StagingFormat format) {
            this.destination = destination;
            this.format = format;
        }
    }

    @VisibleForTesting
    static List<List<FileLoadsCommittable>> partition(List<FileLoadsCommittable> sortedFiles) {
        List<List<FileLoadsCommittable>> partitions = new ArrayList<>();
        List<FileLoadsCommittable> current = new ArrayList<>();
        long currentBytes = 0;
        for (FileLoadsCommittable file : sortedFiles) {
            if (!current.isEmpty()
                    && (current.size() >= MAX_FILES_PER_JOB
                            || currentBytes + file.getByteCount() > MAX_BYTES_PER_JOB)) {
                partitions.add(current);
                current = new ArrayList<>();
                currentBytes = 0;
            }
            current.add(file);
            currentBytes += file.getByteCount();
        }
        partitions.add(current);
        return partitions;
    }

    private void submitLoad(PlannedLoad load) throws IOException {
        Schema schema = finalTableSchema(load.finalDestination);
        loadJobsSubmitted.inc();
        runner.submitLoad(
                load.jobId,
                new LoadJobSpec(
                        load.jobDestination,
                        load.uris,
                        schema,
                        load.direct
                                ? toCreateDisposition(config.getCreateDisposition())
                                : JobInfo.CreateDisposition.CREATE_IF_NEEDED,
                        load.direct
                                ? toWriteDisposition(options.getWriteDisposition())
                                : JobInfo.WriteDisposition.WRITE_TRUNCATE,
                        load.direct ? schemaUpdateOptions() : List.of(),
                        load.format));
    }

    private void submitCopy(PlannedCopy copy) throws IOException {
        runner.submitCopy(copy.jobId, copy.spec);
    }

    private void submitQuery(PlannedQuery query) throws IOException {
        runner.submitQuery(query.jobId, query.spec);
    }

    /**
     * Memoizing wrapper around {@link #ensureFinalTable}: the reconciliation and its create/update
     * side effects run once per destination per run, however many temporary-table loads the
     * destination requires.
     */
    private Schema finalTableSchema(TableDestination destination) throws IOException {
        Schema schema = finalTableSchemas.get(destination);
        if (schema == null) {
            schema = ensureFinalTable(destination);
            finalTableSchemas.put(destination, schema);
        }
        return schema;
    }

    /**
     * Reconciles the destination table and returns the schema every load of this run carries — the
     * one decision shared by direct loads and the temp-table path (through {@link
     * #finalTableSchema}), so the same records cannot succeed or fail depending on partition count.
     * Creates a missing table under {@code CREATE_IF_NEEDED} with the configured
     * partitioning/clustering (fails under {@code CREATE_NEVER}), then re-reads it — creation
     * swallows a lost race, so what exists may not be what was asked for. Under {@code
     * WRITE_TRUNCATE} the load or copy replaces the table's schema wholesale, so the serializer's
     * schema is used as-is. Otherwise — appending, replacing only data, or writing into an empty
     * table — the live schema wins: it is returned untouched when schema updates are disabled, and
     * unioned with the serializer's when they are enabled (new {@code REQUIRED} columns arrive
     * {@code NULLABLE}, since BigQuery cannot add {@code REQUIRED} columns), retrying lost update
     * races.
     */
    private Schema ensureFinalTable(TableDestination destination) throws IOException {
        TableSchema desired = config.getSerializer().getTableSchema(destination);
        TableSchemaSnapshot snapshot = tableAdmin.getSchema(destination);
        if (snapshot == null) {
            if (config.getCreateDisposition() == CreateDisposition.CREATE_NEVER) {
                throw new IOException(
                        "Destination table "
                                + destination
                                + " does not exist and createDisposition is CREATE_NEVER.");
            }
            tableAdmin.create(
                    destination,
                    desired,
                    config.getTableCreateOptionsProvider().optionsFor(destination));
            // Creation swallows a lost race (HTTP 409 = someone else created it first), so the
            // table's actual schema may be a concurrent creator's rather than the desired one —
            // re-read and reconcile against what is really there instead of trusting the
            // argument.
            snapshot = tableAdmin.getSchema(destination);
            if (snapshot == null) {
                throw new IOException(
                        "Destination table "
                                + destination
                                + " disappeared right after it was created.");
            }
        }
        if (options.getWriteDisposition() == WriteDisposition.WRITE_TRUNCATE) {
            return StorageSchemaConverter.toBigQuerySchema(desired);
        }
        if (!config.getSchemaUpdateOptions().isEnabled()) {
            try {
                SchemaUnifier.union(snapshot.getSchema(), desired, config.getSchemaUpdateOptions());
            } catch (SchemaUnifier.SchemaUnionException e) {
                // The union's message names the difference; the outcome depends on which kind it
                // is. A serializer column the table lacks is silently ignored by the load
                // (measured) — dropped data, not an error — while a type disagreement surfaces
                // when the load runs. Either way, say what wins, once per destination per run.
                LOG.warn(
                        "Schema updates are disabled, so the live schema of {} wins over the"
                                + " serializer's: {}",
                        destination,
                        e.getMessage());
            }
            return StorageSchemaConverter.toBigQuerySchema(snapshot.getSchema());
        }
        for (int attempt = 1; attempt <= schemaReconcileSchedule.maxAttempts(); attempt++) {
            SchemaUnifier.UnionResult union =
                    SchemaUnifier.union(
                            snapshot.getSchema(), desired, config.getSchemaUpdateOptions());
            if (!union.isChanged()
                    || tableAdmin.updateSchema(destination, snapshot, union.getSchema())) {
                return StorageSchemaConverter.toBigQuerySchema(union.getSchema());
            }
            Retries.sleep(
                    schemaReconcileSchedule.backoffMs(attempt),
                    "Interrupted while reconciling the schema of " + destination);
            snapshot = tableAdmin.getSchema(destination);
            if (snapshot == null) {
                throw new IOException(
                        "Destination table "
                                + destination
                                + " disappeared while reconciling its"
                                + " schema.");
            }
        }
        throw new IOException(
                "Failed to reconcile the schema of "
                        + destination
                        + " after "
                        + schemaReconcileSchedule.maxAttempts()
                        + " attempts (concurrent updates kept winning).");
    }

    private List<JobInfo.SchemaUpdateOption> schemaUpdateOptions() {
        // BigQuery honors schema update options when appending or replacing only the data.
        // WRITE_TRUNCATE replaces the schema wholesale and WRITE_EMPTY only writes into an empty
        // table after the connector has reconciled it.
        if (options.getWriteDisposition() != WriteDisposition.WRITE_APPEND
                && options.getWriteDisposition() != WriteDisposition.WRITE_TRUNCATE_DATA) {
            return List.of();
        }
        SchemaUpdateOptions updateOptions = config.getSchemaUpdateOptions();
        List<JobInfo.SchemaUpdateOption> jobOptions = new ArrayList<>(2);
        if (updateOptions.isAllowNewFields()) {
            jobOptions.add(JobInfo.SchemaUpdateOption.ALLOW_FIELD_ADDITION);
        }
        if (updateOptions.isAllowFieldRelaxation()) {
            jobOptions.add(JobInfo.SchemaUpdateOption.ALLOW_FIELD_RELAXATION);
        }
        return jobOptions;
    }

    private TableDestination tempTable(
            TableDestination destination,
            StagingFormat format,
            boolean multipleFormats,
            int partitionIndex) {
        String dataset =
                options.getTempDataset() != null
                        ? options.getTempDataset()
                        : destination.getDataset();
        String hashMaterial = destination.toTablePath();
        if (checkpointId != null || multipleFormats) {
            // A format transition can put two independently partitioned loads for one destination
            // in the same checkpoint. Both need distinct temp tables and copy-job source lists.
            hashMaterial += "\n" + format;
        }
        String name =
                "tmp_"
                        + flinkJobId
                        + "_"
                        + sha256Hex(hashMaterial).substring(0, 12)
                        + (checkpointId != null ? "_c" + checkpointId : "")
                        + "_p"
                        + partitionIndex;
        return TableDestination.of(destination.getProject(), dataset, name);
    }

    private TableDestination intermediateTable(
            TableDestination destination, List<TableDestination> sources, int level, int group) {
        String dataset =
                options.getTempDataset() != null
                        ? options.getTempDataset()
                        : destination.getDataset();
        String hash =
                sha256Hex(destination.toTablePath() + "\n" + String.join("\n", tablePaths(sources)))
                        .substring(0, 12);
        String name =
                "tmp_"
                        + flinkJobId
                        + "_"
                        + hash
                        + (checkpointId != null ? "_c" + checkpointId : "")
                        + "_l"
                        + level
                        + "_g"
                        + group;
        return TableDestination.of(destination.getProject(), dataset, name);
    }

    private TableDestination aggregateTable(
            TableDestination destination, List<TableDestination> sources) {
        String dataset =
                options.getTempDataset() != null
                        ? options.getTempDataset()
                        : destination.getDataset();
        String hash =
                sha256Hex(destination.toTablePath() + "\n" + String.join("\n", tablePaths(sources)))
                        .substring(0, 12);
        String name =
                "tmp_"
                        + flinkJobId
                        + "_"
                        + hash
                        + (checkpointId != null ? "_c" + checkpointId : "")
                        + "_aggregate";
        return TableDestination.of(destination.getProject(), dataset, name);
    }

    /**
     * A deterministic job id: prefix, Flink job id, and a hash of the destination and sources. The
     * hash alone is the idempotency key — source URI sets are unique per run by construction — and
     * a streaming id additionally carries a visible {@code -c<checkpointId>} segment so a BigQuery
     * job can be attributed to its checkpoint.
     */
    private String jobId(
            String prefix, TableDestination destination, List<String> sources, String suffix) {
        StringBuilder material = new StringBuilder(destination.toTablePath());
        for (String source : sources) {
            material.append('\n').append(source);
        }
        return prefix
                + "-"
                + flinkJobId
                + (checkpointId != null ? "-c" + checkpointId : "")
                + "-"
                + sha256Hex(material.toString()).substring(0, 16)
                + (suffix != null ? "-" + suffix : "");
    }

    private static List<String> urisOf(List<FileLoadsCommittable> files) {
        List<String> uris = new ArrayList<>(files.size());
        for (FileLoadsCommittable file : files) {
            uris.add(file.getUri());
        }
        return uris;
    }

    private static List<String> tablePaths(List<TableDestination> tables) {
        List<String> paths = new ArrayList<>(tables.size());
        for (TableDestination table : tables) {
            paths.add(table.toTablePath());
        }
        return paths;
    }

    private static String sha256Hex(String value) {
        MessageDigest digest;
        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is unavailable", e);
        }
        return StringUtils.byteToHexString(digest.digest(value.getBytes(StandardCharsets.UTF_8)));
    }

    private static JobInfo.CreateDisposition toCreateDisposition(CreateDisposition disposition) {
        return disposition == CreateDisposition.CREATE_NEVER
                ? JobInfo.CreateDisposition.CREATE_NEVER
                : JobInfo.CreateDisposition.CREATE_IF_NEEDED;
    }

    private static JobInfo.WriteDisposition toWriteDisposition(WriteDisposition disposition) {
        switch (disposition) {
            case WRITE_TRUNCATE:
                return JobInfo.WriteDisposition.WRITE_TRUNCATE;
            case WRITE_TRUNCATE_DATA:
                return JobInfo.WriteDisposition.WRITE_TRUNCATE_DATA;
            case WRITE_EMPTY:
                return JobInfo.WriteDisposition.WRITE_EMPTY;
            default:
                return JobInfo.WriteDisposition.WRITE_APPEND;
        }
    }

    private boolean isReplacementDisposition() {
        return options.getWriteDisposition() == WriteDisposition.WRITE_TRUNCATE
                || options.getWriteDisposition() == WriteDisposition.WRITE_TRUNCATE_DATA;
    }

    /** One destination table's load plan and the job/table names it produced. */
    private static final class DestinationLoad {

        private final TableDestination destination;
        private final StagingFormat format;
        private final List<List<FileLoadsCommittable>> partitions;

        DestinationLoad(
                TableDestination destination,
                StagingFormat format,
                List<List<FileLoadsCommittable>> partitions) {
            this.destination = destination;
            this.format = format;
            this.partitions = partitions;
        }
    }

    /** BigQuery limits used by the planner; tests shrink them to exercise deep hierarchies. */
    @VisibleForTesting
    static final class Limits {

        private static final Limits BIGQUERY =
                new Limits(
                        MAX_SOURCE_TABLES_PER_COPY,
                        MAX_JOBS_PER_COMMIT,
                        MAX_JOBS_PER_COMMIT,
                        MAX_SUBMISSIONS_PER_WAVE);

        private final int maxSourceTablesPerCopy;
        private final int maxLoadJobsPerCommit;
        private final int maxCopyJobsPerCommit;
        private final int maxSubmissionsPerWave;

        Limits(
                int maxSourceTablesPerCopy,
                int maxLoadJobsPerCommit,
                int maxCopyJobsPerCommit,
                int maxSubmissionsPerWave) {
            if (maxSourceTablesPerCopy < 2
                    || maxLoadJobsPerCommit < 1
                    || maxCopyJobsPerCommit < 1
                    || maxSubmissionsPerWave < 1) {
                throw new IllegalArgumentException(
                        "Planner limits must be positive and fan-out >= 2");
            }
            this.maxSourceTablesPerCopy = maxSourceTablesPerCopy;
            this.maxLoadJobsPerCommit = maxLoadJobsPerCommit;
            this.maxCopyJobsPerCommit = maxCopyJobsPerCommit;
            this.maxSubmissionsPerWave = maxSubmissionsPerWave;
        }
    }

    /** Every job and cleanup target for one commit, validated before execution. */
    private static final class CommitPlan {

        private final List<PlannedLoad> loads;
        private final List<DestinationCopy> copies;
        private final int intermediateLevelCount;
        private final int destinationCount;

        CommitPlan(
                List<PlannedLoad> loads,
                List<DestinationCopy> copies,
                int intermediateLevelCount,
                int destinationCount) {
            this.loads = loads;
            this.copies = copies;
            this.intermediateLevelCount = intermediateLevelCount;
            this.destinationCount = destinationCount;
        }
    }

    /** One deterministic leaf load. */
    private static final class PlannedLoad {

        private final TableDestination finalDestination;
        private final TableDestination jobDestination;
        private final StagingFormat format;
        private final List<String> uris;
        private final String jobId;
        private final boolean direct;

        PlannedLoad(
                TableDestination finalDestination,
                TableDestination jobDestination,
                StagingFormat format,
                List<String> uris,
                String jobId,
                boolean direct) {
            this.finalDestination = finalDestination;
            this.jobDestination = jobDestination;
            this.format = format;
            this.uris = uris;
            this.jobId = jobId;
            this.direct = direct;
        }
    }

    /** One deterministic intermediate or final copy. */
    private static final class PlannedCopy {

        private final String jobId;
        private final CopyJobSpec spec;

        PlannedCopy(String jobId, CopyJobSpec spec) {
            this.jobId = jobId;
            this.spec = spec;
        }
    }

    /** One deterministic terminal query that replaces only the final table's data. */
    private static final class PlannedQuery {

        private final String jobId;
        private final QueryJobSpec spec;

        PlannedQuery(String jobId, QueryJobSpec spec) {
            this.jobId = jobId;
            this.spec = spec;
        }
    }

    /** One destination table's combined temporary-table hierarchy across all staging formats. */
    private static final class DestinationCopy {

        private final List<List<PlannedCopy>> intermediateLevels;
        private final PlannedCopy finalCopy;
        @Nullable private final PlannedQuery terminalQuery;
        private final List<TableDestination> cleanupTables;

        DestinationCopy(
                List<List<PlannedCopy>> intermediateLevels,
                PlannedCopy finalCopy,
                @Nullable PlannedQuery terminalQuery,
                List<TableDestination> cleanupTables) {
            this.intermediateLevels = intermediateLevels;
            this.finalCopy = finalCopy;
            this.terminalQuery = terminalQuery;
            this.cleanupTables = cleanupTables;
        }

        long jobCount() {
            long count = 1;
            for (List<PlannedCopy> level : intermediateLevels) {
                count += level.size();
            }
            return count;
        }
    }

    @FunctionalInterface
    private interface JobSubmitter<T> {

        void submit(T job) throws IOException;
    }
}
