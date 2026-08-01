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
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

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
 * <p><b>Common case — one partition.</b> A table whose files fit one load job is loaded directly:
 * the load job carries the reconciled schema, the configured dispositions, and — belt-and-braces
 * against external mid-run schema changes — the native {@code ALLOW_FIELD_ADDITION}/{@code
 * ALLOW_FIELD_RELAXATION} schema update options. (BigQuery only honors schema update options on
 * {@code WRITE_APPEND} jobs; with other dispositions they are omitted — {@code WRITE_TRUNCATE}
 * replaces the schema wholesale anyway.)
 *
 * <p><b>Overflow, batch — temporary tables plus one copy job.</b> A table exceeding the limits is
 * loaded partition-by-partition into temporary tables ({@code WRITE_TRUNCATE} + {@code
 * CREATE_IF_NEEDED}, so a retried partition load is idempotent), then appended to the final table
 * with a single atomic copy job. Copy jobs support no schema update options and require matching
 * schemas, so the temporary tables are loaded with the reconciled schema.
 *
 * <p><b>Overflow, streaming — multiple direct loads.</b> A streaming run (one with a checkpoint id)
 * skips the temporary-table path and submits one direct append job per partition instead:
 * deterministic ids keep the retries exactly-once, only the checkpoint's atomic visibility is lost
 * — rows of a partition become visible as its job completes. Streaming is {@code WRITE_APPEND}
 * only, enforced at graph construction.
 *
 * <p><b>Concurrency.</b> All jobs are submitted first and awaited second — BigQuery runs them
 * concurrently server-side, so no thread pool is needed.
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

    private static final Logger LOG = LoggerFactory.getLogger(LoadJobOrchestrator.class);

    /** BigQuery's per-load-job source URI limit. */
    @VisibleForTesting static final int MAX_FILES_PER_JOB = 10_000;

    /** Per-load-job byte budget: 11 TiB, a safety margin under BigQuery's 15 TB limit. */
    @VisibleForTesting static final long MAX_BYTES_PER_JOB = 11L * (1L << 40);

    private final BigQuerySinkConfig<?> config;
    private final FileLoadsOptions options;
    private final RetrySchedule schemaReconcileSchedule;
    private final LoadJobRunner runner;
    private final TableAdmin tableAdmin;
    private final StagingStorage storage;
    private final String flinkJobId;
    @Nullable private final Long checkpointId;

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
     *     run; a non-null id selects the streaming behavior (visible job-id segment, direct loads
     *     on overflow)
     */
    public LoadJobOrchestrator(
            BigQuerySinkConfig<?> config,
            FileLoadsOptions options,
            LoadJobRunner runner,
            TableAdmin tableAdmin,
            StagingStorage storage,
            String flinkJobId,
            @Nullable Long checkpointId) {
        this.config = config;
        this.options = options;
        this.schemaReconcileSchedule = options.toSchemaReconcileSchedule();
        this.runner = runner;
        this.tableAdmin = tableAdmin;
        this.storage = storage;
        this.flinkJobId = flinkJobId;
        this.checkpointId = checkpointId;
    }

    /**
     * Loads all staged files of the run into their destination tables.
     *
     * @param committables the staged files
     * @throws IOException if any load or copy fails; staged files are left in place
     */
    public void run(List<FileLoadsCommittable> committables) throws IOException {
        if (committables.isEmpty()) {
            LOG.info("No staged files; nothing to load");
            return;
        }
        List<DestinationLoad> loads = plan(committables);

        for (DestinationLoad load : loads) {
            submitLoads(load);
        }
        for (DestinationLoad load : loads) {
            for (String jobId : load.loadJobIds) {
                runner.awaitJob(jobId);
            }
        }
        for (DestinationLoad load : loads) {
            submitCopyIfNeeded(load);
        }
        for (DestinationLoad load : loads) {
            if (load.copyJobId != null) {
                runner.awaitJob(load.copyJobId);
            }
        }
        for (DestinationLoad load : loads) {
            for (TableDestination tempTable : load.tempTables) {
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
                loads.size(),
                checkpointId != null ? " for checkpoint " + checkpointId : "");
    }

    /** Groups, sorts and bin-packs the committables into per-destination load plans. */
    private List<DestinationLoad> plan(List<FileLoadsCommittable> committables) {
        Map<TableDestination, List<FileLoadsCommittable>> byDestination =
                new TreeMap<>(Comparator.comparing(TableDestination::toTablePath));
        for (FileLoadsCommittable committable : committables) {
            byDestination
                    .computeIfAbsent(committable.getDestination(), unused -> new ArrayList<>())
                    .add(committable);
        }
        List<DestinationLoad> loads = new ArrayList<>(byDestination.size());
        for (Map.Entry<TableDestination, List<FileLoadsCommittable>> entry :
                byDestination.entrySet()) {
            List<FileLoadsCommittable> files = entry.getValue();
            files.sort(Comparator.comparing(FileLoadsCommittable::getUri));
            loads.add(new DestinationLoad(entry.getKey(), partition(files)));
        }
        return loads;
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

    private void submitLoads(DestinationLoad load) throws IOException {
        TableDestination destination = load.destination;
        if (load.partitions.size() == 1) {
            load.loadJobIds.add(submitDirectLoad(destination, load.partitions.get(0), null));
            return;
        }
        if (checkpointId != null) {
            // Streaming overflow: no temporary tables — one direct append job per partition.
            // Only the checkpoint's atomic visibility is lost; deterministic per-partition ids
            // keep retries exactly-once. The partitions run sequentially (each awaited before
            // the next is submitted): the schema is reconciled once up front, but the jobs still
            // carry schema-update options, which must not race each other on the destination
            // table the way concurrent ALLOW_FIELD_ADDITION jobs would.
            for (int i = 0; i < load.partitions.size(); i++) {
                runner.awaitJob(submitDirectLoad(destination, load.partitions.get(i), "p" + i));
            }
            return;
        }
        // Copy jobs require matching schemas, so temp tables are loaded with the final table's
        // reconciled schema.
        Schema schema = finalTableSchema(destination);
        for (int i = 0; i < load.partitions.size(); i++) {
            List<String> uris = urisOf(load.partitions.get(i));
            TableDestination tempTable = tempTable(destination, i);
            load.tempTables.add(tempTable);
            String jobId = jobId("flink-bq-load", destination, uris, "p" + i);
            load.loadJobIds.add(jobId);
            runner.submitLoad(
                    jobId,
                    new LoadJobSpec(
                            tempTable,
                            uris,
                            schema,
                            JobInfo.CreateDisposition.CREATE_IF_NEEDED,
                            // Truncating makes a retried partition load idempotent.
                            JobInfo.WriteDisposition.WRITE_TRUNCATE,
                            List.of()));
        }
    }

    /**
     * Submits one load job straight into the destination table — reconciled first, so the job
     * carries the live table's schema — with the configured dispositions, and returns its job id
     * (not yet awaited).
     */
    private String submitDirectLoad(
            TableDestination destination,
            List<FileLoadsCommittable> partition,
            @Nullable String suffix)
            throws IOException {
        Schema schema = finalTableSchema(destination);
        List<String> uris = urisOf(partition);
        String jobId = jobId("flink-bq-load", destination, uris, suffix);
        runner.submitLoad(
                jobId,
                new LoadJobSpec(
                        destination,
                        uris,
                        schema,
                        toCreateDisposition(config.getCreateDisposition()),
                        toWriteDisposition(options.getWriteDisposition()),
                        schemaUpdateOptions()));
        return jobId;
    }

    private void submitCopyIfNeeded(DestinationLoad load) throws IOException {
        if (load.tempTables.isEmpty()) {
            return;
        }
        TableDestination destination = load.destination;
        List<String> tempTablePaths = new ArrayList<>(load.tempTables.size());
        for (TableDestination tempTable : load.tempTables) {
            tempTablePaths.add(tempTable.toTablePath());
        }
        load.copyJobId = jobId("flink-bq-copy", destination, tempTablePaths, null);
        runner.submitCopy(
                load.copyJobId,
                new CopyJobSpec(
                        load.tempTables,
                        destination,
                        toWriteDisposition(options.getWriteDisposition())));
    }

    /**
     * Memoizing wrapper around {@link #ensureFinalTable}: the reconciliation and its create/update
     * side effects run once per destination per run, however many loads the destination receives
     * (streaming overflow submits one per partition).
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
     * schema is used as-is. Otherwise — appending, or writing into an empty table — the live schema
     * wins: it is returned untouched when schema updates are disabled, and unioned with the
     * serializer's when they are enabled (new {@code REQUIRED} columns arrive {@code NULLABLE},
     * since BigQuery cannot add {@code REQUIRED} columns), retrying lost update races.
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
        // BigQuery only honors schema update options when appending; WRITE_TRUNCATE replaces the
        // schema wholesale and WRITE_EMPTY only ever writes into empty tables.
        if (options.getWriteDisposition() != WriteDisposition.WRITE_APPEND) {
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

    private TableDestination tempTable(TableDestination destination, int partitionIndex) {
        String dataset =
                options.getTempDataset() != null
                        ? options.getTempDataset()
                        : destination.getDataset();
        String name =
                "tmp_"
                        + flinkJobId
                        + "_"
                        + sha256Hex(destination.toTablePath()).substring(0, 12)
                        + "_p"
                        + partitionIndex;
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
            case WRITE_EMPTY:
                return JobInfo.WriteDisposition.WRITE_EMPTY;
            default:
                return JobInfo.WriteDisposition.WRITE_APPEND;
        }
    }

    /** One destination table's load plan and the job/table names it produced. */
    private static final class DestinationLoad {

        private final TableDestination destination;
        private final List<List<FileLoadsCommittable>> partitions;
        private final List<String> loadJobIds = new ArrayList<>();
        private final List<TableDestination> tempTables = new ArrayList<>();
        private String copyJobId;

        DestinationLoad(TableDestination destination, List<List<FileLoadsCommittable>> partitions) {
            this.destination = destination;
            this.partitions = partitions;
        }
    }
}
