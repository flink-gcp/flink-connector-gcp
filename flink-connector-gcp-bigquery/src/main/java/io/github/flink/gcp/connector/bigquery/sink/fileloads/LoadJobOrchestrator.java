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

package io.github.flink.gcp.connector.bigquery.sink.fileloads;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;

import com.google.cloud.bigquery.Clustering;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.TimePartitioning;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import io.github.flink.gcp.connector.bigquery.sink.BigQuerySinkConfig;
import io.github.flink.gcp.connector.bigquery.sink.CreateDisposition;
import io.github.flink.gcp.connector.bigquery.sink.FileLoadsOptions;
import io.github.flink.gcp.connector.bigquery.sink.SchemaUpdateOptions;
import io.github.flink.gcp.connector.bigquery.sink.TableCreateOptions;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.WriteDisposition;
import io.github.flink.gcp.connector.bigquery.sink.writer.RetrySchedule;
import io.github.flink.gcp.connector.bigquery.sink.writer.SchemaUnifier;
import io.github.flink.gcp.connector.bigquery.sink.writer.StorageSchemaConverter;
import io.github.flink.gcp.connector.bigquery.sink.writer.TableAdmin;
import io.github.flink.gcp.connector.bigquery.sink.writer.TableSchemaSnapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Turns the staged files of one batch run into BigQuery load jobs: groups files per destination
 * table, bin-packs each table's files against the per-load-job limits, and executes the resulting
 * jobs through a {@link LoadJobRunner}.
 *
 * <p><b>Common case — one partition.</b> A table whose files fit one load job is loaded directly:
 * the load job carries the serializer's schema, the configured dispositions, the partitioning and
 * clustering of an auto-created table, and — gated by {@link SchemaUpdateOptions} — the native
 * {@code ALLOW_FIELD_ADDITION}/{@code ALLOW_FIELD_RELAXATION} schema update options. (BigQuery only
 * honors schema update options on {@code WRITE_APPEND} jobs; with other dispositions they are
 * omitted — {@code WRITE_TRUNCATE} replaces the schema wholesale anyway.)
 *
 * <p><b>Overflow — temporary tables plus one copy job.</b> A table exceeding the limits is loaded
 * partition-by-partition into temporary tables ({@code WRITE_TRUNCATE} + {@code CREATE_IF_NEEDED},
 * so a retried partition load is idempotent), then appended to the final table with a single atomic
 * copy job. Copy jobs support no schema update options, so the final table is reconciled
 * <em>before</em> the copy: created via {@link TableAdmin} when missing (with the configured
 * partitioning/clustering), or schema-unioned via {@link SchemaUnifier} when updates are enabled.
 *
 * <p><b>Concurrency.</b> All jobs are submitted first and awaited second — BigQuery runs them
 * concurrently server-side, so no thread pool is needed.
 *
 * <p><b>Determinism and retries.</b> Files are sorted by URI before bin-packing, so a retried run
 * over the same committables produces identical partitions, temporary table names and job ids
 * (hashes over destination and source URIs), letting the {@link LoadJobRunner} re-attach instead of
 * double-loading. On success staged files are deleted best-effort; on failure everything is
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

    private static final RetrySchedule SCHEMA_UPDATE_SCHEDULE =
            new RetrySchedule(500, 10_000, 10, 0.25);

    private final BigQuerySinkConfig<?> config;
    private final FileLoadsOptions options;
    private final LoadJobRunner runner;
    private final TableAdmin tableAdmin;
    private final StagingStorage storage;
    private final String flinkJobId;

    /**
     * Creates an orchestrator.
     *
     * @param config the sink configuration
     * @param options the FILE_LOADS options
     * @param runner the job runner
     * @param tableAdmin the table admin (pre-copy table creation and schema reconciliation)
     * @param storage the staging storage (post-load cleanup)
     * @param flinkJobId the Flink job id (hex), scoping temporary table names and job ids
     */
    public LoadJobOrchestrator(
            BigQuerySinkConfig<?> config,
            FileLoadsOptions options,
            LoadJobRunner runner,
            TableAdmin tableAdmin,
            StagingStorage storage,
            String flinkJobId) {
        this.config = config;
        this.options = options;
        this.runner = runner;
        this.tableAdmin = tableAdmin;
        this.storage = storage;
        this.flinkJobId = flinkJobId;
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
        for (FileLoadsCommittable committable : committables) {
            uris.add(committable.getUri());
        }
        storage.deleteObjects(uris);
        LOG.info("Loaded {} staged files into {} tables", committables.size(), loads.size());
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
        Schema schema =
                StorageSchemaConverter.toBigQuerySchema(
                        config.getSerializer().getTableSchema(destination));
        if (load.partitions.size() == 1) {
            List<String> uris = urisOf(load.partitions.get(0));
            TableCreateOptions createOptions =
                    config.getTableCreateOptionsProvider().optionsFor(destination);
            String jobId = jobId("flink-bq-load", destination, uris, null);
            load.loadJobIds.add(jobId);
            runner.submitLoad(
                    jobId,
                    new LoadJobSpec(
                            destination,
                            uris,
                            schema,
                            toCreateDisposition(config.getCreateDisposition()),
                            toWriteDisposition(options.getWriteDisposition()),
                            schemaUpdateOptions(),
                            toTimePartitioning(createOptions),
                            toClustering(createOptions)));
            return;
        }
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
                            List.of(),
                            null,
                            null));
        }
    }

    private void submitCopyIfNeeded(DestinationLoad load) throws IOException {
        if (load.tempTables.isEmpty()) {
            return;
        }
        TableDestination destination = load.destination;
        ensureFinalTable(destination);
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
     * Makes the final table ready for the copy job, which can neither create it with
     * partitioning/clustering nor update its schema: creates a missing table under {@code
     * CREATE_IF_NEEDED} (fails under {@code CREATE_NEVER}), and — when schema updates are enabled —
     * unions the live schema with the serializer's, retrying lost update races.
     */
    private void ensureFinalTable(TableDestination destination) throws IOException {
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
            return;
        }
        if (!config.getSchemaUpdateOptions().isEnabled()) {
            return;
        }
        for (int attempt = 1; attempt <= SCHEMA_UPDATE_SCHEDULE.maxAttempts(); attempt++) {
            SchemaUnifier.UnionResult union =
                    SchemaUnifier.union(
                            snapshot.getSchema(), desired, config.getSchemaUpdateOptions());
            if (!union.isChanged()
                    || tableAdmin.updateSchema(destination, snapshot, union.getSchema())) {
                return;
            }
            try {
                Thread.sleep(SCHEMA_UPDATE_SCHEDULE.backoffMs(attempt));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException(
                        "Interrupted while reconciling the schema of " + destination, e);
            }
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
                        + SCHEMA_UPDATE_SCHEDULE.maxAttempts()
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

    /** A deterministic job id: prefix, Flink job id, and a hash of the destination and sources. */
    private String jobId(
            String prefix, TableDestination destination, List<String> sources, String suffix) {
        StringBuilder material = new StringBuilder(destination.toTablePath());
        for (String source : sources) {
            material.append('\n').append(source);
        }
        return prefix
                + "-"
                + flinkJobId
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
        StringBuilder hex = new StringBuilder();
        for (byte b : digest.digest(value.getBytes(StandardCharsets.UTF_8))) {
            hex.append(String.format("%02x", b));
        }
        return hex.toString();
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

    private static TimePartitioning toTimePartitioning(TableCreateOptions options) {
        if (options.getTimePartitioningType() == null) {
            return null;
        }
        TimePartitioning.Builder partitioning =
                TimePartitioning.newBuilder(
                        TimePartitioning.Type.valueOf(options.getTimePartitioningType().name()));
        if (options.getTimePartitioningField() != null) {
            partitioning.setField(options.getTimePartitioningField());
        }
        if (options.getTimePartitioningExpirationMs() != null) {
            partitioning.setExpirationMs(options.getTimePartitioningExpirationMs());
        }
        return partitioning.build();
    }

    private static Clustering toClustering(TableCreateOptions options) {
        if (options.getClusteredFields().isEmpty()) {
            return null;
        }
        return Clustering.newBuilder().setFields(options.getClusteredFields()).build();
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
