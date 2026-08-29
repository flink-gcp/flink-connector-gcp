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

package io.github.flink.gcp.connector.bigquery.sink.fileloads.loadjob;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.util.StringUtils;

import com.google.cloud.bigquery.JobInfo;
import io.github.flink.gcp.connector.bigquery.sink.BigQuerySinkConfig;
import io.github.flink.gcp.connector.bigquery.sink.CreateDisposition;
import io.github.flink.gcp.connector.bigquery.sink.SchemaUpdateOptions;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.WriteDisposition;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.FileLoadsCommittable;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.FileLoadsOptions;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.StagingFormat;

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

/**
 * Plans one {@code FILE_LOADS} commit: groups the staged files per destination table and staging
 * format, bin-packs each group against the per-load-job limits, and names every load, copy and
 * terminal query the commit will run — all of it before {@link LoadJobOrchestrator} performs any of
 * them.
 *
 * <p><b>Nothing here can cause a side effect, and that is the point.</b> The fields are the sink
 * configuration, the {@code FILE_LOADS} options, the Flink job id, the checkpoint id and the
 * planner {@link Limits} — no {@link LoadJobRunner}, no {@code TableAdmin}, no {@code
 * StagingStorage}. {@code docs/adr/0018} requires the whole job graph to be bounded and validated
 * before any table is created or any job submitted; with the planning code here that rule is a
 * constructor signature rather than something every review has to re-check.
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
 * <p><b>Determinism.</b> Files are sorted by URI before bin-packing, so a retried run over the same
 * committables produces identical partitions, temporary table names and job ids (hashes over
 * destination and source URIs; streaming ids additionally carry a visible {@code -c<checkpointId>}
 * segment for attribution), letting the {@link LoadJobRunner} re-attach instead of double-loading.
 */
@Internal
final class CommitPlanner {

    /** BigQuery's per-load-job source URI limit. */
    @VisibleForTesting static final int MAX_FILES_PER_JOB = 10_000;

    /** Per-load-job byte budget: 11 TiB, a safety margin under BigQuery's 15 TB limit. */
    private static final long MAX_BYTES_PER_JOB = 11L * (1L << 40);

    private final BigQuerySinkConfig<?> config;
    private final FileLoadsOptions options;
    private final String flinkJobId;
    @Nullable private final Long checkpointId;
    private final Limits limits;

    /**
     * Creates a planner.
     *
     * @param config the sink configuration
     * @param options the FILE_LOADS options
     * @param flinkJobId the Flink job id (hex), scoping temporary table names and job ids
     * @param checkpointId the checkpoint whose files this commit loads, or {@code null} for a batch
     *     run; a non-null id scopes streaming job ids and temporary-table names
     * @param limits the BigQuery limits to plan against
     */
    CommitPlanner(
            BigQuerySinkConfig<?> config,
            FileLoadsOptions options,
            String flinkJobId,
            @Nullable Long checkpointId,
            Limits limits) {
        this.config = config;
        this.options = options;
        this.flinkJobId = flinkJobId;
        this.checkpointId = checkpointId;
        this.limits = limits;
    }

    /** Builds and validates every load, copy level, terminal query and cleanup target first. */
    CommitPlan plan(List<FileLoadsCommittable> committables) throws IOException {
        List<DestinationLoad> destinationLoads = groupByDestinationAndFormat(committables);
        Map<TableDestination, Integer> formatsPerDestination = new HashMap<>();
        Set<TableDestination> overflowingDestinations = new HashSet<>();
        for (DestinationLoad load : destinationLoads) {
            formatsPerDestination.merge(load.destination, 1, Integer::sum);
            if (load.partitions.size() > 1) {
                overflowingDestinations.add(load.destination);
            }
        }

        Map<TableDestination, List<PlannedLoad>> loadsByDestination = new LinkedHashMap<>();
        int loadJobCount = 0;
        Map<TableDestination, List<TableDestination>> copySources = new LinkedHashMap<>();
        for (DestinationLoad load : destinationLoads) {
            boolean replacementAcrossFormats =
                    isReplacementDisposition() && formatsPerDestination.get(load.destination) > 1;
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
                                    formatsPerDestination.get(load.destination) > 1,
                                    partitionIndex);
                    loadsByDestination
                            .computeIfAbsent(load.destination, unused -> new ArrayList<>())
                            .add(
                                    new PlannedLoad(
                                            tempTable,
                                            load.format,
                                            uris,
                                            jobId(
                                                    "flink-bq-load",
                                                    load.destination,
                                                    uris,
                                                    "p" + partitionIndex),
                                            JobInfo.CreateDisposition.CREATE_IF_NEEDED,
                                            JobInfo.WriteDisposition.WRITE_TRUNCATE,
                                            List.of()));
                    copySources
                            .computeIfAbsent(load.destination, unused -> new ArrayList<>())
                            .add(tempTable);
                } else {
                    loadsByDestination
                            .computeIfAbsent(load.destination, unused -> new ArrayList<>())
                            .add(
                                    new PlannedLoad(
                                            load.destination,
                                            load.format,
                                            uris,
                                            jobId("flink-bq-load", load.destination, uris, null),
                                            toCreateDisposition(config.getCreateDisposition()),
                                            toWriteDisposition(options.getWriteDisposition()),
                                            schemaUpdateOptions()));
                }
                loadJobCount++;
            }
        }

        long copyJobCount = 0;
        List<DestinationCommitPlan> destinations = new ArrayList<>(loadsByDestination.size());
        for (Map.Entry<TableDestination, List<PlannedLoad>> entry : loadsByDestination.entrySet()) {
            List<TableDestination> leafTables = copySources.get(entry.getKey());
            DestinationCopy copy = leafTables == null ? null : planCopy(entry.getKey(), leafTables);
            if (copy != null) {
                copyJobCount += copy.jobCount();
            }
            destinations.add(new DestinationCommitPlan(entry.getKey(), entry.getValue(), copy));
        }
        validateJobCounts(loadJobCount, copyJobCount, limits);
        return new CommitPlan(destinations);
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
    private List<DestinationLoad> groupByDestinationAndFormat(
            List<FileLoadsCommittable> committables) {
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
        String hashMaterial = destination.toTablePath();
        if (checkpointId != null || multipleFormats) {
            // A format transition can put two independently partitioned loads for one destination
            // in the same checkpoint. Both need distinct temp tables and copy-job source lists.
            // Streaming folds the format in unconditionally, not only where a transition is
            // visible in this commit, so a destination's name is unchanged by the checkpoint on
            // which a second format appears. Do not narrow this to multipleFormats alone: it
            // renames every streaming single-format temp table, no test fails, and the first thing
            // to notice is a retried commit that no longer re-attaches to the temp tables its
            // predecessor left behind. Measured on #818.
            hashMaterial += "\n" + format;
        }
        return tempTableNamed(destination, hashMaterial, "_p" + partitionIndex);
    }

    private TableDestination intermediateTable(
            TableDestination destination, List<TableDestination> sources, int level, int group) {
        return tempTableNamed(
                destination, hashMaterialOf(destination, sources), "_l" + level + "_g" + group);
    }

    private TableDestination aggregateTable(
            TableDestination destination, List<TableDestination> sources) {
        return tempTableNamed(destination, hashMaterialOf(destination, sources), "_aggregate");
    }

    /**
     * The temporary table one step of a commit writes into. The name is deterministic in {@code
     * hashMaterial}, so a retried commit re-attaches to the tables its predecessor created instead
     * of stranding them; callers vary only that material and the suffix.
     */
    private TableDestination tempTableNamed(
            TableDestination destination, String hashMaterial, String suffix) {
        String dataset =
                options.getTempDataset() != null
                        ? options.getTempDataset()
                        : destination.getDataset();
        String name =
                "tmp_"
                        + flinkJobId
                        + "_"
                        + sha256Hex(hashMaterial).substring(0, 12)
                        + (checkpointId != null ? "_c" + checkpointId : "")
                        + suffix;
        return TableDestination.of(destination.getProject(), dataset, name);
    }

    /** What a copy target hashes: its final destination and the sources feeding that step. */
    private static String hashMaterialOf(
            TableDestination destination, List<TableDestination> sources) {
        return destination.toTablePath() + "\n" + String.join("\n", tablePaths(sources));
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

    /** One destination-and-format's sorted, bin-packed partitions. */
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
}
