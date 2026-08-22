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

package io.github.flink.gcp.connector.bigquery.table.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;

import io.github.flink.gcp.connector.base.options.ResourceNames;
import io.github.flink.gcp.connector.bigquery.sink.WriteMethod;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.FileLoadsOptions;
import io.github.flink.gcp.connector.bigquery.table.BigQueryConnectorOptions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

/**
 * Maps the {@code sink.file-loads.*} options onto {@link FileLoadsOptions}.
 *
 * <p>Under the same contract as {@code DefaultStreamOptionsMapper}: every knob is applied through
 * the builder and no default is introduced. Value validation — a staging path that is not a {@code
 * gs://} URI, a sub-millisecond backoff — stays with that builder so a SQL user gets the message a
 * DataStream user gets, <em>except</em> where the builder's message would name a setter the SQL
 * user never wrote.
 *
 * <p><b>It always builds</b>, for the reason {@code BufferedStreamOptionsMapper} states: {@code
 * fileLoadsOptions(...)} is <em>required</em> for {@code FILE_LOADS}, so whether an options object
 * is wanted is decided by the write method — which the factory knows — and not by key presence.
 * {@link #presentKeys(ReadableConfig)} survives for the factory's wrong-family check.
 *
 * <p>Two rules are owned here rather than left to the builder, because their messages have to name
 * an option key. The first is <b>a missing staging path</b>. {@code FileLoadsOptions.build()}
 * rejects it too, but names {@code stagingPath("gs://...")}, a builder method a SQL user cannot
 * call — and this is the one option on the whole table surface that has no default, so its message
 * is all that stands between a DDL and a write method with nowhere to stage. The second is <b>a
 * {@code sink.file-loads.temp-dataset} that is not one path component</b> (issue #1027): {@code
 * FileLoadsOptions} rejects it as {@code tempDataset}, which is not what the DDL said.
 *
 * <p>A malformed <em>staging path</em> is still reported as {@code stagingPath} and is not
 * translated here, because that grammar lives in a private pattern on {@code FileLoadsOptions} and
 * restating it would put a second copy of it in the tree.
 */
@Internal
public final class FileLoadsOptionsMapper {

    /** Every key of the family, for the "is any of these set?" scan. */
    private static final List<ConfigOption<?>> FAMILY =
            Arrays.asList(
                    BigQueryConnectorOptions.SINK_FILE_LOADS_STAGING_PATH,
                    BigQueryConnectorOptions.SINK_FILE_LOADS_TEMP_DATASET,
                    BigQueryConnectorOptions.SINK_FILE_LOADS_WRITE_DISPOSITION,
                    BigQueryConnectorOptions.SINK_FILE_LOADS_MIN_CHECKPOINT_INTERVAL,
                    BigQueryConnectorOptions.SINK_FILE_LOADS_MAX_STAGING_FILE_BYTES,
                    BigQueryConnectorOptions.SINK_FILE_LOADS_STAGING_FORMAT,
                    BigQueryConnectorOptions.SINK_FILE_LOADS_PARQUET_COMPRESSION,
                    BigQueryConnectorOptions.SINK_FILE_LOADS_LOAD_JOB_POLL_INITIAL_BACKOFF,
                    BigQueryConnectorOptions.SINK_FILE_LOADS_LOAD_JOB_POLL_MAX_BACKOFF,
                    BigQueryConnectorOptions.SINK_FILE_LOADS_SCHEMA_RECONCILE_INITIAL_BACKOFF,
                    BigQueryConnectorOptions.SINK_FILE_LOADS_SCHEMA_RECONCILE_MAX_BACKOFF,
                    BigQueryConnectorOptions.SINK_FILE_LOADS_SCHEMA_RECONCILE_MAX_ATTEMPTS,
                    BigQueryConnectorOptions.SINK_FILE_LOADS_PER_DESTINATION_METRICS);

    private FileLoadsOptionsMapper() {}

    /** Returns the keys of the family that the given configuration sets, in declaration order. */
    public static List<String> presentKeys(ReadableConfig config) {
        List<String> present = new ArrayList<>();
        for (ConfigOption<?> option : FAMILY) {
            if (config.getOptional(option).isPresent()) {
                present.add(option.key());
            }
        }
        return present;
    }

    /**
     * Builds the options, leaving every knob the configuration does not set at its default.
     *
     * @param config the table's options
     * @return the options, never {@code null}
     * @throws ValidationException if the staging path, which has no default, is not set
     */
    public static FileLoadsOptions map(ReadableConfig config) {
        Optional<String> stagingPath =
                config.getOptional(BigQueryConnectorOptions.SINK_FILE_LOADS_STAGING_PATH);
        if (!stagingPath.isPresent()) {
            throw new ValidationException(
                    String.format(
                            "Option '%s' is required when '%s' is '%s': that write method stages"
                                    + " rows as files on Cloud Storage before loading them, and"
                                    + " there is no default location to stage them in.",
                            BigQueryConnectorOptions.SINK_FILE_LOADS_STAGING_PATH.key(),
                            BigQueryConnectorOptions.SINK_WRITE_METHOD.key(),
                            WriteMethod.FILE_LOADS));
        }
        FileLoadsOptions.Builder builder = FileLoadsOptions.builder();
        builder.stagingPath(stagingPath.get());

        // Checked here under the DDL key rather than left to FileLoadsOptions' own check, which
        // names the tempDataset(...) setter a SQL caller never wrote (issue #1027, docs/adr/0127).
        config.getOptional(BigQueryConnectorOptions.SINK_FILE_LOADS_TEMP_DATASET)
                .ifPresent(
                        value ->
                                builder.tempDataset(
                                        ResourceNames.checkComponent(
                                                value,
                                                BigQueryConnectorOptions
                                                        .SINK_FILE_LOADS_TEMP_DATASET
                                                        .key())));
        config.getOptional(BigQueryConnectorOptions.SINK_FILE_LOADS_WRITE_DISPOSITION)
                .ifPresent(builder::writeDisposition);
        config.getOptional(BigQueryConnectorOptions.SINK_FILE_LOADS_MIN_CHECKPOINT_INTERVAL)
                .ifPresent(builder::minCheckpointInterval);
        config.getOptional(BigQueryConnectorOptions.SINK_FILE_LOADS_MAX_STAGING_FILE_BYTES)
                .map(MemorySize::getBytes)
                .ifPresent(builder::maxStagingFileBytes);
        config.getOptional(BigQueryConnectorOptions.SINK_FILE_LOADS_STAGING_FORMAT)
                .ifPresent(builder::stagingFormat);
        // Applied unconditionally, so the builder's "only with PARQUET" rule fires for a DDL that
        // sets it under Avro rather than the mapper quietly dropping it.
        config.getOptional(BigQueryConnectorOptions.SINK_FILE_LOADS_PARQUET_COMPRESSION)
                .ifPresent(builder::parquetCompression);

        config.getOptional(BigQueryConnectorOptions.SINK_FILE_LOADS_LOAD_JOB_POLL_INITIAL_BACKOFF)
                .ifPresent(builder::loadJobPollInitialBackoff);
        config.getOptional(BigQueryConnectorOptions.SINK_FILE_LOADS_LOAD_JOB_POLL_MAX_BACKOFF)
                .ifPresent(builder::loadJobPollMaxBackoff);

        // The keys are spelled after the schemaReconcile* setters and getters, which is what
        // keeps them clear of the unrelated sink.schema-update.* family.
        config.getOptional(
                        BigQueryConnectorOptions.SINK_FILE_LOADS_SCHEMA_RECONCILE_INITIAL_BACKOFF)
                .ifPresent(builder::schemaReconcileInitialBackoff);
        config.getOptional(BigQueryConnectorOptions.SINK_FILE_LOADS_SCHEMA_RECONCILE_MAX_BACKOFF)
                .ifPresent(builder::schemaReconcileMaxBackoff);
        config.getOptional(BigQueryConnectorOptions.SINK_FILE_LOADS_SCHEMA_RECONCILE_MAX_ATTEMPTS)
                .ifPresent(builder::schemaReconcileMaxAttempts);

        config.getOptional(BigQueryConnectorOptions.SINK_FILE_LOADS_PER_DESTINATION_METRICS)
                .ifPresent(builder::perDestinationMetrics);

        return builder.build();
    }
}
