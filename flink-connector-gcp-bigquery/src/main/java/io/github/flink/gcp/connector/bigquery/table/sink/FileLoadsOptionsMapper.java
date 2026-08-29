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
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;

import io.github.flink.gcp.connector.bigquery.sink.WriteMethod;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.FileLoadsOptions;
import io.github.flink.gcp.connector.bigquery.table.BigQueryConnectorOptions;
import io.github.flink.gcp.connector.bigquery.table.OptionSetters;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

/**
 * Maps the {@code sink.file-loads.*} options onto {@link FileLoadsOptions}.
 *
 * <p>Under the same contract as {@code DefaultStreamOptionsMapper}: every knob is applied through
 * {@link OptionSetters}, no default is introduced, and each bound — a staging path that is not a
 * {@code gs://} URI, a blank temp dataset, a sub-millisecond backoff — stays with the builder,
 * whose rejection is renamed to the option key (issue #1030).
 *
 * <p><b>It always builds</b>, for the reason {@code BufferedStreamOptionsMapper} states: {@code
 * fileLoadsOptions(...)} is <em>required</em> for {@code FILE_LOADS}, so whether an options object
 * is wanted is decided by the write method — which the factory knows — and not by key presence.
 * {@link #presentKeys(ReadableConfig)} survives for the factory's wrong-family check.
 *
 * <p>Two rules are owned here rather than left to the builder. The first fires on an
 * <em>absent</em> option, which no setter ever sees: <b>a missing staging path</b>. {@code
 * FileLoadsOptions.build()} rejects it too, but names {@code stagingPath("gs://...")}, a builder
 * method a SQL user cannot call — and this is the one option on the whole table surface that has no
 * default, so its message is all that stands between a DDL and a write method with nowhere to
 * stage. The second compares two options and therefore names both DDL keys before the builder can
 * reject the same relationship in Java setter vocabulary.
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
                    BigQueryConnectorOptions.SINK_FILE_LOADS_MAX_OPEN_DESTINATIONS,
                    BigQueryConnectorOptions.SINK_FILE_LOADS_MAX_PENDING_FILES,
                    BigQueryConnectorOptions.SINK_FILE_LOADS_DESTINATION_IDLE_TIMEOUT,
                    BigQueryConnectorOptions.SINK_FILE_LOADS_MAX_SERIALIZED_ROW_BYTES,
                    BigQueryConnectorOptions.SINK_FILE_LOADS_STAGING_FORMAT,
                    BigQueryConnectorOptions.SINK_FILE_LOADS_PARQUET_COMPRESSION,
                    BigQueryConnectorOptions.SINK_FILE_LOADS_LOAD_JOB_POLL_INITIAL_BACKOFF,
                    BigQueryConnectorOptions.SINK_FILE_LOADS_LOAD_JOB_POLL_MAX_BACKOFF,
                    BigQueryConnectorOptions.SINK_FILE_LOADS_SCHEMA_RECONCILE_INITIAL_BACKOFF,
                    BigQueryConnectorOptions.SINK_FILE_LOADS_SCHEMA_RECONCILE_MAX_BACKOFF,
                    BigQueryConnectorOptions.SINK_FILE_LOADS_SCHEMA_RECONCILE_MAX_ATTEMPTS,
                    BigQueryConnectorOptions.SINK_FILE_LOADS_METRICS_PER_DESTINATION);

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
        OptionSetters.accept(
                BigQueryConnectorOptions.SINK_FILE_LOADS_STAGING_PATH.key(),
                stagingPath.get(),
                builder::stagingPath);

        OptionSetters.apply(
                config,
                BigQueryConnectorOptions.SINK_FILE_LOADS_TEMP_DATASET,
                builder::tempDataset);
        OptionSetters.apply(
                config,
                BigQueryConnectorOptions.SINK_FILE_LOADS_WRITE_DISPOSITION,
                builder::writeDisposition);
        OptionSetters.apply(
                config,
                BigQueryConnectorOptions.SINK_FILE_LOADS_MIN_CHECKPOINT_INTERVAL,
                builder::minCheckpointInterval);
        OptionSetters.apply(
                config,
                BigQueryConnectorOptions.SINK_FILE_LOADS_MAX_STAGING_FILE_BYTES,
                size -> builder.maxStagingFileBytes(size.getBytes()));
        OptionSetters.apply(
                config,
                BigQueryConnectorOptions.SINK_FILE_LOADS_MAX_OPEN_DESTINATIONS,
                builder::maxOpenDestinations);
        OptionSetters.apply(
                config,
                BigQueryConnectorOptions.SINK_FILE_LOADS_MAX_PENDING_FILES,
                builder::maxPendingFiles);
        OptionSetters.apply(
                config,
                BigQueryConnectorOptions.SINK_FILE_LOADS_DESTINATION_IDLE_TIMEOUT,
                builder::destinationIdleTimeout);
        OptionSetters.apply(
                config,
                BigQueryConnectorOptions.SINK_FILE_LOADS_MAX_SERIALIZED_ROW_BYTES,
                size -> builder.maxSerializedRowBytes(size.getBytes()));
        OptionSetters.apply(
                config,
                BigQueryConnectorOptions.SINK_FILE_LOADS_STAGING_FORMAT,
                builder::stagingFormat);
        // Applied unconditionally, so the builder's "only with PARQUET" rule fires for a DDL that
        // sets it under Avro rather than the mapper quietly dropping it.
        OptionSetters.apply(
                config,
                BigQueryConnectorOptions.SINK_FILE_LOADS_PARQUET_COMPRESSION,
                builder::parquetCompression);

        OptionSetters.apply(
                config,
                BigQueryConnectorOptions.SINK_FILE_LOADS_LOAD_JOB_POLL_INITIAL_BACKOFF,
                builder::loadJobPollInitialBackoff);
        OptionSetters.apply(
                config,
                BigQueryConnectorOptions.SINK_FILE_LOADS_LOAD_JOB_POLL_MAX_BACKOFF,
                builder::loadJobPollMaxBackoff);

        // The keys are spelled after the schemaReconcile* setters and getters, which is what
        // keeps them clear of the unrelated sink.schema-update.* family.
        OptionSetters.apply(
                config,
                BigQueryConnectorOptions.SINK_FILE_LOADS_SCHEMA_RECONCILE_INITIAL_BACKOFF,
                builder::schemaReconcileInitialBackoff);
        OptionSetters.apply(
                config,
                BigQueryConnectorOptions.SINK_FILE_LOADS_SCHEMA_RECONCILE_MAX_BACKOFF,
                builder::schemaReconcileMaxBackoff);
        OptionSetters.apply(
                config,
                BigQueryConnectorOptions.SINK_FILE_LOADS_SCHEMA_RECONCILE_MAX_ATTEMPTS,
                builder::schemaReconcileMaxAttempts);

        OptionSetters.apply(
                config,
                BigQueryConnectorOptions.SINK_FILE_LOADS_METRICS_PER_DESTINATION,
                builder::perDestinationMetrics);

        int maxOpenDestinations =
                config.getOptional(BigQueryConnectorOptions.SINK_FILE_LOADS_MAX_OPEN_DESTINATIONS)
                        .orElse(FileLoadsOptions.DEFAULT_MAX_OPEN_DESTINATIONS);
        int maxPendingFiles =
                config.getOptional(BigQueryConnectorOptions.SINK_FILE_LOADS_MAX_PENDING_FILES)
                        .orElse(FileLoadsOptions.DEFAULT_MAX_PENDING_FILES);
        if (maxPendingFiles < maxOpenDestinations) {
            throw new ValidationException(
                    String.format(
                            "Option '%s' (%d) must be at least option '%s' (%d).",
                            BigQueryConnectorOptions.SINK_FILE_LOADS_MAX_PENDING_FILES.key(),
                            maxPendingFiles,
                            BigQueryConnectorOptions.SINK_FILE_LOADS_MAX_OPEN_DESTINATIONS.key(),
                            maxOpenDestinations));
        }

        return builder.build();
    }
}
