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

package io.github.flink.gcp.connector.bigquery.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import io.github.flink.gcp.connector.bigquery.sink.SchemaUpdateOptions;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.WriteDisposition;
import io.github.flink.gcp.connector.bigquery.sink.WriteMethod;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.FileLoadsOptions;
import io.github.flink.gcp.connector.bigquery.sink.storage.BufferedStreamOptions;
import io.github.flink.gcp.connector.bigquery.table.sink.BigQueryDynamicSink;
import io.github.flink.gcp.connector.bigquery.table.sink.BufferedStreamOptionsMapper;
import io.github.flink.gcp.connector.bigquery.table.sink.DefaultStreamOptionsMapper;
import io.github.flink.gcp.connector.bigquery.table.sink.FileLoadsOptionsMapper;
import io.github.flink.gcp.connector.bigquery.table.sink.RowDataSchemaOptions;
import io.github.flink.gcp.connector.bigquery.table.sink.TableCreateOptionsMapper;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Creates the {@code bigquery} table sink from a {@code CREATE TABLE} statement's options.
 *
 * <p>This is the only place a DDL option becomes a value. Value validation stays in the connector's
 * own builders, so a SQL user gets the same message a DataStream user does; what this class owns
 * are the checks whose message has to name <em>option keys</em>, which a builder's cannot. Most are
 * cross-checks against the selected write method, decided from the {@code WITH} clause alone: a
 * tuning family belonging to another one, schema evolution under {@code storage-api-exactly-once},
 * an emulator endpoint under {@code file-loads}, and — in {@code FileLoadsOptionsMapper} — a
 * missing staging path.
 *
 * <p>{@code checkFileLoadsStreamingRules} is the exception, and the only place this class reads the
 * <em>session</em> configuration rather than the table's own options: a non-append write
 * disposition and a checkpoint interval below the connector's floor both depend on {@code
 * execution.runtime-mode}.
 *
 * <p>Every one of them restates a rule {@code BigQuerySinkBuilder} or {@code BigQueryFileLoadsSink}
 * also has, and each of those stays exactly where it is: it is the DataStream API's backstop, not a
 * duplicate of this.
 *
 * <p>The identifier {@code bigquery} is also the Dataproc connector's. A classpath carrying both
 * fails factory discovery loudly, which is the acceptable outcome: the natural name wins.
 */
@Internal
public class BigQueryDynamicTableFactory implements DynamicTableSinkFactory {

    /** The value of {@code 'connector'} that selects this factory. */
    public static final String IDENTIFIER = "bigquery";

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return new HashSet<>(
                Arrays.asList(
                        BigQueryConnectorOptions.PROJECT,
                        BigQueryConnectorOptions.DATASET,
                        BigQueryConnectorOptions.TABLE));
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return new HashSet<>(
                Arrays.asList(
                        BigQueryConnectorOptions.EMULATOR_ENDPOINT,
                        BigQueryConnectorOptions.EMULATOR_REST_ENDPOINT,
                        BigQueryConnectorOptions.SERVICE_ACCOUNT_KEY_FILE,
                        BigQueryConnectorOptions.SINK_WRITE_METHOD,
                        BigQueryConnectorOptions.SINK_CREATE_DISPOSITION,
                        BigQueryConnectorOptions.SINK_LOCATION,
                        BigQueryConnectorOptions.SINK_SCHEMA_UPDATE_ALLOW_NEW_FIELDS,
                        BigQueryConnectorOptions.SINK_SCHEMA_UPDATE_ALLOW_FIELD_RELAXATION,
                        BigQueryConnectorOptions.SINK_TABLE_CREATE_TIME_PARTITIONING_TYPE,
                        BigQueryConnectorOptions.SINK_TABLE_CREATE_TIME_PARTITIONING_FIELD,
                        BigQueryConnectorOptions.SINK_TABLE_CREATE_TIME_PARTITIONING_EXPIRATION,
                        BigQueryConnectorOptions.SINK_TABLE_CREATE_CLUSTERED_FIELDS,
                        BigQueryConnectorOptions.SINK_DERIVE_REQUIRED_COLUMNS,
                        BigQueryConnectorOptions.SINK_JSON_FIELD_PATHS,
                        BigQueryConnectorOptions.SINK_GEOGRAPHY_FIELD_PATHS,
                        BigQueryConnectorOptions.SINK_DEFAULT_STREAM_MAX_APPEND_REQUEST_BYTES,
                        BigQueryConnectorOptions.SINK_DEFAULT_STREAM_RECOVERY_INITIAL_BACKOFF,
                        BigQueryConnectorOptions.SINK_DEFAULT_STREAM_RECOVERY_MAX_BACKOFF,
                        BigQueryConnectorOptions.SINK_DEFAULT_STREAM_RECOVERY_MAX_ATTEMPTS,
                        BigQueryConnectorOptions.SINK_DEFAULT_STREAM_RETRY_INITIAL_DELAY,
                        BigQueryConnectorOptions.SINK_DEFAULT_STREAM_RETRY_DELAY_MULTIPLIER,
                        BigQueryConnectorOptions.SINK_DEFAULT_STREAM_RETRY_MAX_DELAY,
                        BigQueryConnectorOptions.SINK_DEFAULT_STREAM_RETRY_MAX_ATTEMPTS,
                        BigQueryConnectorOptions.SINK_DEFAULT_STREAM_RETRY_MAX_DURATION,
                        BigQueryConnectorOptions.SINK_DEFAULT_STREAM_MAX_INFLIGHT_REQUESTS,
                        BigQueryConnectorOptions.SINK_DEFAULT_STREAM_MAX_INFLIGHT_BYTES,
                        BigQueryConnectorOptions.SINK_DEFAULT_STREAM_MIN_CONNECTIONS_PER_REGION,
                        BigQueryConnectorOptions.SINK_DEFAULT_STREAM_MAX_CONNECTIONS_PER_REGION,
                        BigQueryConnectorOptions.SINK_DEFAULT_STREAM_DESTINATION_IDLE_TIMEOUT,
                        BigQueryConnectorOptions.SINK_DEFAULT_STREAM_FLUSH_INTERVAL,
                        BigQueryConnectorOptions.SINK_DEFAULT_STREAM_PER_DESTINATION_METRICS,
                        BigQueryConnectorOptions.SINK_BUFFERED_STREAM_MAX_APPEND_REQUEST_BYTES,
                        BigQueryConnectorOptions.SINK_BUFFERED_STREAM_RECOVERY_INITIAL_BACKOFF,
                        BigQueryConnectorOptions.SINK_BUFFERED_STREAM_RECOVERY_MAX_BACKOFF,
                        BigQueryConnectorOptions.SINK_BUFFERED_STREAM_RECOVERY_MAX_ATTEMPTS,
                        BigQueryConnectorOptions.SINK_BUFFERED_STREAM_RETRY_INITIAL_DELAY,
                        BigQueryConnectorOptions.SINK_BUFFERED_STREAM_RETRY_DELAY_MULTIPLIER,
                        BigQueryConnectorOptions.SINK_BUFFERED_STREAM_RETRY_MAX_DELAY,
                        BigQueryConnectorOptions.SINK_BUFFERED_STREAM_RETRY_MAX_ATTEMPTS,
                        BigQueryConnectorOptions.SINK_BUFFERED_STREAM_RETRY_MAX_DURATION,
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
                        BigQueryConnectorOptions.SINK_FILE_LOADS_PER_DESTINATION_METRICS,
                        FactoryUtil.SINK_PARALLELISM));
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        helper.validate();

        ReadableConfig config = helper.getOptions();
        // Read once and defaulted once: the checks below need a value, while the sink must not
        // carry a copy of the builder's default (BigQuerySinkBuilder's own field), because an
        // unset option has to leave that setter uncalled.
        Optional<WriteMethod> configuredWriteMethod =
                config.getOptional(BigQueryConnectorOptions.SINK_WRITE_METHOD);
        WriteMethod writeMethod =
                configuredWriteMethod.orElse(WriteMethod.STORAGE_API_AT_LEAST_ONCE);

        // Every rejection this class owns runs before the first mapper, so that the write method
        // being unusable is reported ahead of anything configured under it. The mappers throw too
        // — a table-creation column BigQuery cannot use, a staging path that is not a gs:// URI —
        // and evaluating them inside the builder chain would let those messages arrive first.
        SchemaUpdateOptions schemaUpdateOptions = schemaUpdateOptions(config);
        checkFamiliesMatchTheWriteMethod(config, writeMethod);
        checkSchemaUpdatesAreSupported(config, writeMethod, schemaUpdateOptions);
        checkEmulatorEndpointsAreSupported(config, writeMethod);
        checkCredentials(config);

        // Built from the write method rather than from key presence, unlike the default-stream
        // family: the builder requires each of these for its write method, so a DDL that selects
        // one and tunes nothing still needs the object — and every knob of it is defaulted, which
        // is exactly what "tunes nothing" means. FileLoadsOptionsMapper is also where the missing
        // staging path is reported, which is why it is called here rather than further down.
        BufferedStreamOptions bufferedStreamOptions =
                writeMethod == WriteMethod.STORAGE_API_EXACTLY_ONCE
                        ? BufferedStreamOptionsMapper.map(config)
                        : null;
        FileLoadsOptions fileLoadsOptions =
                writeMethod == WriteMethod.FILE_LOADS ? FileLoadsOptionsMapper.map(config) : null;
        // Below the mapper rather than above it, and it has to be: both rules compare against a
        // FileLoadsOptions knob whose default lives on that builder, so reading the options
        // directly would put a second copy of two defaults in this class. The ordering the
        // rejections above rely on survives — a missing staging path is still reported before
        // this, and TableCreateOptionsMapper still after it.
        checkFileLoadsStreamingRules(context.getConfiguration(), fileLoadsOptions);

        DataType physicalDataType = context.getPhysicalRowDataType();
        return BigQueryDynamicSink.builder()
                .physicalDataType(physicalDataType)
                .destination(
                        TableDestination.of(
                                config.get(BigQueryConnectorOptions.PROJECT),
                                config.get(BigQueryConnectorOptions.DATASET),
                                config.get(BigQueryConnectorOptions.TABLE)))
                .schemaOptions(schemaOptions(config))
                .writeMethod(configuredWriteMethod.orElse(null))
                .createDisposition(
                        config.getOptional(BigQueryConnectorOptions.SINK_CREATE_DISPOSITION)
                                .orElse(null))
                .tableCreateOptions(
                        TableCreateOptionsMapper.map(
                                config, (RowType) physicalDataType.getLogicalType()))
                .location(config.getOptional(BigQueryConnectorOptions.SINK_LOCATION).orElse(null))
                .schemaUpdateOptions(schemaUpdateOptions)
                .defaultStreamOptions(DefaultStreamOptionsMapper.map(config))
                .bufferedStreamOptions(bufferedStreamOptions)
                .fileLoadsOptions(fileLoadsOptions)
                .serviceAccountKeyFile(
                        config.getOptional(BigQueryConnectorOptions.SERVICE_ACCOUNT_KEY_FILE)
                                .orElse(null))
                .emulatorEndpoint(
                        config.getOptional(BigQueryConnectorOptions.EMULATOR_ENDPOINT).orElse(null))
                .emulatorRestEndpoint(
                        config.getOptional(BigQueryConnectorOptions.EMULATOR_REST_ENDPOINT)
                                .orElse(null))
                .parallelism(config.getOptional(FactoryUtil.SINK_PARALLELISM).orElse(null))
                .build();
    }

    private static void checkCredentials(ReadableConfig config) {
        Optional<String> keyFile =
                config.getOptional(BigQueryConnectorOptions.SERVICE_ACCOUNT_KEY_FILE);
        if (keyFile.isPresent() && keyFile.get().isBlank()) {
            throw new ValidationException(
                    "Option '"
                            + BigQueryConnectorOptions.SERVICE_ACCOUNT_KEY_FILE.key()
                            + "' must not be blank.");
        }
        if (keyFile.isPresent()
                && (config.getOptional(BigQueryConnectorOptions.EMULATOR_ENDPOINT).isPresent()
                        || config.getOptional(BigQueryConnectorOptions.EMULATOR_REST_ENDPOINT)
                                .isPresent())) {
            throw new ValidationException(
                    "Option '"
                            + BigQueryConnectorOptions.SERVICE_ACCOUNT_KEY_FILE.key()
                            + "' cannot be combined with '"
                            + BigQueryConnectorOptions.EMULATOR_ENDPOINT.key()
                            + "' or '"
                            + BigQueryConnectorOptions.EMULATOR_REST_ENDPOINT.key()
                            + "': emulator connections are credential-free.");
        }
    }

    /**
     * Rejects keys of a tuning family belonging to a write method other than the selected one.
     *
     * <p>The builder rejects the mismatched options object too, but its message names builder
     * methods, which a SQL user cannot act on — so the keys are named here.
     */
    private static void checkFamiliesMatchTheWriteMethod(
            ReadableConfig config, WriteMethod writeMethod) {
        rejectForeignFamily(
                writeMethod,
                WriteMethod.STORAGE_API_AT_LEAST_ONCE,
                DefaultStreamOptionsMapper.presentKeys(config));
        rejectForeignFamily(
                writeMethod,
                WriteMethod.STORAGE_API_EXACTLY_ONCE,
                BufferedStreamOptionsMapper.presentKeys(config));
        rejectForeignFamily(
                writeMethod, WriteMethod.FILE_LOADS, FileLoadsOptionsMapper.presentKeys(config));
    }

    private static void rejectForeignFamily(
            WriteMethod selected, WriteMethod owner, List<String> present) {
        if (selected == owner || present.isEmpty()) {
            return;
        }
        // "this table's write method is" rather than naming the key as the thing that selected it:
        // the key may be absent, in which case the connector's default is what selected the write
        // method and a message blaming a key the DDL never wrote would send the reader looking for
        // it. The key still appears, in the remedy, which is where it can be acted on.
        throw new ValidationException(
                String.format(
                        "Options %s tune the '%s' write method, but this table's write method is"
                                + " '%s'. Remove them, or set '%s' = '%s'.",
                        present,
                        owner,
                        selected,
                        BigQueryConnectorOptions.SINK_WRITE_METHOD.key(),
                        owner));
    }

    /**
     * Rejects schema evolution under the write method that cannot do it.
     *
     * <p>A buffered stream's schema is pinned when the stream is created, so the builder refuses
     * the pair — naming {@code schemaUpdateOptions(...)}, which is the DataStream API's vocabulary.
     * This says the same thing in keys. It fires on the same condition the builder uses, an
     * <em>enabled</em> options object, so {@code allow-new-fields = false} passes here exactly as
     * it passes there.
     */
    private static void checkSchemaUpdatesAreSupported(
            ReadableConfig config,
            WriteMethod writeMethod,
            @Nullable SchemaUpdateOptions schemaUpdateOptions) {
        if (writeMethod != WriteMethod.STORAGE_API_EXACTLY_ONCE
                || schemaUpdateOptions == null
                || !schemaUpdateOptions.isEnabled()) {
            return;
        }
        throw new ValidationException(
                String.format(
                        "Options %s ask the sink to evolve the table schema, which '%s' = '%s'"
                                + " cannot do: a buffered stream's schema is pinned when the stream"
                                + " is created. Update the table schema out of band and restart the"
                                + " job, or choose another write method.",
                        enabledKeysOf(config),
                        BigQueryConnectorOptions.SINK_WRITE_METHOD.key(),
                        WriteMethod.STORAGE_API_EXACTLY_ONCE));
    }

    /** The schema-update keys the configuration sets to {@code true}, in declaration order. */
    private static List<String> enabledKeysOf(ReadableConfig config) {
        List<String> enabled = new ArrayList<>();
        for (ConfigOption<Boolean> option :
                Arrays.asList(
                        BigQueryConnectorOptions.SINK_SCHEMA_UPDATE_ALLOW_NEW_FIELDS,
                        BigQueryConnectorOptions.SINK_SCHEMA_UPDATE_ALLOW_FIELD_RELAXATION)) {
            if (config.getOptional(option).orElse(false)) {
                enabled.add(option.key());
            }
        }
        return enabled;
    }

    /**
     * Rejects an emulator endpoint under the write method no emulator stands in for.
     *
     * <p>FILE_LOADS stages files to Cloud Storage, which the BigQuery emulator does not provide, so
     * an endpoint would be honored by the metadata half and silently ignored by the half that moves
     * the rows. The builder refuses it naming {@code emulatorEndpoint(...)}; this names the keys.
     */
    private static void checkEmulatorEndpointsAreSupported(
            ReadableConfig config, WriteMethod writeMethod) {
        if (writeMethod != WriteMethod.FILE_LOADS) {
            return;
        }
        List<String> present = new ArrayList<>();
        for (ConfigOption<String> option :
                Arrays.asList(
                        BigQueryConnectorOptions.EMULATOR_ENDPOINT,
                        BigQueryConnectorOptions.EMULATOR_REST_ENDPOINT)) {
            if (config.getOptional(option).isPresent()) {
                present.add(option.key());
            }
        }
        if (present.isEmpty()) {
            return;
        }
        throw new ValidationException(
                String.format(
                        "Options %s point at a BigQuery emulator, which '%s' = '%s' cannot use:"
                                + " that write method stages rows as files on Cloud Storage, which"
                                + " the emulator does not provide.",
                        present,
                        BigQueryConnectorOptions.SINK_WRITE_METHOD.key(),
                        WriteMethod.FILE_LOADS));
    }

    /**
     * Rejects the two {@code file-loads} rules that only apply in streaming execution.
     *
     * <p>{@code BigQueryFileLoadsSink.validateStreaming(...)} holds both, and keeps them: it is the
     * DataStream API's backstop. Its messages name {@code WriteDisposition.WRITE_APPEND} and {@code
     * FileLoadsOptions.minCheckpointInterval(...)}, and mixing the DDL spellings into them was
     * declined — the two vocabularies must not meet inside one message. So the rules are restated
     * here in keys, and this message stays in DDL vocabulary throughout, which is why the runtime
     * mode appears as the literal {@code streaming}/{@code batch} a user writes rather than as
     * {@link RuntimeExecutionMode}'s own spelling.
     *
     * <p>The session configuration is safe to decide from, and by two mechanisms rather than one.
     * The {@code TableConfig} the planner hands this factory falls back to the {@code
     * StreamExecutionEnvironment}'s own {@code Configuration}, the very object its {@code
     * CheckpointConfig} is a view over; and {@code PlannerBase.translate} pushes {@code
     * TableConfig}'s own layer onto that environment with {@code configure(...)}, which is what
     * covers the {@code attachAsDataStream} bridge path where nothing else would. Measured
     * 2026-08-09 on Flink 2.2.1: with the environment at five minutes and {@code TableConfig} at
     * thirty seconds, removing this check makes the sink reject the same plan with the same
     * verdict. What neither mechanism can cover is a value changed <em>after</em> the plan is
     * built, which no plan-time check could; the sink still catches it, in its own vocabulary.
     */
    private static void checkFileLoadsStreamingRules(
            ReadableConfig sessionConfig, @Nullable FileLoadsOptions options) {
        if (options == null) {
            return;
        }
        // What this excludes is BATCH, which takes any disposition and triggers on end of input
        // rather than on checkpoints, so neither rule applies there. AUTOMATIC is refused by
        // Flink's own DefaultPlannerFactory when the TableEnvironment is created (measured
        // 2026-08-09 on Flink 2.2.1, one run), so it arrives only if set on the session
        // afterwards — and the comparison is written against STREAMING rather than against BATCH
        // so that such a mode takes the silent path instead of falling through into rules it was
        // never checked against.
        if (sessionConfig.get(ExecutionOptions.RUNTIME_MODE) != RuntimeExecutionMode.STREAMING) {
            return;
        }
        String runtimeModeKey = ExecutionOptions.RUNTIME_MODE.key();
        if (options.getWriteDisposition() != WriteDisposition.WRITE_APPEND) {
            throw new ValidationException(
                    String.format(
                            "Option '%s' = '%s' cannot be used in streaming execution ('%s' ="
                                    + " 'streaming'), where '%s' = '%s' appends each checkpoint's"
                                    + " rows with a load job of its own: replacing or rejecting the"
                                    + " table on every checkpoint is not a meaningful write. Use"
                                    + " '%s', or run in batch execution ('%s' = 'batch').",
                            BigQueryConnectorOptions.SINK_FILE_LOADS_WRITE_DISPOSITION.key(),
                            options.getWriteDisposition(),
                            runtimeModeKey,
                            BigQueryConnectorOptions.SINK_WRITE_METHOD.key(),
                            WriteMethod.FILE_LOADS,
                            WriteDisposition.WRITE_APPEND,
                            runtimeModeKey));
        }
        // CheckpointConfig.isCheckpointingEnabled() reproduced: the option has no default, and an
        // absent or non-positive interval means checkpointing is off. This rule alone stays silent
        // then, and not out of deference to BigQueryFileLoadsSink's "requires checkpointing"
        // message — it is that an interval that does not exist cannot be compared with a floor.
        // The disposition above has a true answer with or without checkpointing, so it speaks.
        long intervalMs =
                sessionConfig
                        .getOptional(CheckpointingOptions.CHECKPOINTING_INTERVAL)
                        .map(Duration::toMillis)
                        .orElse(-1L);
        long minIntervalMs = options.getMinCheckpointInterval().toMillis();
        if (intervalMs > 0 && intervalMs < minIntervalMs) {
            String intervalKey = CheckpointingOptions.CHECKPOINTING_INTERVAL.key();
            throw new ValidationException(
                    String.format(
                            "'%s' (%d ms) is shorter than the smallest checkpoint interval '%s' ="
                                    + " '%s' accepts in streaming execution (%d ms): BigQuery allows"
                                    + " 1,500 load jobs per table per day and each checkpoint issues"
                                    + " at least one load job per destination table (1 min ="
                                    + " 1,440/day, 2 min = 720/day, 5 min = 288/day). Increase '%s',"
                                    + " or set '%s' lower explicitly for a short-lived job whose"
                                    + " daily load count stays safe.",
                            intervalKey,
                            intervalMs,
                            BigQueryConnectorOptions.SINK_WRITE_METHOD.key(),
                            WriteMethod.FILE_LOADS,
                            minIntervalMs,
                            intervalKey,
                            BigQueryConnectorOptions.SINK_FILE_LOADS_MIN_CHECKPOINT_INTERVAL
                                    .key()));
        }
    }

    private static RowDataSchemaOptions schemaOptions(ReadableConfig config) {
        RowDataSchemaOptions.Builder builder = RowDataSchemaOptions.builder();
        config.getOptional(BigQueryConnectorOptions.SINK_DERIVE_REQUIRED_COLUMNS)
                .ifPresent(builder::deriveRequiredColumns);
        builder.jsonFieldPaths(
                config.getOptional(BigQueryConnectorOptions.SINK_JSON_FIELD_PATHS)
                        .orElse(Collections.emptyList()));
        builder.geographyFieldPaths(
                config.getOptional(BigQueryConnectorOptions.SINK_GEOGRAPHY_FIELD_PATHS)
                        .orElse(Collections.emptyList()));
        return builder.build();
    }

    /**
     * Builds the schema update options, or returns {@code null} when neither key is set — the
     * connector's own default then applies, rather than a copy of it made here.
     */
    private static SchemaUpdateOptions schemaUpdateOptions(ReadableConfig config) {
        Optional<Boolean> newFields =
                config.getOptional(BigQueryConnectorOptions.SINK_SCHEMA_UPDATE_ALLOW_NEW_FIELDS);
        Optional<Boolean> relaxation =
                config.getOptional(
                        BigQueryConnectorOptions.SINK_SCHEMA_UPDATE_ALLOW_FIELD_RELAXATION);
        if (!newFields.isPresent() && !relaxation.isPresent()) {
            return null;
        }
        SchemaUpdateOptions.Builder builder = SchemaUpdateOptions.builder();
        if (newFields.orElse(false)) {
            builder.allowNewFields();
        }
        if (relaxation.orElse(false)) {
            builder.allowFieldRelaxation();
        }
        return builder.build();
    }
}
