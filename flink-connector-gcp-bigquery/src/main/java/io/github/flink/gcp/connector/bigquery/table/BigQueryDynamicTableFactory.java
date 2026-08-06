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
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;

import io.github.flink.gcp.connector.bigquery.sink.SchemaUpdateOptions;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.WriteMethod;
import io.github.flink.gcp.connector.bigquery.table.sink.BigQueryDynamicSink;
import io.github.flink.gcp.connector.bigquery.table.sink.DefaultStreamOptionsMapper;
import io.github.flink.gcp.connector.bigquery.table.sink.RowDataSchemaOptions;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

/**
 * Creates the {@code bigquery} table sink from a {@code CREATE TABLE} statement's options.
 *
 * <p>This is the only place a DDL option becomes a value. Value validation stays in the connector's
 * own builders, so a SQL user gets the same message a DataStream user does; what this class owns
 * are the checks whose message has to name <em>option keys</em>, which a builder's cannot — a
 * required key that is missing, and a key belonging to a write method other than the selected one.
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
                        BigQueryConnectorOptions.SINK_WRITE_METHOD,
                        BigQueryConnectorOptions.SINK_CREATE_DISPOSITION,
                        BigQueryConnectorOptions.SINK_LOCATION,
                        BigQueryConnectorOptions.SINK_SCHEMA_UPDATE_ALLOW_NEW_FIELDS,
                        BigQueryConnectorOptions.SINK_SCHEMA_UPDATE_ALLOW_FIELD_RELAXATION,
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
                        FactoryUtil.SINK_PARALLELISM));
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        helper.validate();

        ReadableConfig config = helper.getOptions();
        checkWriteMethod(config);

        return new BigQueryDynamicSink(
                context.getPhysicalRowDataType(),
                TableDestination.of(
                        config.get(BigQueryConnectorOptions.PROJECT),
                        config.get(BigQueryConnectorOptions.DATASET),
                        config.get(BigQueryConnectorOptions.TABLE)),
                schemaOptions(config),
                config.getOptional(BigQueryConnectorOptions.SINK_CREATE_DISPOSITION).orElse(null),
                config.getOptional(BigQueryConnectorOptions.SINK_LOCATION).orElse(null),
                schemaUpdateOptions(config),
                DefaultStreamOptionsMapper.map(config),
                config.getOptional(BigQueryConnectorOptions.EMULATOR_ENDPOINT).orElse(null),
                config.getOptional(BigQueryConnectorOptions.EMULATOR_REST_ENDPOINT).orElse(null),
                config.getOptional(FactoryUtil.SINK_PARALLELISM).orElse(null));
    }

    /**
     * Rejects a write method this layer does not carry yet, and options belonging to one that was
     * not selected.
     *
     * <p>The builder rejects a mismatched options object too, but its message names builder
     * methods, which a SQL user cannot act on — so the keys are named here.
     */
    private static void checkWriteMethod(ReadableConfig config) {
        WriteMethod writeMethod =
                config.getOptional(BigQueryConnectorOptions.SINK_WRITE_METHOD)
                        .orElse(WriteMethod.STORAGE_API_AT_LEAST_ONCE);
        if (writeMethod != WriteMethod.STORAGE_API_AT_LEAST_ONCE) {
            throw new ValidationException(
                    String.format(
                            "Option '%s' = '%s' is not supported by the '%s' connector yet; only"
                                    + " '%s' is. The other write methods arrive with their own"
                                    + " option families.",
                            BigQueryConnectorOptions.SINK_WRITE_METHOD.key(),
                            writeMethod,
                            IDENTIFIER,
                            WriteMethod.STORAGE_API_AT_LEAST_ONCE));
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
