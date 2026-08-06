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

package io.github.flink.gcp.connector.bigquery.table.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import io.github.flink.gcp.connector.bigquery.sink.BigQuerySink;
import io.github.flink.gcp.connector.bigquery.sink.BigQuerySinkBuilder;
import io.github.flink.gcp.connector.bigquery.sink.CreateDisposition;
import io.github.flink.gcp.connector.bigquery.sink.SchemaUpdateOptions;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.storage.DefaultStreamOptions;

import javax.annotation.Nullable;

import java.util.Objects;

/**
 * The {@code bigquery} table sink.
 *
 * <p>Deliberately takes no {@code ReadableConfig}: turning a DDL option into a value happens in one
 * place, {@link io.github.flink.gcp.connector.bigquery.table.BigQueryDynamicTableFactory the
 * factory}, so this class has no configuration vocabulary at all.
 *
 * <p>Three abilities are <b>not</b> implemented, each for its own reason. {@code
 * SupportsPartitioning}: Flink's {@code PARTITIONED BY} models Hive-style value partitioning, which
 * BigQuery time partitioning is not, and ingestion-time partitioning has no column to name — so a
 * partition spec fails at plan time rather than being silently ignored, and everything goes through
 * {@code sink.table-create.*}. {@code SupportsOverwrite}: {@code INSERT OVERWRITE} has no meaning
 * for the Storage Write API, while {@code WRITE_TRUNCATE} stays reachable as a FILE_LOADS option.
 * {@code SupportsWritingMetadata}: a BigQuery row has no envelope around it to expose.
 */
@Internal
public final class BigQueryDynamicSink implements DynamicTableSink {

    private final DataType physicalDataType;
    private final TableDestination destination;
    private final RowDataSchemaOptions schemaOptions;
    @Nullable private final CreateDisposition createDisposition;
    @Nullable private final String location;
    @Nullable private final SchemaUpdateOptions schemaUpdateOptions;
    @Nullable private final DefaultStreamOptions defaultStreamOptions;
    @Nullable private final String emulatorEndpoint;
    @Nullable private final String emulatorRestEndpoint;
    @Nullable private final Integer parallelism;

    /**
     * Creates the sink from fully resolved values.
     *
     * @param physicalDataType the physical columns of the table
     * @param destination the destination table
     * @param schemaOptions how the columns derive a BigQuery schema
     * @param createDisposition the create disposition, or {@code null} to leave it at the
     *     connector's default
     * @param location the BigQuery location, or {@code null}
     * @param schemaUpdateOptions the schema update options, or {@code null}
     * @param defaultStreamOptions the default-stream tuning, or {@code null}
     * @param emulatorEndpoint the emulator's gRPC endpoint, or {@code null}
     * @param emulatorRestEndpoint the emulator's REST endpoint, or {@code null}
     * @param parallelism the sink parallelism, or {@code null} for the planner's own
     */
    public BigQueryDynamicSink(
            DataType physicalDataType,
            TableDestination destination,
            RowDataSchemaOptions schemaOptions,
            @Nullable CreateDisposition createDisposition,
            @Nullable String location,
            @Nullable SchemaUpdateOptions schemaUpdateOptions,
            @Nullable DefaultStreamOptions defaultStreamOptions,
            @Nullable String emulatorEndpoint,
            @Nullable String emulatorRestEndpoint,
            @Nullable Integer parallelism) {
        this.physicalDataType =
                Preconditions.checkNotNull(physicalDataType, "physicalDataType must not be null");
        this.destination = Preconditions.checkNotNull(destination, "destination must not be null");
        this.schemaOptions =
                Preconditions.checkNotNull(schemaOptions, "schemaOptions must not be null");
        this.createDisposition = createDisposition;
        this.location = location;
        this.schemaUpdateOptions = schemaUpdateOptions;
        this.defaultStreamOptions = defaultStreamOptions;
        this.emulatorEndpoint = emulatorEndpoint;
        this.emulatorRestEndpoint = emulatorRestEndpoint;
        this.parallelism = parallelism;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        // BigQuery's append-only write paths cannot express a retraction, so an updating query is
        // rejected at plan time rather than having its -U and -D rows appended as ordinary ones.
        // Upserts are their own issue (#65).
        return ChangelogMode.insertOnly();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        RowType rowType = (RowType) physicalDataType.getLogicalType();
        BigQuerySinkBuilder<RowData> builder =
                BigQuerySink.<RowData>builder()
                        .destination(destination)
                        .serializer(new RowDataSerializer(rowType, schemaOptions));
        if (createDisposition != null) {
            builder.createDisposition(createDisposition);
        }
        if (location != null) {
            builder.location(location);
        }
        if (schemaUpdateOptions != null) {
            builder.schemaUpdateOptions(schemaUpdateOptions);
        }
        if (defaultStreamOptions != null) {
            builder.defaultStreamOptions(defaultStreamOptions);
        }
        if (emulatorEndpoint != null) {
            builder.emulatorEndpoint(emulatorEndpoint);
        }
        if (emulatorRestEndpoint != null) {
            builder.emulatorRestEndpoint(emulatorRestEndpoint);
        }
        Sink<RowData> sink = builder.build();
        return SinkV2Provider.of(sink, parallelism);
    }

    @Override
    public DynamicTableSink copy() {
        return new BigQueryDynamicSink(
                physicalDataType,
                destination,
                schemaOptions,
                createDisposition,
                location,
                schemaUpdateOptions,
                defaultStreamOptions,
                emulatorEndpoint,
                emulatorRestEndpoint,
                parallelism);
    }

    @Override
    public String asSummaryString() {
        return "BigQuery table sink";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BigQueryDynamicSink that = (BigQueryDynamicSink) o;
        return physicalDataType.equals(that.physicalDataType)
                && destination.equals(that.destination)
                && schemaOptions.equals(that.schemaOptions)
                && createDisposition == that.createDisposition
                && Objects.equals(location, that.location)
                && Objects.equals(schemaUpdateOptions, that.schemaUpdateOptions)
                && Objects.equals(defaultStreamOptions, that.defaultStreamOptions)
                && Objects.equals(emulatorEndpoint, that.emulatorEndpoint)
                && Objects.equals(emulatorRestEndpoint, that.emulatorRestEndpoint)
                && Objects.equals(parallelism, that.parallelism);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                physicalDataType,
                destination,
                schemaOptions,
                createDisposition,
                location,
                schemaUpdateOptions,
                defaultStreamOptions,
                emulatorEndpoint,
                emulatorRestEndpoint,
                parallelism);
    }
}
