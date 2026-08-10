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

package io.github.flink.gcp.connector.bigtable.table.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Preconditions;

import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.sink.BigtableSink;
import io.github.flink.gcp.connector.bigtable.sink.BigtableSinkBuilder;
import io.github.flink.gcp.connector.bigtable.sink.BigtableWriterOptions;
import io.github.flink.gcp.connector.bigtable.sink.CreateDisposition;
import io.github.flink.gcp.connector.bigtable.sink.TableCreateOptions;
import io.github.flink.gcp.connector.bigtable.table.BigtableTableSchema;

import javax.annotation.Nullable;

import java.util.Objects;

/**
 * The {@code bigtable} table sink.
 *
 * <p>Deliberately takes no {@code ReadableConfig}: turning a DDL option into a value happens in one
 * place, {@link io.github.flink.gcp.connector.bigtable.table.BigtableDynamicTableFactory the
 * factory}, so this class has no configuration vocabulary at all.
 *
 * <p>Two abilities are <b>not</b> implemented. {@code SupportsPartitioning}: a Bigtable table is
 * partitioned by row-key range, which the service chooses and moves on its own, so there is nothing
 * a {@code PARTITIONED BY} clause could name. {@code SupportsWritingMetadata}: the one piece of
 * envelope a mutation has is the cell timestamp, which is deferred to a follow-up issue rather than
 * being decided under this one.
 */
@Internal
public final class BigtableDynamicSink implements DynamicTableSink {

    private final BigtableTableSchema schema;
    private final TableDestination destination;
    private final String nullStringLiteral;
    @Nullable private final String appProfileId;
    private final BigtableWriterOptions writerOptions;
    @Nullable private final CreateDisposition createDisposition;
    @Nullable private final TableCreateOptions tableCreateOptions;
    @Nullable private final String emulatorEndpoint;
    @Nullable private final Integer parallelism;
    private final boolean primaryKeyDeclared;

    /**
     * Creates the sink from fully resolved values.
     *
     * @param schema the table's parsed DDL model
     * @param destination the destination table
     * @param nullStringLiteral the cell value standing for a null character string
     * @param appProfileId the app profile the writes are attributed to, or {@code null}
     * @param writerOptions the writer tuning
     * @param createDisposition the create disposition, or {@code null} for the connector's default
     * @param tableCreateOptions the creation settings, or {@code null} when the sink creates
     *     nothing
     * @param emulatorEndpoint the emulator's endpoint, or {@code null} for the real service
     * @param parallelism the sink parallelism, or {@code null} for the planner's own
     * @param primaryKeyDeclared whether the DDL declared a {@code PRIMARY KEY}, which the factory
     *     has already held to be the row-key column alone
     */
    public BigtableDynamicSink(
            BigtableTableSchema schema,
            TableDestination destination,
            String nullStringLiteral,
            @Nullable String appProfileId,
            BigtableWriterOptions writerOptions,
            @Nullable CreateDisposition createDisposition,
            @Nullable TableCreateOptions tableCreateOptions,
            @Nullable String emulatorEndpoint,
            @Nullable Integer parallelism,
            boolean primaryKeyDeclared) {
        this.schema = Preconditions.checkNotNull(schema, "schema must not be null");
        this.destination = Preconditions.checkNotNull(destination, "destination must not be null");
        this.nullStringLiteral =
                Preconditions.checkNotNull(nullStringLiteral, "nullStringLiteral must not be null");
        this.appProfileId = appProfileId;
        this.writerOptions =
                Preconditions.checkNotNull(writerOptions, "writerOptions must not be null");
        this.createDisposition = createDisposition;
        this.tableCreateOptions = tableCreateOptions;
        this.emulatorEndpoint = emulatorEndpoint;
        this.parallelism = parallelism;
        this.primaryKeyDeclared = primaryKeyDeclared;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        // A Bigtable write is an upsert on the row key by construction: setCell overwrites, and
        // there is no append-only path to offer instead.
        //
        // Whether a delete may carry the upsert key *alone* is a different question, and the
        // answer is whether that key is the row key. A declared PRIMARY KEY guarantees it, the
        // factory having already refused one naming anything else; with none declared the planner
        // keys its upserts on whatever the query happens to be unique by, and a key-only delete
        // then reaches RowDataSerializationSchema with the row-key column null (#470).
        //
        // Measured on Flink 2.2.1 against an upsert source keyed on a non-row-key column: with a
        // key declared the plan already carries ChangelogNormalize and upsertMaterialize, so the
        // row key is filled in either way; with none, answering false is what puts
        // ChangelogNormalize there. An insert-only query into the same table gets neither, so the
        // honest answer is free wherever there is no delete to complete.
        return CrossVersionChangelogMode.upsert(primaryKeyDeclared);
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        BigtableSinkBuilder<RowData> builder =
                BigtableSink.<RowData>builder()
                        .table(destination)
                        .serializer(new RowDataSerializationSchema(schema, nullStringLiteral))
                        .writerOptions(writerOptions);
        if (appProfileId != null) {
            builder.appProfileId(appProfileId);
        }
        if (createDisposition != null) {
            builder.createDisposition(createDisposition);
        }
        if (tableCreateOptions != null) {
            builder.tableCreateOptions(tableCreateOptions);
        }
        if (emulatorEndpoint != null) {
            builder.emulatorEndpoint(emulatorEndpoint);
        }
        Sink<RowData> sink = builder.build();
        return SinkV2Provider.of(sink, parallelism);
    }

    @Override
    public DynamicTableSink copy() {
        return new BigtableDynamicSink(
                schema,
                destination,
                nullStringLiteral,
                appProfileId,
                writerOptions,
                createDisposition,
                tableCreateOptions,
                emulatorEndpoint,
                parallelism,
                primaryKeyDeclared);
    }

    @Override
    public String asSummaryString() {
        return "Bigtable table sink";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BigtableDynamicSink that = (BigtableDynamicSink) o;
        return schema.equals(that.schema)
                && destination.equals(that.destination)
                && nullStringLiteral.equals(that.nullStringLiteral)
                && Objects.equals(appProfileId, that.appProfileId)
                && writerOptions.equals(that.writerOptions)
                && createDisposition == that.createDisposition
                && Objects.equals(tableCreateOptions, that.tableCreateOptions)
                && Objects.equals(emulatorEndpoint, that.emulatorEndpoint)
                && Objects.equals(parallelism, that.parallelism)
                && primaryKeyDeclared == that.primaryKeyDeclared;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                schema,
                destination,
                nullStringLiteral,
                appProfileId,
                writerOptions,
                createDisposition,
                tableCreateOptions,
                emulatorEndpoint,
                parallelism,
                primaryKeyDeclared);
    }
}
