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

package io.github.flink.gcp.connector.datastore.table.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Preconditions;

import io.github.flink.gcp.connector.datastore.DatabaseDestination;
import io.github.flink.gcp.connector.datastore.sink.DatastoreSink;
import io.github.flink.gcp.connector.datastore.sink.DatastoreSinkBuilder;
import io.github.flink.gcp.connector.datastore.sink.DatastoreWriterOptions;
import io.github.flink.gcp.connector.datastore.table.DatastoreTableLineage;
import io.github.flink.gcp.connector.datastore.table.DatastoreTableSchema;

import javax.annotation.Nullable;

import java.util.Objects;

/**
 * The {@code datastore} table sink, backed by the DataStream {@code DatastoreSink}.
 *
 * <p>The sink keeps that sink's default for what a {@code WITH} clause cannot express: a refused
 * write fails the job, as no DDL can name a serializable failure handler.
 */
@Internal
public final class DatastoreDynamicSink implements DynamicTableSink {

    private final DatastoreTableSchema schema;
    private final DatabaseDestination database;
    private final String namespace;
    private final String kind;
    private final DatastoreWriterOptions writerOptions;
    @Nullable private final String emulatorEndpoint;
    @Nullable private final String serviceAccountKeyFile;
    @Nullable private final Integer parallelism;
    private final DatastoreTableLineage lineage;

    private DatastoreDynamicSink(Builder builder, DatastoreTableLineage lineage) {
        this.schema = Preconditions.checkNotNull(builder.schema, "schema must not be null");
        this.database = Preconditions.checkNotNull(builder.database, "database must not be null");
        this.namespace =
                Preconditions.checkNotNull(builder.namespace, "namespace must not be null");
        this.kind = Preconditions.checkNotNull(builder.kind, "kind must not be null");
        this.writerOptions =
                Preconditions.checkNotNull(builder.writerOptions, "writerOptions must not be null");
        this.emulatorEndpoint = builder.emulatorEndpoint;
        this.serviceAccountKeyFile = builder.serviceAccountKeyFile;
        this.parallelism = builder.parallelism;
        this.lineage = Preconditions.checkNotNull(lineage, "lineage must not be null");
    }

    /** Returns a builder for the sink. */
    public static Builder builder() {
        return new Builder();
    }

    /** Insert-only without a PRIMARY KEY; an upsert changelog with one. */
    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        if (!schema.hasPrimaryKey() || requestedMode.containsOnly(RowKind.INSERT)) {
            return ChangelogMode.insertOnly();
        }
        return CrossVersionChangelogMode.upsert();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        DatastoreSinkBuilder<RowData> builder =
                DatastoreSink.<RowData>builder()
                        .database(database)
                        .serializer(
                                new RowDataSerializationSchema(schema, database, namespace, kind))
                        .writerOptions(writerOptions);
        if (emulatorEndpoint != null) {
            builder.emulatorEndpoint(emulatorEndpoint);
        }
        if (serviceAccountKeyFile != null) {
            builder.serviceAccountKeyFile(serviceAccountKeyFile);
        }
        Sink<RowData> sink = builder.build();
        return SinkV2Provider.of(lineage.sink(sink), parallelism);
    }

    @Override
    public DynamicTableSink copy() {
        return builder()
                .schema(schema)
                .database(database)
                .namespace(namespace)
                .kind(kind)
                .writerOptions(writerOptions)
                .emulatorEndpoint(emulatorEndpoint)
                .serviceAccountKeyFile(serviceAccountKeyFile)
                .parallelism(parallelism)
                .build(lineage);
    }

    @Override
    public String asSummaryString() {
        return "Datastore table sink";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DatastoreDynamicSink that = (DatastoreDynamicSink) o;
        return schema.equals(that.schema)
                && database.equals(that.database)
                && namespace.equals(that.namespace)
                && kind.equals(that.kind)
                && writerOptions.equals(that.writerOptions)
                && Objects.equals(emulatorEndpoint, that.emulatorEndpoint)
                && Objects.equals(serviceAccountKeyFile, that.serviceAccountKeyFile)
                && Objects.equals(parallelism, that.parallelism)
                && lineage.equals(that.lineage);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                schema,
                database,
                namespace,
                kind,
                writerOptions,
                emulatorEndpoint,
                serviceAccountKeyFile,
                parallelism,
                lineage);
    }

    /** Collects the values of the immutable sink. */
    public static final class Builder {
        private DatastoreTableSchema schema;
        private DatabaseDestination database;
        private String namespace = "";
        private String kind;
        private DatastoreWriterOptions writerOptions;
        @Nullable private String emulatorEndpoint;
        @Nullable private String serviceAccountKeyFile;
        @Nullable private Integer parallelism;

        private Builder() {}

        /** Sets the checked table schema. */
        public Builder schema(DatastoreTableSchema schema) {
            this.schema = schema;
            return this;
        }

        /** Sets the database. */
        public Builder database(DatabaseDestination database) {
            this.database = database;
            return this;
        }

        /** Sets the entities' namespace, empty for the default one. */
        public Builder namespace(String namespace) {
            this.namespace = namespace;
            return this;
        }

        /** Sets the entities' kind. */
        public Builder kind(String kind) {
            this.kind = kind;
            return this;
        }

        /** Sets the writer options. */
        public Builder writerOptions(DatastoreWriterOptions writerOptions) {
            this.writerOptions = writerOptions;
            return this;
        }

        /** Sets the emulator endpoint, or {@code null} for the service. */
        public Builder emulatorEndpoint(@Nullable String emulatorEndpoint) {
            this.emulatorEndpoint = emulatorEndpoint;
            return this;
        }

        /** Sets the service-account key file, or {@code null} for application default ones. */
        public Builder serviceAccountKeyFile(@Nullable String serviceAccountKeyFile) {
            this.serviceAccountKeyFile = serviceAccountKeyFile;
            return this;
        }

        /** Sets the sink parallelism, or {@code null} for the planner's. */
        public Builder parallelism(@Nullable Integer parallelism) {
            this.parallelism = parallelism;
            return this;
        }

        /** Builds the sink, reporting the given lineage. */
        public DatastoreDynamicSink build(DatastoreTableLineage lineage) {
            return new DatastoreDynamicSink(this, lineage);
        }
    }
}
