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

package io.github.flink.gcp.connector.firestore.table.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Preconditions;

import io.github.flink.gcp.connector.firestore.DatabaseDestination;
import io.github.flink.gcp.connector.firestore.sink.FirestoreSink;
import io.github.flink.gcp.connector.firestore.sink.FirestoreSinkBuilder;
import io.github.flink.gcp.connector.firestore.sink.FirestoreWriterOptions;
import io.github.flink.gcp.connector.firestore.table.FirestoreTableLineage;
import io.github.flink.gcp.connector.firestore.table.FirestoreTableSchema;
import io.github.flink.gcp.connector.firestore.table.WriteMode;

import javax.annotation.Nullable;

import java.util.Objects;

/**
 * The {@code firestore} table sink, backed by the DataStream {@code FirestoreSink}.
 *
 * <p>The sink keeps that sink's defaults for what a {@code WITH} clause cannot express: a refused
 * write fails the job, as no DDL can name a serializable failure handler, and no write carries a
 * precondition.
 */
@Internal
public final class FirestoreDynamicSink implements DynamicTableSink {

    private final FirestoreTableSchema schema;
    private final DatabaseDestination database;
    private final String collection;
    private final WriteMode writeMode;
    private final FirestoreWriterOptions writerOptions;
    @Nullable private final String emulatorEndpoint;
    @Nullable private final String serviceAccountKeyFile;
    @Nullable private final Integer parallelism;
    @Nullable private final FirestoreTableLineage lineage;

    private FirestoreDynamicSink(Builder builder, @Nullable FirestoreTableLineage lineage) {
        this.schema = Preconditions.checkNotNull(builder.schema, "schema must not be null");
        this.database = Preconditions.checkNotNull(builder.database, "database must not be null");
        this.collection =
                Preconditions.checkNotNull(builder.collection, "collection must not be null");
        this.writeMode =
                Preconditions.checkNotNull(builder.writeMode, "writeMode must not be null");
        this.writerOptions =
                Preconditions.checkNotNull(builder.writerOptions, "writerOptions must not be null");
        this.emulatorEndpoint = builder.emulatorEndpoint;
        this.serviceAccountKeyFile = builder.serviceAccountKeyFile;
        this.parallelism = builder.parallelism;
        this.lineage = lineage;
    }

    /** Returns a builder for the sink. */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Insert-only without a PRIMARY KEY; an upsert changelog with one, except under {@code update},
     * which takes inserts and updates but no deletes. An {@code update} fails the job on a missing
     * document, and the sink keeps neither the order of one document's writes nor its writes'
     * application across a replay, so a delete in the input would let an update meet the document
     * it deleted, and fail again on every restart. Leaving deletes out makes the planner refuse
     * such an input when the statement is planned.
     */
    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        if (!schema.hasPrimaryKey() || requestedMode.containsOnly(RowKind.INSERT)) {
            return ChangelogMode.insertOnly();
        }
        if (writeMode == WriteMode.UPDATE) {
            return ChangelogMode.newBuilder()
                    .addContainedKind(RowKind.INSERT)
                    .addContainedKind(RowKind.UPDATE_AFTER)
                    .build();
        }
        return CrossVersionChangelogMode.upsert();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        FirestoreSinkBuilder<RowData> builder =
                FirestoreSink.<RowData>builder()
                        .database(database)
                        .serializer(new RowDataSerializationSchema(schema, collection, writeMode))
                        .writerOptions(writerOptions);
        if (emulatorEndpoint != null) {
            builder.emulatorEndpoint(emulatorEndpoint);
        }
        if (serviceAccountKeyFile != null) {
            builder.serviceAccountKeyFile(serviceAccountKeyFile);
        }
        Sink<RowData> sink = builder.build();
        return SinkV2Provider.of(lineage == null ? sink : lineage.sink(sink), parallelism);
    }

    @Override
    public DynamicTableSink copy() {
        return builder()
                .schema(schema)
                .database(database)
                .collection(collection)
                .writeMode(writeMode)
                .writerOptions(writerOptions)
                .emulatorEndpoint(emulatorEndpoint)
                .serviceAccountKeyFile(serviceAccountKeyFile)
                .parallelism(parallelism)
                .build(lineage);
    }

    @Override
    public String asSummaryString() {
        return "Firestore table sink";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FirestoreDynamicSink that = (FirestoreDynamicSink) o;
        return schema.equals(that.schema)
                && database.equals(that.database)
                && collection.equals(that.collection)
                && writeMode == that.writeMode
                && writerOptions.equals(that.writerOptions)
                && Objects.equals(emulatorEndpoint, that.emulatorEndpoint)
                && Objects.equals(serviceAccountKeyFile, that.serviceAccountKeyFile)
                && Objects.equals(parallelism, that.parallelism)
                && Objects.equals(lineage, that.lineage);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                schema,
                database,
                collection,
                writeMode,
                writerOptions,
                emulatorEndpoint,
                serviceAccountKeyFile,
                parallelism,
                lineage);
    }

    /** Collects the values of the immutable sink. */
    public static final class Builder {
        private FirestoreTableSchema schema;
        private DatabaseDestination database;
        private String collection;
        private WriteMode writeMode = WriteMode.SET;
        private FirestoreWriterOptions writerOptions;
        @Nullable private String emulatorEndpoint;
        @Nullable private String serviceAccountKeyFile;
        @Nullable private Integer parallelism;

        private Builder() {}

        /** Sets the checked table schema. */
        public Builder schema(FirestoreTableSchema schema) {
            this.schema = schema;
            return this;
        }

        /** Sets the database. */
        public Builder database(DatabaseDestination database) {
            this.database = database;
            return this;
        }

        /** Sets the collection path the documents are written into. */
        public Builder collection(String collection) {
            this.collection = collection;
            return this;
        }

        /** Sets what an insert or an update-after does for a table with a PRIMARY KEY. */
        public Builder writeMode(WriteMode writeMode) {
            this.writeMode = writeMode;
            return this;
        }

        /** Sets the writer options. */
        public Builder writerOptions(FirestoreWriterOptions writerOptions) {
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

        /** Builds the sink without lineage. */
        public FirestoreDynamicSink build() {
            return build(null);
        }

        /** Builds the sink, reporting the given lineage. */
        public FirestoreDynamicSink build(@Nullable FirestoreTableLineage lineage) {
            return new FirestoreDynamicSink(this, lineage);
        }
    }
}
