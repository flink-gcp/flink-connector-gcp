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

package io.github.flink.gcp.connector.spanner.table.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Preconditions;

import io.github.flink.gcp.connector.spanner.SpannerDatabase;
import io.github.flink.gcp.connector.spanner.sink.SpannerSink;
import io.github.flink.gcp.connector.spanner.sink.SpannerSinkBuilder;
import io.github.flink.gcp.connector.spanner.sink.SpannerWriterOptions;
import io.github.flink.gcp.connector.spanner.table.SpannerTableSchemaConverter;

import javax.annotation.Nullable;

import java.util.Objects;

/** The {@code spanner} table sink, backed by the DataStream {@link SpannerSink}. */
@Internal
public final class SpannerDynamicSink implements DynamicTableSink {

    private final SpannerTableSchemaConverter schema;
    private final SpannerDatabase database;
    private final String table;
    private final SpannerWriterOptions writerOptions;
    @Nullable private final String emulatorEndpoint;
    @Nullable private final String serviceAccountKeyFile;
    @Nullable private final Integer parallelism;

    private SpannerDynamicSink(Builder builder) {
        this.schema = Preconditions.checkNotNull(builder.schema, "schema must not be null");
        this.database = Preconditions.checkNotNull(builder.database, "database must not be null");
        this.table = Preconditions.checkNotNull(builder.table, "table must not be null");
        this.writerOptions =
                Preconditions.checkNotNull(builder.writerOptions, "writerOptions must not be null");
        this.emulatorEndpoint = builder.emulatorEndpoint;
        this.serviceAccountKeyFile = builder.serviceAccountKeyFile;
        this.parallelism = builder.parallelism;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        if (!schema.hasPrimaryKey() || requestedMode.containsOnly(RowKind.INSERT)) {
            return ChangelogMode.insertOnly();
        }
        return CrossVersionChangelogMode.upsert();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        SpannerSinkBuilder<RowData> builder =
                SpannerSink.<RowData>builder()
                        .database(database)
                        .serializer(new RowDataSerializationSchema(schema, table))
                        .writerOptions(writerOptions);
        if (emulatorEndpoint != null) {
            builder.emulatorEndpoint(emulatorEndpoint);
        }
        if (serviceAccountKeyFile != null) {
            builder.serviceAccountKeyFile(serviceAccountKeyFile);
        }
        Sink<RowData> sink = builder.build();
        return SinkV2Provider.of(sink, parallelism);
    }

    @Override
    public DynamicTableSink copy() {
        return builder()
                .schema(schema)
                .database(database)
                .table(table)
                .writerOptions(writerOptions)
                .emulatorEndpoint(emulatorEndpoint)
                .serviceAccountKeyFile(serviceAccountKeyFile)
                .parallelism(parallelism)
                .build();
    }

    @Override
    public String asSummaryString() {
        return "Spanner table sink";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SpannerDynamicSink that = (SpannerDynamicSink) o;
        return schema.equals(that.schema)
                && database.equals(that.database)
                && table.equals(that.table)
                && writerOptions.equals(that.writerOptions)
                && Objects.equals(emulatorEndpoint, that.emulatorEndpoint)
                && Objects.equals(serviceAccountKeyFile, that.serviceAccountKeyFile)
                && Objects.equals(parallelism, that.parallelism);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                schema,
                database,
                table,
                writerOptions,
                emulatorEndpoint,
                serviceAccountKeyFile,
                parallelism);
    }

    /** Collects values for the immutable sink. */
    public static final class Builder {
        private SpannerTableSchemaConverter schema;
        private SpannerDatabase database;
        private String table;
        private SpannerWriterOptions writerOptions;
        @Nullable private String emulatorEndpoint;
        @Nullable private String serviceAccountKeyFile;
        @Nullable private Integer parallelism;

        private Builder() {}

        public Builder schema(SpannerTableSchemaConverter schema) {
            this.schema = schema;
            return this;
        }

        public Builder database(SpannerDatabase database) {
            this.database = database;
            return this;
        }

        public Builder table(String table) {
            this.table = table;
            return this;
        }

        public Builder writerOptions(SpannerWriterOptions writerOptions) {
            this.writerOptions = writerOptions;
            return this;
        }

        public Builder emulatorEndpoint(@Nullable String emulatorEndpoint) {
            this.emulatorEndpoint = emulatorEndpoint;
            return this;
        }

        public Builder serviceAccountKeyFile(@Nullable String serviceAccountKeyFile) {
            this.serviceAccountKeyFile = serviceAccountKeyFile;
            return this;
        }

        public Builder parallelism(@Nullable Integer parallelism) {
            this.parallelism = parallelism;
            return this;
        }

        public SpannerDynamicSink build() {
            return new SpannerDynamicSink(this);
        }
    }
}
