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

package io.github.flink.gcp.connector.bigquery.sink.storage.writer;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.storage.v1.TableFieldSchema;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import io.github.flink.gcp.connector.bigquery.sink.BigQuerySink;
import io.github.flink.gcp.connector.bigquery.sink.BigQuerySinkConfig;
import io.github.flink.gcp.connector.bigquery.sink.CreateDisposition;
import io.github.flink.gcp.connector.bigquery.sink.RetrySchedule;
import io.github.flink.gcp.connector.bigquery.sink.SchemaUpdateOptions;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.serializer.BigQueryProtoSerializer;
import io.github.flink.gcp.connector.bigquery.sink.storage.BigQueryDefaultStreamSink;
import io.github.flink.gcp.connector.bigquery.sink.tables.BigQueryTableAdmin;
import io.github.flink.gcp.connector.bigquery.sink.tables.StorageSchemaConverter;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for schema evolution against the BigQuery emulator (goccy/bigquery-emulator): a
 * nullable column added mid-stream — externally and connector-driven — with writes continuing
 * without a job restart, end-to-end through the Storage Write API gRPC endpoint and the REST
 * table-administration path.
 *
 * <p>Server-side behaviors the emulator does not implement — pushed {@code updated_schema} on
 * append responses, {@code SCHEMA_MISMATCH_EXTRA_FIELDS} rejections and etag-conditioned update
 * races — are covered by {@link BigQueryDefaultStreamWriterSchemaEvolutionTest} against fakes. The
 * emulator also applies {@code tables.update} only to the table <em>metadata</em>, not to its query
 * engine ({@code metadata.Table.Replace} merely swaps the stored resource), so the evolved column's
 * values cannot be queried back; the assertions verify the metadata schema and that rows keep
 * landing instead.
 */
class BigQuerySchemaEvolutionITCase extends AbstractBigQueryEmulatorITCase {
    private static TableFieldSchema nullableString(String name) {
        return TableFieldSchema.newBuilder()
                .setName(name)
                .setType(TableFieldSchema.Type.STRING)
                .setMode(TableFieldSchema.Mode.NULLABLE)
                .build();
    }

    private static final TableSchema V1 =
            TableSchema.newBuilder().addFields(nullableString("name")).build();
    private static final TableSchema V2 =
            TableSchema.newBuilder()
                    .addFields(nullableString("name"))
                    .addFields(nullableString("note"))
                    .build();

    /**
     * Serializer over a mutable schema, writing rows via {@link DynamicMessage}. Records are {@code
     * "name"} or {@code "name:note"}; the note is written only while the schema has the column.
     */
    private static final class EvolvingSerializer extends BigQueryProtoSerializer<String> {
        private static final long serialVersionUID = 1L;

        private TableSchema schema;
        private int version;
        private transient Descriptors.Descriptor descriptor;

        EvolvingSerializer(TableSchema schema) {
            this.schema = schema;
        }

        void evolveTo(TableSchema newSchema) {
            this.schema = newSchema;
            this.version++;
            this.descriptor = null;
        }

        @Override
        public TableSchema getTableSchema(TableDestination destination) {
            return schema;
        }

        @Override
        public Descriptors.Descriptor getDescriptor(TableDestination destination) {
            if (descriptor == null) {
                descriptor = super.getDescriptor(destination);
            }
            return descriptor;
        }

        @Override
        public Object getSchemaFingerprint(TableDestination destination) {
            return version;
        }

        @Override
        public ByteString serialize(String element) {
            String[] parts = element.split(":", 2);
            Descriptors.Descriptor d = getDescriptor(null);
            DynamicMessage.Builder message =
                    DynamicMessage.newBuilder(d).setField(d.findFieldByName("name"), parts[0]);
            Descriptors.FieldDescriptor note = d.findFieldByName("note");
            if (note != null && parts.length > 1) {
                message.setField(note, parts[1]);
            }
            return message.build().toByteString();
        }
    }

    private static BigQueryDefaultStreamWriter<String> writer(
            TableDestination destination,
            EvolvingSerializer serializer,
            SchemaUpdateOptions schemaUpdateOptions) {
        BigQuerySinkConfig<String> config =
                ((BigQueryDefaultStreamSink<String>)
                                BigQuerySink.<String>builder()
                                        .destination(destination)
                                        .serializer(serializer)
                                        .createDisposition(CreateDisposition.CREATE_NEVER)
                                        .schemaUpdateOptions(schemaUpdateOptions)
                                        .build())
                        .getConfig();
        return new BigQueryDefaultStreamWriter<>(
                config,
                new EmulatorAppenderFactory(grpcEndpoint()),
                new BigQueryTableAdmin(restClient),
                BigQueryDefaultStreamWriter.DEFAULT_MAX_APPEND_REQUEST_BYTES,
                new RetrySchedule(100, 1_000, 30, 0),
                new RetrySchedule(100, 1_000, 30, 0));
    }

    private static List<String> tableFieldNames(String table) {
        Schema schema =
                restClient
                        .getTable(TableId.of(DATASET, table))
                        .<StandardTableDefinition>getDefinition()
                        .getSchema();
        List<String> fieldNames = new ArrayList<>();
        for (Field field : schema.getFields()) {
            fieldNames.add(field.getName());
        }
        return fieldNames;
    }

    /**
     * The acceptance scenario of issue #12: a nullable column is added mid-stream (externally, via
     * the REST API — the DDL path) and the serializer starts populating it; writes continue on the
     * same writer without a job restart.
     */
    @Test
    void addingANullableColumnMidStreamKeepsWritesGoing() throws Exception {
        String table = "evolving";
        createTable(table, V1);
        TableDestination destination = TableDestination.of(PROJECT, DATASET, table);
        EvolvingSerializer serializer = new EvolvingSerializer(V1);
        BigQueryDefaultStreamWriter<String> writer =
                writer(destination, serializer, SchemaUpdateOptions.defaults());
        try {
            writer.write("alice", CONTEXT);
            writer.flush(false);

            // The externally driven ALTER: add the nullable column through the REST API.
            Table existing = restClient.getTable(TableId.of(DATASET, table));
            restClient.update(
                    existing.toBuilder()
                            .setDefinition(
                                    StandardTableDefinition.newBuilder()
                                            .setSchema(StorageSchemaConverter.toBigQuerySchema(V2))
                                            .build())
                            .build());
            serializer.evolveTo(V2);

            writer.write("bob:hello", CONTEXT);
            writer.flush(false);
        } finally {
            writer.close();
        }

        // Rows from before and after the schema change landed, on the same writer.
        assertThat(queryNames(table)).containsExactly("alice", "bob");
        assertThat(tableFieldNames(table)).containsExactly("name", "note");
    }

    /**
     * The connector-driven variant: schema updates are enabled, the serializer evolves, and the
     * sink itself widens the destination table before continuing to write.
     */
    @Test
    void serializerEvolutionUpdatesTheTableSchemaWhenEnabled() throws Exception {
        String table = "self_evolving";
        createTable(table, V1);
        TableDestination destination = TableDestination.of(PROJECT, DATASET, table);
        EvolvingSerializer serializer = new EvolvingSerializer(V1);
        BigQueryDefaultStreamWriter<String> writer =
                writer(
                        destination,
                        serializer,
                        SchemaUpdateOptions.builder().allowNewFields().build());
        try {
            writer.write("alice", CONTEXT);
            writer.flush(false);

            serializer.evolveTo(V2);

            writer.write("bob:hello", CONTEXT);
            writer.flush(false);
        } finally {
            writer.close();
        }

        // The sink widened the table schema itself, and rows kept landing.
        assertThat(tableFieldNames(table)).containsExactly("name", "note");
        assertThat(
                        restClient
                                .getTable(TableId.of(DATASET, table))
                                .<StandardTableDefinition>getDefinition()
                                .getSchema()
                                .getFields()
                                .get("note")
                                .getType()
                                .getStandardType())
                .isEqualTo(StandardSQLTypeName.STRING);
        assertThat(queryNames(table)).containsExactly("alice", "bob");
    }
}
