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

package io.github.flink.gcp.connector.docs;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import io.github.flink.gcp.connector.bigquery.sink.BigQuerySink;
import io.github.flink.gcp.connector.bigquery.sink.CdcTableOptions;
import io.github.flink.gcp.connector.bigquery.sink.CdcTableReconciliationPolicy;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.WriteMethod;
import io.github.flink.gcp.connector.bigquery.sink.cdc.CdcChangeType;
import io.github.flink.gcp.connector.bigquery.sink.cdc.CdcOptions;
import io.github.flink.gcp.connector.bigquery.sink.cdc.DebeziumPostgreSqlCdcSequenceNumberProvider;
import io.github.flink.gcp.connector.bigquery.sink.serializer.BigQueryProtoSerializationSchema;
import io.github.flink.gcp.connector.bigquery.sink.serializer.avro.AvroRecordSerializationSchema;
import io.github.flink.gcp.connector.bigquery.sink.serializer.avro.AvroSchemaOptions;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

final class BigQueryExamplesDebeziumPostgreSqlCdc {

    private BigQueryExamplesDebeziumPostgreSqlCdc() {}

    // tag::bigquery-debezium-postgresql-cdc-kafka-source[]
    static KafkaSource<GenericRecord> kafkaSource(org.apache.avro.Schema debeziumEnvelopeSchema) {
        return KafkaSource.<GenericRecord>builder()
                .setBootstrapServers("kafka:9092")
                .setTopics("dbserver1.public.orders")
                .setGroupId("bigquery-cdc-orders")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(
                        ConfluentRegistryAvroDeserializationSchema.forGeneric(
                                debeziumEnvelopeSchema, "http://schema-registry:8081"))
                .build();
    }

    // end::bigquery-debezium-postgresql-cdc-kafka-source[]

    static void runDataStream(
            StreamExecutionEnvironment env,
            KafkaSource<GenericRecord> kafkaSource,
            org.apache.avro.Schema rowSchema)
            throws Exception {
        // tag::bigquery-debezium-postgresql-cdc-datastream[]
        env.enableCheckpointing(60_000);
        env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Debezium PostgreSQL orders")
                .sinkTo(
                        BigQuerySink.<GenericRecord>builder()
                                .writeMethod(WriteMethod.STORAGE_API_AT_LEAST_ONCE)
                                .table(
                                        TableDestination.of(
                                                "my-project", "analytics", "current_orders"))
                                .serializer(
                                        new DebeziumEnvelopeSerializer(
                                                AvroRecordSerializationSchema.of(
                                                        rowSchema,
                                                        AvroSchemaOptions.builder()
                                                                .deriveRequiredColumns()
                                                                .build())))
                                .cdcTableOptions(
                                        CdcTableOptions.builder()
                                                .primaryKeyColumns(Collections.singletonList("id"))
                                                .build())
                                .cdcTableReconciliationPolicy(
                                        CdcTableReconciliationPolicy.RECONCILE)
                                .cdcOptions(
                                        CdcOptions.<GenericRecord>builder(
                                                        envelope ->
                                                                isDelete(envelope)
                                                                        ? CdcChangeType.DELETE
                                                                        : CdcChangeType.UPSERT)
                                                .sequenceNumberProvider(
                                                        envelope ->
                                                                POSTGRESQL_SEQUENCE_NUMBERS
                                                                        .getSequenceNumber(
                                                                                sourceProperties(
                                                                                        source(
                                                                                                envelope))))
                                                .build())
                                .build());

        env.execute("debezium-postgresql-to-bigquery-cdc");
        // end::bigquery-debezium-postgresql-cdc-datastream[]
    }

    static void registerSqlSource(
            StreamExecutionEnvironment env,
            StreamTableEnvironment tableEnv,
            KafkaSource<GenericRecord> kafkaSource) {
        // tag::bigquery-debezium-postgresql-cdc-sql-bridge[]
        env.enableCheckpointing(60_000);
        DataStream<Row> changes =
                env.fromSource(
                                kafkaSource,
                                WatermarkStrategy.noWatermarks(),
                                "Debezium PostgreSQL orders")
                        .map(
                                envelope -> {
                                    GenericRecord value = currentRow(envelope);
                                    return Row.ofKind(
                                            rowKind(envelope),
                                            value.get("id").toString(),
                                            value.get("amount"),
                                            sourceProperties(source(envelope)));
                                })
                        .returns(
                                Types.ROW_NAMED(
                                        new String[] {"id", "amount", "source_properties"},
                                        Types.STRING,
                                        Types.LONG,
                                        Types.MAP(Types.STRING, Types.STRING)));

        tableEnv.createTemporaryView(
                "source_changes",
                tableEnv.fromChangelogStream(
                        changes,
                        Schema.newBuilder()
                                .column("id", DataTypes.STRING().notNull())
                                .column("amount", DataTypes.BIGINT())
                                .column(
                                        "source_properties",
                                        DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING()))
                                .primaryKey("id")
                                .build(),
                        ChangelogMode.upsert()));
        // end::bigquery-debezium-postgresql-cdc-sql-bridge[]
    }

    // tag::bigquery-debezium-postgresql-cdc-adapter[]
    private static final DebeziumPostgreSqlCdcSequenceNumberProvider POSTGRESQL_SEQUENCE_NUMBERS =
            new DebeziumPostgreSqlCdcSequenceNumberProvider();

    private static GenericRecord source(GenericRecord envelope) {
        Object value = envelope.get("source");
        if (!(value instanceof GenericRecord)) {
            throw new IllegalArgumentException("Debezium change has no source record");
        }
        return (GenericRecord) value;
    }

    private static GenericRecord currentRow(GenericRecord envelope) {
        String operation = stringField(envelope, "op");
        String field;
        if ("d".equals(operation)) {
            field = "before";
        } else if ("c".equals(operation) || "r".equals(operation) || "u".equals(operation)) {
            field = "after";
        } else {
            throw new IllegalArgumentException(
                    "Unsupported Debezium operation '" + operation + "'");
        }
        Object row = envelope.get(field);
        if (!(row instanceof GenericRecord)) {
            throw new IllegalArgumentException("Debezium change has no " + field + " row");
        }
        return (GenericRecord) row;
    }

    private static boolean isDelete(GenericRecord envelope) {
        return "d".equals(stringField(envelope, "op"));
    }

    private static RowKind rowKind(GenericRecord envelope) {
        String operation = stringField(envelope, "op");
        if ("d".equals(operation)) {
            return RowKind.DELETE;
        }
        if ("u".equals(operation)) {
            return RowKind.UPDATE_AFTER;
        }
        if ("c".equals(operation) || "r".equals(operation)) {
            return RowKind.INSERT;
        }
        throw new IllegalArgumentException("Unsupported Debezium operation '" + operation + "'");
    }

    private static Map<String, String> sourceProperties(GenericRecord source) {
        Map<String, String> properties = new HashMap<>(3);
        copySourceProperty(source, properties, "connector");
        copySourceProperty(source, properties, "sequence");
        copySourceProperty(source, properties, "lsn");
        return properties;
    }

    private static void copySourceProperty(
            GenericRecord source, Map<String, String> properties, String field) {
        Object value = source.get(field);
        if (value != null) {
            properties.put(field, value.toString());
        }
    }

    private static String stringField(GenericRecord record, String field) {
        Object value = record.get(field);
        if (!(value instanceof CharSequence)) {
            throw new IllegalArgumentException("Debezium record has no " + field);
        }
        return value.toString();
    }

    private static final class DebeziumEnvelopeSerializer
            extends BigQueryProtoSerializationSchema<GenericRecord> {

        private static final long serialVersionUID = 1L;

        private final AvroRecordSerializationSchema delegate;

        private DebeziumEnvelopeSerializer(AvroRecordSerializationSchema delegate) {
            this.delegate = delegate;
        }

        @Override
        public TableSchema getTableSchema(TableDestination destination) {
            return delegate.getTableSchema(destination);
        }

        @Override
        public Descriptors.Descriptor getDescriptor(TableDestination destination) {
            return delegate.getDescriptor(destination);
        }

        @Override
        public Object getSchemaFingerprint(TableDestination destination) {
            return delegate.getSchemaFingerprint(destination);
        }

        @Override
        public ByteString serialize(GenericRecord envelope) throws IOException {
            IndexedRecord row = currentRow(envelope);
            return delegate.serialize(row);
        }
    }
    // end::bigquery-debezium-postgresql-cdc-adapter[]
}
