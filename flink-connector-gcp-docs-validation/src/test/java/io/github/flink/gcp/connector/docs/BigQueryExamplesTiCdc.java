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

package io.github.flink.gcp.connector.docs;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

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
import io.github.flink.gcp.connector.bigquery.sink.cdc.TiCdcSequenceNumberProvider;
import io.github.flink.gcp.connector.bigquery.sink.serializer.BigQueryProtoSerializer;
import io.github.flink.gcp.connector.bigquery.sink.serializer.json.JsonDocumentSerializer;
import org.json.JSONObject;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

final class BigQueryExamplesTiCdc {

    private BigQueryExamplesTiCdc() {}

    // tag::bigquery-ticdc-cdc-kafka-source[]
    static KafkaSource<String> kafkaSource() {
        return KafkaSource.<String>builder()
                .setBootstrapServers("kafka:9092")
                .setTopics("tidb_test.test.orders")
                .setGroupId("bigquery-cdc-orders")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
    }

    // end::bigquery-ticdc-cdc-kafka-source[]

    static void runDataStream(
            StreamExecutionEnvironment env, KafkaSource<String> kafkaSource, TableSchema rowSchema)
            throws Exception {
        // tag::bigquery-ticdc-cdc-datastream[]
        env.enableCheckpointing(60_000);
        env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "TiCDC orders")
                .sinkTo(
                        BigQuerySink.<String>builder()
                                .writeMethod(WriteMethod.STORAGE_API_AT_LEAST_ONCE)
                                .destination(
                                        TableDestination.of(
                                                "my-project", "analytics", "current_orders"))
                                .serializer(
                                        new TiCdcEnvelopeSerializer(
                                                JsonDocumentSerializer.of(rowSchema)))
                                .cdcTableOptions(
                                        CdcTableOptions.builder()
                                                .primaryKeyColumns(Collections.singletonList("id"))
                                                .build())
                                .cdcTableReconciliationPolicy(
                                        CdcTableReconciliationPolicy.RECONCILE)
                                .cdcOptions(
                                        CdcOptions.<String>builder(
                                                        message ->
                                                                isDelete(payload(message))
                                                                        ? CdcChangeType.DELETE
                                                                        : CdcChangeType.UPSERT)
                                                .sequenceNumberProvider(
                                                        message ->
                                                                COMMIT_TSO_SEQUENCE_NUMBERS
                                                                        .getSequenceNumber(
                                                                                sourceProperties(
                                                                                        payload(
                                                                                                message))))
                                                .build())
                                .build());

        env.execute("ticdc-to-bigquery-cdc");
        // end::bigquery-ticdc-cdc-datastream[]
    }

    // tag::bigquery-ticdc-cdc-adapter[]
    private static final TiCdcSequenceNumberProvider COMMIT_TSO_SEQUENCE_NUMBERS =
            new TiCdcSequenceNumberProvider("tidb-prod");

    /**
     * Returns the change itself. TiCDC wraps it in a {@code payload} object unless the changefeed
     * sets {@code debezium-disable-schema=true}.
     */
    private static JSONObject payload(String message) {
        JSONObject envelope = new JSONObject(message);
        return envelope.optJSONObject("payload") == null
                ? envelope
                : envelope.getJSONObject("payload");
    }

    private static JSONObject currentRow(JSONObject payload) {
        String field = isDelete(payload) ? "before" : "after";
        JSONObject row = payload.optJSONObject(field);
        if (row == null) {
            throw new IllegalArgumentException("TiCDC change has no " + field + " row");
        }
        return row;
    }

    private static boolean isDelete(JSONObject payload) {
        String operation = payload.optString("op", null);
        if ("c".equals(operation) || "u".equals(operation) || "r".equals(operation)) {
            return false;
        }
        if ("d".equals(operation)) {
            return true;
        }
        // A DDL or watermark event carries no row to write; only TiCDC's new architecture
        // emits them.
        throw new IllegalArgumentException("Unsupported TiCDC operation '" + operation + "'");
    }

    private static Map<String, String> sourceProperties(JSONObject payload) {
        JSONObject source = payload.optJSONObject("source");
        if (source == null) {
            throw new IllegalArgumentException("TiCDC change has no source object");
        }
        Map<String, String> properties = new HashMap<>(4);
        copySourceProperty(source, properties, "connector");
        copySourceProperty(source, properties, "snapshot");
        copySourceProperty(source, properties, "commit_ts");
        copySourceProperty(source, properties, "cluster_id");
        return properties;
    }

    private static void copySourceProperty(
            JSONObject source, Map<String, String> properties, String field) {
        if (!source.isNull(field)) {
            properties.put(field, String.valueOf(source.get(field)));
        }
    }

    private static final class TiCdcEnvelopeSerializer extends BigQueryProtoSerializer<String> {

        private static final long serialVersionUID = 1L;

        private final JsonDocumentSerializer delegate;

        private TiCdcEnvelopeSerializer(JsonDocumentSerializer delegate) {
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
        public ByteString serialize(String message) throws IOException {
            return delegate.serialize(currentRow(payload(message)).toString());
        }
    }
    // end::bigquery-ticdc-cdc-adapter[]
}
