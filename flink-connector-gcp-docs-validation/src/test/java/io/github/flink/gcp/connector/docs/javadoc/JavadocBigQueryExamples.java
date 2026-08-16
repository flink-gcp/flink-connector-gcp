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
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.GeneratedMessage;
import io.github.flink.gcp.connector.bigquery.sink.BigQuerySink;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.WriteMethod;
import io.github.flink.gcp.connector.bigquery.sink.cdc.CdcChangeType;
import io.github.flink.gcp.connector.bigquery.sink.cdc.CdcOptions;
import io.github.flink.gcp.connector.bigquery.sink.cdc.SpannerCdcSequenceNumber;
import io.github.flink.gcp.connector.bigquery.sink.serializer.BigQueryProtoSerializer;
import io.github.flink.gcp.connector.bigquery.sink.serializer.proto.ProtoSchemaOptions;
import io.github.flink.gcp.connector.bigquery.source.BigQuerySource;
import io.github.flink.gcp.connector.bigquery.source.serializer.BigQueryRowDeserializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import java.io.IOException;
import java.time.Instant;

final class JavadocBigQueryExamples {

    private JavadocBigQueryExamples() {}

    static Sink<MyEvent> sink() {
        // tag::sink[]
        Sink<MyEvent> sink =
                BigQuerySink.<MyEvent>builder()
                        .writeMethod(WriteMethod.STORAGE_API_AT_LEAST_ONCE)
                        .destinationResolver(
                                (e, ctx) ->
                                        TableDestination.of(
                                                "my-project", "my_dataset", e.tableName()))
                        .serializer(new MyEventProtoSerializer())
                        .build();
        // end::sink[]
        return sink;
    }

    static void source(StreamExecutionEnvironment env, Schema schema) {
        // tag::source[]
        Source<GenericRecord, ?, ?> source =
                BigQuerySource.<GenericRecord>builder()
                        .table(TableDestination.of("my-project", "my_dataset", "my_table"))
                        .deserializer(BigQueryRowDeserializer.genericRecord(schema))
                        .rowRestriction("state = 'CA'")
                        .build();

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "BigQuery");
        // end::source[]
    }

    static ProtoSchemaOptions jsonOption() {
        ProtoSchemaOptions options =
                // tag::json-option[]
                ProtoSchemaOptions.builder().jsonFieldOption(MyAnnotations.json).build();
        // end::json-option[]
        return options;
    }

    static ProtoSchemaOptions geographyOption() {
        ProtoSchemaOptions options =
                // tag::geography-option[]
                ProtoSchemaOptions.builder().geographyFieldOption(MyAnnotations.geography).build();
        // end::geography-option[]
        return options;
    }

    static CdcOptions<SpannerChange> spannerCdcSequence() {
        CdcOptions<SpannerChange> options =
                // tag::spanner-cdc-sequence[]
                CdcOptions.<SpannerChange>builder(
                                change ->
                                        change.isDeletion()
                                                ? CdcChangeType.DELETE
                                                : CdcChangeType.UPSERT)
                        .sequenceNumberProvider(
                                change ->
                                        SpannerCdcSequenceNumber.of(
                                                change.commitTimestamp(),
                                                change.recordSequence(),
                                                change.modNumber()))
                        .build();
        // end::spanner-cdc-sequence[]
        return options;
    }

    private static final class MyEvent {

        String tableName() {
            return "orders";
        }
    }

    private static final class SpannerChange {

        boolean isDeletion() {
            return false;
        }

        Instant commitTimestamp() {
            return Instant.EPOCH;
        }

        String recordSequence() {
            return "00000000";
        }

        int modNumber() {
            return 0;
        }
    }

    private static final class MyEventProtoSerializer extends BigQueryProtoSerializer<MyEvent> {

        private static final long serialVersionUID = 1L;

        @Override
        public TableSchema getTableSchema(TableDestination destination) {
            return TableSchema.getDefaultInstance();
        }

        @Override
        public ByteString serialize(MyEvent element) throws IOException {
            return ByteString.EMPTY;
        }
    }

    @SuppressWarnings("checkstyle:ConstantName")
    private static final class MyAnnotations {

        private static final GeneratedMessage.GeneratedExtension<
                        DescriptorProtos.FieldOptions, Boolean>
                json = null;

        private static final GeneratedMessage.GeneratedExtension<
                        DescriptorProtos.FieldOptions, Boolean>
                geography = null;

        private MyAnnotations() {}
    }
}
