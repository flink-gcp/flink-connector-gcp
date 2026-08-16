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

package io.github.flink.gcp.connector.bigquery.sink.serializer;

import com.google.cloud.bigquery.storage.v1.TableFieldSchema;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Timestamp;
import io.github.flink.gcp.connector.bigquery.sink.BigQuerySink;
import io.github.flink.gcp.connector.bigquery.sink.BigQuerySinkConfig;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.serializer.avro.AvroRecordSerializer;
import io.github.flink.gcp.connector.bigquery.sink.serializer.json.JsonDocumentSerializer;
import io.github.flink.gcp.connector.bigquery.sink.serializer.proto.ProtoMessageSerializer;
import io.github.flink.gcp.connector.bigquery.sink.storage.BigQueryDefaultStreamSink;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.IndexedRecord;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

class AdditionalFieldsBuiltInSerializersTest {

    private static final TableDestination DESTINATION =
            TableDestination.of("project", "dataset", "table");

    @Test
    void appendsAfterTheProtobufSerializer() throws Exception {
        Timestamp record = Timestamp.newBuilder().setSeconds(123L).build();

        assertAdditionalField(
                ProtoMessageSerializer.of(Timestamp.class),
                record,
                value -> Long.toString(value.getSeconds()),
                "123");
    }

    @Test
    void appendsAfterTheAvroSerializer() throws Exception {
        Schema schema =
                new Schema.Parser()
                        .parse(
                                "{\"type\":\"record\",\"name\":\"Event\",\"fields\":["
                                        + "{\"name\":\"name\",\"type\":\"string\"}]}");
        IndexedRecord record = new GenericData.Record(schema);
        record.put(0, "alice");

        assertAdditionalField(
                AvroRecordSerializer.of(schema), record, value -> value.get(0).toString(), "alice");
    }

    @Test
    void appendsAfterTheJsonSerializer() throws Exception {
        String record = "{\"name\":\"alice\"}";
        com.google.cloud.bigquery.storage.v1.TableSchema schema =
                com.google.cloud.bigquery.storage.v1.TableSchema.newBuilder()
                        .addFields(
                                TableFieldSchema.newBuilder()
                                        .setName("name")
                                        .setType(TableFieldSchema.Type.STRING)
                                        .setMode(TableFieldSchema.Mode.REQUIRED))
                        .build();

        assertAdditionalField(JsonDocumentSerializer.of(schema), record, value -> value, record);
    }

    private static <T> void assertAdditionalField(
            BigQueryProtoSerializer<? super T> serializer,
            T record,
            AdditionalFieldValueProvider<? super T> provider,
            String expectedValue)
            throws IOException {
        @SuppressWarnings("unchecked")
        BigQueryDefaultStreamSink<T> sink =
                (BigQueryDefaultStreamSink<T>)
                        BigQuerySink.<T>builder()
                                .destination(DESTINATION)
                                .serializer(serializer)
                                .additionalFields(
                                        AdditionalFields.<T>builder()
                                                .field(
                                                        AdditionalField.of(
                                                                "__source",
                                                                AdditionalFieldType.STRING,
                                                                AdditionalFieldNullPolicy.REQUIRED,
                                                                provider))
                                                .build())
                                .build();
        BigQuerySinkConfig<T> config = sink.getConfig();
        Descriptors.Descriptor descriptor = config.getWriteDescriptor(DESTINATION);
        DynamicMessage row =
                DynamicMessage.parseFrom(descriptor, config.serialize(record, DESTINATION));

        assertThat(config.getTableSchema(DESTINATION).getFieldsList())
                .extracting(TableFieldSchema::getName)
                .endsWith("__source");
        assertThat(row.getField(descriptor.findFieldByName("__source"))).isEqualTo(expectedValue);
    }
}
