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

package io.github.flink.gcp.connector.bigquery.sink.serializer.proto;

import org.apache.flink.util.InstantiationUtil;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.StringValue;
import com.google.protobuf.Timestamp;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link ProtoMessageSerializer}, using {@link Timestamp} as an arbitrary generated
 * message class (as the root record type its fields map to plain INT64 columns).
 */
class ProtoMessageSerializerTest {

    private static final TableDestination DESTINATION =
            TableDestination.of("my-project", "my_dataset", "my_table");

    @Test
    void exposesTheLosslessTableSchema() {
        ProtoMessageSerializer<Timestamp> serializer = ProtoMessageSerializer.of(Timestamp.class);

        com.google.cloud.bigquery.storage.v1.TableSchema schema =
                serializer.getTableSchema(DESTINATION);

        assertThat(schema.getFieldsList())
                .extracting(com.google.cloud.bigquery.storage.v1.TableFieldSchema::getName)
                .containsExactlyInAnyOrder("seconds", "nanos");
    }

    @Test
    void derivesRowDescriptorFromMessageClass() {
        ProtoMessageSerializer<Timestamp> serializer = ProtoMessageSerializer.of(Timestamp.class);

        Descriptors.Descriptor descriptor = serializer.getDescriptor(DESTINATION);

        assertThat(descriptor.getFields())
                .extracting(Descriptors.FieldDescriptor::getName)
                .containsExactlyInAnyOrder("seconds", "nanos");
        assertThat(descriptor.findFieldByName("seconds").getJavaType())
                .isEqualTo(Descriptors.FieldDescriptor.JavaType.LONG);
        assertThat(descriptor.findFieldByName("nanos").getJavaType())
                .isEqualTo(Descriptors.FieldDescriptor.JavaType.LONG);
    }

    @Test
    void serializedRowsParseAgainstTheRowDescriptor() throws Exception {
        ProtoMessageSerializer<Timestamp> serializer = ProtoMessageSerializer.of(Timestamp.class);
        Timestamp record = Timestamp.newBuilder().setSeconds(123L).setNanos(456).build();

        DynamicMessage row =
                DynamicMessage.parseFrom(
                        serializer.getDescriptor(DESTINATION), serializer.serialize(record));

        Descriptors.Descriptor descriptor = row.getDescriptorForType();
        assertThat(row.getField(descriptor.findFieldByName("seconds"))).isEqualTo(123L);
        assertThat(row.getField(descriptor.findFieldByName("nanos"))).isEqualTo(456L);
    }

    @Test
    void carriesSchemaOptionsThroughToBothConversionSides() throws Exception {
        // StringValue's single string field stands in for a column holding JSON text: the schema
        // must say JSON while the value is still written through as a string.
        ProtoMessageSerializer<StringValue> serializer =
                ProtoMessageSerializer.of(
                        StringValue.class,
                        ProtoSchemaOptions.builder().jsonFieldPath("value").build());

        assertThat(serializer.getTableSchema(DESTINATION).getFieldsList())
                .extracting(
                        com.google.cloud.bigquery.storage.v1.TableFieldSchema::getName,
                        com.google.cloud.bigquery.storage.v1.TableFieldSchema::getType)
                .containsExactly(
                        org.assertj.core.groups.Tuple.tuple(
                                "value",
                                com.google.cloud.bigquery.storage.v1.TableFieldSchema.Type.JSON));

        DynamicMessage row =
                DynamicMessage.parseFrom(
                        serializer.getDescriptor(DESTINATION),
                        serializer.serialize(StringValue.of("{\"a\":1}")));
        assertThat(row.getField(row.getDescriptorForType().findFieldByName("value")))
                .isEqualTo("{\"a\":1}");
    }

    @Test
    void carriesTheGeographyMarkerThroughToBothConversionSides() throws Exception {
        // As above, with StringValue's single string field standing in for a column holding a
        // geometry literal.
        ProtoMessageSerializer<StringValue> serializer =
                ProtoMessageSerializer.of(
                        StringValue.class,
                        ProtoSchemaOptions.builder().geographyFieldPath("value").build());

        assertThat(serializer.getTableSchema(DESTINATION).getFields(0).getType())
                .isEqualTo(com.google.cloud.bigquery.storage.v1.TableFieldSchema.Type.GEOGRAPHY);

        DynamicMessage row =
                DynamicMessage.parseFrom(
                        serializer.getDescriptor(DESTINATION),
                        serializer.serialize(StringValue.of("POINT(1 2)")));
        assertThat(row.getField(row.getDescriptorForType().findFieldByName("value")))
                .isEqualTo("POINT(1 2)");
    }

    @Test
    void survivesJavaSerialization() throws Exception {
        ProtoMessageSerializer<Timestamp> serializer = ProtoMessageSerializer.of(Timestamp.class);
        // Initialize transient state before cloning to prove it is rebuilt, not carried over.
        serializer.getDescriptor(DESTINATION);

        ProtoMessageSerializer<Timestamp> copy = InstantiationUtil.clone(serializer);

        Timestamp record = Timestamp.newBuilder().setSeconds(9L).setNanos(1000).build();
        DynamicMessage row =
                DynamicMessage.parseFrom(copy.getDescriptor(DESTINATION), copy.serialize(record));
        assertThat(row.getField(row.getDescriptorForType().findFieldByName("seconds")))
                .isEqualTo(9L);
    }
}
