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

package io.github.flink.gcp.connector.bigquery.sink.serializer.proto;

import org.apache.flink.util.InstantiationUtil;

import com.google.cloud.bigquery.storage.v1.BigQuerySchemaUtil;
import com.google.cloud.bigquery.storage.v1.TableFieldSchema;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.StringValue;
import com.google.protobuf.Timestamp;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.testproto.MixedCase;
import io.github.flink.gcp.connector.bigquery.testproto.MixedCaseChild;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link ProtoMessageSerializationSchema}, using {@link Timestamp} as an arbitrary
 * generated message class (as the root record type its fields map to plain INT64 columns).
 */
class ProtoMessageSerializationSchemaTest {

    private static final TableDestination DESTINATION =
            TableDestination.of("my-project", "my_dataset", "my_table");

    @Test
    void exposesTheLosslessTableSchema() {
        ProtoMessageSerializationSchema<Timestamp> serializer =
                ProtoMessageSerializationSchema.of(Timestamp.class);

        com.google.cloud.bigquery.storage.v1.TableSchema schema =
                serializer.getTableSchema(DESTINATION);

        assertThat(schema.getFieldsList())
                .extracting(com.google.cloud.bigquery.storage.v1.TableFieldSchema::getName)
                .containsExactlyInAnyOrder("seconds", "nanos");
    }

    @Test
    void derivesRowDescriptorFromMessageClass() {
        ProtoMessageSerializationSchema<Timestamp> serializer =
                ProtoMessageSerializationSchema.of(Timestamp.class);

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
    void cachesTheDerivedDescriptor() {
        ProtoMessageSerializationSchema<Timestamp> serializer =
                ProtoMessageSerializationSchema.of(Timestamp.class);

        assertThat(serializer.getDescriptor(DESTINATION))
                .isSameAs(serializer.getDescriptor(DESTINATION));
    }

    @Test
    void serializedRowsParseAgainstTheRowDescriptor() throws Exception {
        ProtoMessageSerializationSchema<Timestamp> serializer =
                ProtoMessageSerializationSchema.of(Timestamp.class);
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
        ProtoMessageSerializationSchema<StringValue> serializer =
                ProtoMessageSerializationSchema.of(
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
        ProtoMessageSerializationSchema<StringValue> serializer =
                ProtoMessageSerializationSchema.of(
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

    /**
     * The columns keep the field names as written, while the client library lower-cases the row
     * descriptor's field names; the serializer used to pair the two by exact name and failed while
     * the job graph was built (#2179).
     */
    @Test
    void writesFieldNamesWithUpperCaseLettersToTheirColumns() throws Exception {
        ProtoMessageSerializationSchema<MixedCase> serializer =
                ProtoMessageSerializationSchema.of(MixedCase.class);

        TableSchema schema = serializer.getTableSchema(DESTINATION);
        assertThat(schema.getFieldsList())
                .extracting(TableFieldSchema::getName)
                .containsExactly(
                        "id", "hasNext", "beforeUpdate", "Items", "countsByName", "userId");
        assertThat(schema.getFields(2).getFieldsList())
                .extracting(TableFieldSchema::getName)
                .containsExactly("innerValue", "Count");

        MixedCase record =
                MixedCase.newBuilder()
                        .setId("a")
                        .setHasNext(true)
                        .setBeforeUpdate(
                                MixedCaseChild.newBuilder().setInnerValue("b").setCount(2L))
                        .addItems(MixedCaseChild.newBuilder().setInnerValue("c").setCount(3L))
                        .putCountsByName("k", 4L)
                        .setUserId(5L)
                        .build();
        DynamicMessage row =
                DynamicMessage.parseFrom(
                        serializer.getDescriptor(DESTINATION), serializer.serialize(record));

        assertThat(column(row, "id")).isEqualTo("a");
        assertThat(column(row, "hasNext")).isEqualTo(true);
        DynamicMessage beforeUpdate = (DynamicMessage) column(row, "beforeUpdate");
        assertThat(column(beforeUpdate, "innerValue")).isEqualTo("b");
        assertThat(column(beforeUpdate, "Count")).isEqualTo(2L);
        List<?> items = (List<?>) column(row, "Items");
        assertThat(items).hasSize(1);
        assertThat(column((DynamicMessage) items.get(0), "innerValue")).isEqualTo("c");
        assertThat(column((DynamicMessage) items.get(0), "Count")).isEqualTo(3L);
        List<?> countsByName = (List<?>) column(row, "countsByName");
        assertThat(countsByName).hasSize(1);
        assertThat(column((DynamicMessage) countsByName.get(0), "key")).isEqualTo("k");
        assertThat(column((DynamicMessage) countsByName.get(0), "value")).isEqualTo(4L);
        assertThat(column(row, "userId")).isEqualTo(5L);
    }

    /**
     * Not on the first record. {@code serialize()} runs inside the writers' {@code FailureHandler}
     * catch, so a lazily derived schema makes one misconfiguration look like a poison record — and
     * a log-and-drop or DLQ policy then swallows it once per record for the life of the job,
     * leaving the table empty and the job green. The Avro serializer has always derived eagerly for
     * this reason; this side did not until the geography marker added three more ways to
     * misconfigure it.
     */
    @Test
    void schemaMappingProblemsFailWhenTheSerializerIsCreated() {
        assertThatThrownBy(
                        () ->
                                ProtoMessageSerializationSchema.of(
                                        StringValue.class,
                                        ProtoSchemaOptions.builder()
                                                .geographyFieldPath("no_such_field")
                                                .build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("no_such_field");

        // The JSON marker gets the same treatment, which is the half this closes for free.
        assertThatThrownBy(
                        () ->
                                ProtoMessageSerializationSchema.of(
                                        StringValue.class,
                                        ProtoSchemaOptions.builder()
                                                .jsonFieldPath("no_such_field")
                                                .build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("no_such_field");
    }

    @Test
    void survivesJavaSerialization() throws Exception {
        ProtoMessageSerializationSchema<Timestamp> serializer =
                ProtoMessageSerializationSchema.of(Timestamp.class);
        // Initialize transient state before cloning to prove it is rebuilt, not carried over.
        serializer.getDescriptor(DESTINATION);

        ProtoMessageSerializationSchema<Timestamp> copy = InstantiationUtil.clone(serializer);

        Timestamp record = Timestamp.newBuilder().setSeconds(9L).setNanos(1000).build();
        DynamicMessage row =
                DynamicMessage.parseFrom(copy.getDescriptor(DESTINATION), copy.serialize(record));
        assertThat(row.getField(row.getDescriptorForType().findFieldByName("seconds")))
                .isEqualTo(9L);
    }

    /**
     * Returns the value of the row field that writes the named column, matching the name as
     * BigQuery matches a column: ignoring case.
     */
    private static Object column(DynamicMessage row, String column) {
        for (Descriptors.FieldDescriptor field : row.getDescriptorForType().getFields()) {
            if (BigQuerySchemaUtil.getFieldName(field).equalsIgnoreCase(column)) {
                return row.getField(field);
            }
        }
        throw new AssertionError("The row descriptor has no field for column " + column);
    }
}
