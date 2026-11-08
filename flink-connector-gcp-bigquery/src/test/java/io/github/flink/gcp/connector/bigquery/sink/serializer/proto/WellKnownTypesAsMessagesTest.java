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

import com.google.cloud.bigquery.storage.v1.TableFieldSchema;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.protobuf.Duration;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.StringValue;
import io.github.flink.gcp.connector.bigquery.sink.SchemaUpdateOptions;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.writer.ProtoToAvroConverter;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.writer.TableSchemaToAvroConverter;
import io.github.flink.gcp.connector.bigquery.sink.tables.SchemaUnifier;
import io.github.flink.gcp.connector.bigquery.testproto.WellKnownTypesChild;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@link ProtoSchemaOptions.Builder#wellKnownTypesAsMessages} against a table another writer
 * created, through the pieces between the serializer and the service: the schema union the sink
 * runs against the live table, and the FILE_LOADS Avro staging conversion. {@code
 * WellKnownTypesChild} carries a {@code StringValue} and a {@code Duration}, the two shapes of the
 * issue (#2107).
 */
class WellKnownTypesAsMessagesTest {

    private static final TableDestination DESTINATION =
            TableDestination.of("my-project", "my_dataset", "my_table");

    private static final ProtoSchemaOptions AS_MESSAGES =
            ProtoSchemaOptions.builder()
                    .wellKnownTypesAsMessages(
                            ProtoSchemaOptions.WellKnownType.DURATION,
                            ProtoSchemaOptions.WellKnownType.WRAPPERS)
                    .build();

    /**
     * A table with each type as a NULLABLE record of its fields, its sub-fields NULLABLE too. A
     * writer may instead declare the sub-fields REQUIRED; {@link
     * #aTableWithRequiredSubFieldsIsLeftAsItIs} covers that shape.
     */
    private static final TableSchema EXISTING_TABLE =
            TableSchema.newBuilder()
                    .addFields(struct("c_string", column("value", TableFieldSchema.Type.STRING)))
                    .addFields(
                            struct(
                                    "c_duration",
                                    column("seconds", TableFieldSchema.Type.INT64),
                                    column("nanos", TableFieldSchema.Type.INT64)))
                    .build();

    private static TableFieldSchema column(String name, TableFieldSchema.Type type) {
        return TableFieldSchema.newBuilder()
                .setName(name)
                .setType(type)
                .setMode(TableFieldSchema.Mode.NULLABLE)
                .build();
    }

    private static TableFieldSchema struct(String name, TableFieldSchema... fields) {
        return TableFieldSchema.newBuilder()
                .setName(name)
                .setType(TableFieldSchema.Type.STRUCT)
                .setMode(TableFieldSchema.Mode.NULLABLE)
                .addAllFields(Arrays.asList(fields))
                .build();
    }

    private static TableSchema derived(ProtoSchemaOptions options) {
        return ProtoMessageSerializationSchema.of(WellKnownTypesChild.class, options)
                .getTableSchema(DESTINATION);
    }

    @Test
    void derivesExactlyTheExistingTableSoTheUnionChangesNothing() throws Exception {
        assertThat(derived(AS_MESSAGES)).isEqualTo(EXISTING_TABLE);

        SchemaUnifier.UnionResult union =
                SchemaUnifier.union(
                        EXISTING_TABLE,
                        derived(AS_MESSAGES),
                        SchemaUpdateOptions.builder().build());

        assertThat(union.isChanged()).isFalse();
        assertThat(union.getSchema()).isEqualTo(EXISTING_TABLE);
    }

    /**
     * Sub-fields declared REQUIRED by another writer stay REQUIRED: the union never tightens, and
     * relaxing is opt-in. The rows still fit, since every scalar sub-field of a set value is
     * written. The gated {@code BigQueryProtoWellKnownTypesAsMessagesRealGcpITCase} writes to this
     * shape on the service.
     */
    @Test
    void aTableWithRequiredSubFieldsIsLeftAsItIs() throws Exception {
        TableSchema required =
                TableSchema.newBuilder()
                        .addFields(
                                struct(
                                        "c_string",
                                        column("value", TableFieldSchema.Type.STRING).toBuilder()
                                                .setMode(TableFieldSchema.Mode.REQUIRED)
                                                .build()))
                        .addFields(
                                struct(
                                        "c_duration",
                                        column("seconds", TableFieldSchema.Type.INT64).toBuilder()
                                                .setMode(TableFieldSchema.Mode.REQUIRED)
                                                .build(),
                                        column("nanos", TableFieldSchema.Type.INT64).toBuilder()
                                                .setMode(TableFieldSchema.Mode.REQUIRED)
                                                .build()))
                        .build();

        SchemaUnifier.UnionResult union =
                SchemaUnifier.union(
                        required, derived(AS_MESSAGES), SchemaUpdateOptions.builder().build());

        assertThat(union.isChanged()).isFalse();
        assertThat(union.getSchema()).isEqualTo(required);
    }

    /** The failure the option exists to avoid, pinned so the test above is shown to matter. */
    @Test
    void withoutTheOptionTheUnionRefusesTheTypeChange() {
        assertThatThrownBy(
                        () ->
                                SchemaUnifier.union(
                                        EXISTING_TABLE,
                                        derived(ProtoSchemaOptions.defaults()),
                                        SchemaUpdateOptions.builder().build()))
                .isInstanceOf(SchemaUnifier.SchemaUnionException.class)
                .hasMessageContaining("c_string from STRUCT to STRING");
    }

    /**
     * FILE_LOADS stages the serializer's row through the Avro schema of the derived table schema,
     * so the expanded types arrive as nested records: NULL where unset, components where set.
     */
    @Test
    void stagesExpandedValuesAsNestedAvroRecords() throws Exception {
        ProtoMessageSerializationSchema<WellKnownTypesChild> serializer =
                ProtoMessageSerializationSchema.of(WellKnownTypesChild.class, AS_MESSAGES);
        TableSchema schema = serializer.getTableSchema(DESTINATION);
        ProtoToAvroConverter converter =
                new ProtoToAvroConverter(
                        schema,
                        serializer.getDescriptor(DESTINATION),
                        TableSchemaToAvroConverter.convert(schema));

        GenericRecord set =
                converter.convert(
                        DynamicMessage.parseFrom(
                                serializer.getDescriptor(DESTINATION),
                                serializer.serialize(
                                        WellKnownTypesChild.newBuilder()
                                                .setCString(StringValue.of(""))
                                                .setCDuration(
                                                        Duration.newBuilder()
                                                                .setSeconds(90)
                                                                .setNanos(5))
                                                .build())));
        GenericRecord string = (GenericRecord) set.get("c_string");
        assertThat(string.get("value")).hasToString("");
        GenericRecord duration = (GenericRecord) set.get("c_duration");
        assertThat(duration.get("seconds")).isEqualTo(90L);
        assertThat(duration.get("nanos")).isEqualTo(5L);

        GenericRecord unset =
                converter.convert(
                        DynamicMessage.parseFrom(
                                serializer.getDescriptor(DESTINATION),
                                serializer.serialize(WellKnownTypesChild.getDefaultInstance())));
        assertThat(unset.get("c_string")).isNull();
        assertThat(unset.get("c_duration")).isNull();
    }
}
