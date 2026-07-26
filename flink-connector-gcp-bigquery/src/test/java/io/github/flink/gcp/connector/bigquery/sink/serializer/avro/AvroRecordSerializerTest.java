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

package io.github.flink.gcp.connector.bigquery.sink.serializer.avro;

import org.apache.flink.util.InstantiationUtil;

import com.google.cloud.bigquery.storage.v1.TableFieldSchema;
import com.google.protobuf.DynamicMessage;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link AvroRecordSerializer}. */
class AvroRecordSerializerTest {

    private static final TableDestination DESTINATION =
            TableDestination.of("project", "dataset", "table");

    private static final String SCHEMA_JSON =
            "{\"type\":\"record\",\"name\":\"Event\",\"namespace\":\"it\",\"fields\":["
                    + "{\"name\":\"name\",\"type\":\"string\"},"
                    + "{\"name\":\"count\",\"type\":[\"null\",\"long\"]}]}";

    private static Schema schema() {
        return new Schema.Parser().parse(SCHEMA_JSON);
    }

    private static GenericRecord record(String name, Long count) {
        GenericRecord record = new GenericData.Record(schema());
        record.put("name", name);
        record.put("count", count);
        return record;
    }

    @Test
    void derivesTheTableSchemaFromTheAvroSchema() {
        AvroRecordSerializer serializer = AvroRecordSerializer.of(schema());

        assertThat(serializer.getTableSchema(DESTINATION).getFieldsList())
                .extracting(
                        TableFieldSchema::getName,
                        TableFieldSchema::getType,
                        TableFieldSchema::getMode)
                .containsExactly(
                        org.assertj.core.groups.Tuple.tuple(
                                "name",
                                TableFieldSchema.Type.STRING,
                                TableFieldSchema.Mode.NULLABLE),
                        org.assertj.core.groups.Tuple.tuple(
                                "count",
                                TableFieldSchema.Type.INT64,
                                TableFieldSchema.Mode.NULLABLE));
    }

    @Test
    void schemaObjectAndSchemaTextFactoriesAgree() {
        assertThat(AvroRecordSerializer.of(SCHEMA_JSON).getTableSchema(DESTINATION))
                .isEqualTo(AvroRecordSerializer.of(schema()).getTableSchema(DESTINATION));
    }

    @Test
    void optionsReachSchemaDerivation() {
        // Asserted through the opt-in, not the default: NULLABLE is now what a *lost* options
        // object would produce too, so only the tightening direction distinguishes the two.
        AvroRecordSerializer serializer =
                AvroRecordSerializer.of(
                        schema(), AvroSchemaOptions.builder().deriveRequiredColumns().build());

        assertThat(serializer.getTableSchema(DESTINATION).getFields(0).getMode())
                .isEqualTo(TableFieldSchema.Mode.REQUIRED);
    }

    @Test
    void cachesTheDerivedDescriptor() {
        AvroRecordSerializer serializer = AvroRecordSerializer.of(schema());

        assertThat(serializer.getDescriptor(DESTINATION))
                .isSameAs(serializer.getDescriptor(DESTINATION));
    }

    @Test
    void schemaIsStaticSoThereIsNoFingerprint() {
        assertThat(AvroRecordSerializer.of(schema()).getSchemaFingerprint(DESTINATION)).isNull();
    }

    @Test
    void serializesRowsMatchingTheDerivedDescriptor() throws Exception {
        AvroRecordSerializer serializer = AvroRecordSerializer.of(schema());

        DynamicMessage row =
                DynamicMessage.parseFrom(
                        serializer.getDescriptor(DESTINATION),
                        serializer.serialize(record("a", 3L)));

        assertThat(row.getField(row.getDescriptorForType().findFieldByName("name"))).isEqualTo("a");
        assertThat(row.getField(row.getDescriptorForType().findFieldByName("count"))).isEqualTo(3L);
    }

    @Test
    void survivesJobGraphSerializationCarryingItsOptions() throws Exception {
        AvroRecordSerializer original =
                AvroRecordSerializer.of(
                        schema(),
                        AvroSchemaOptions.builder()
                                .jsonFieldPath("name")
                                .deriveRequiredColumns()
                                .build());
        // Use it first, so the transient conversion state exists and has to be rebuilt.
        original.serialize(record("a", 1L));

        AvroRecordSerializer copy = InstantiationUtil.clone(original);

        assertThat(copy.getTableSchema(DESTINATION))
                .isEqualTo(original.getTableSchema(DESTINATION));
        assertThat(copy.getTableSchema(DESTINATION).getFields(0).getType())
                .isEqualTo(TableFieldSchema.Type.JSON);
        // REQUIRED, not NULLABLE: a copy that lost its options would derive NULLABLE, so only the
        // tightened mode proves the options survived the round trip.
        assertThat(copy.getTableSchema(DESTINATION).getFields(0).getMode())
                .isEqualTo(TableFieldSchema.Mode.REQUIRED);
        assertThat(copy.serialize(record("b", 2L))).isEqualTo(original.serialize(record("b", 2L)));
    }

    @Test
    void schemaMappingProblemsFailWhenTheSerializerIsCreated() {
        // Not on the first record: serialize() runs inside the writers' FailedRowHandler catch, so
        // a lazily derived schema would make one misconfiguration look like a poison record —
        // and a log-and-drop policy would swallow the whole stream.
        assertThatThrownBy(
                        () ->
                                AvroRecordSerializer.of(
                                        "{\"type\":\"record\",\"name\":\"Bad\",\"fields\":"
                                                + "[{\"name\":\"f\",\"type\":"
                                                + "[\"null\",\"string\",\"long\"]}]}"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("more than one non-null branch");
    }

    @Test
    void malformedSchemaTextFailsWhereItIsSupplied() {
        assertThatThrownBy(() -> AvroRecordSerializer.of("{not a schema"))
                .isInstanceOf(org.apache.avro.SchemaParseException.class);
    }

    @Test
    void rejectsNullArguments() {
        assertThatThrownBy(() -> AvroRecordSerializer.of((Schema) null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> AvroRecordSerializer.of((String) null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> AvroRecordSerializer.of(schema(), null))
                .isInstanceOf(NullPointerException.class);
    }
}
