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

/** Tests for {@link AvroRecordSerializationSchema}. */
class AvroRecordSerializationSchemaTest {

    private static final TableDestination DESTINATION =
            TableDestination.of("project", "dataset", "table");

    private static final String SCHEMA_JSON =
            "{\"type\":\"record\",\"name\":\"Event\",\"namespace\":\"it\",\"fields\":["
                    + "{\"name\":\"name\",\"type\":\"string\"},"
                    + "{\"name\":\"count\",\"type\":[\"null\",\"long\"]},"
                    // A third field so a test can read a *tightened* mode off something that is
                    // neither a union nor a JSON column, both of which stay NULLABLE.
                    + "{\"name\":\"note\",\"type\":\"string\"}]}";

    private static Schema schema() {
        return new Schema.Parser().parse(SCHEMA_JSON);
    }

    private static GenericRecord record(String name, Long count) {
        GenericRecord record = new GenericData.Record(schema());
        record.put("name", name);
        record.put("count", count);
        record.put("note", "n");
        return record;
    }

    @Test
    void derivesTheTableSchemaFromTheAvroSchema() {
        AvroRecordSerializationSchema serializer = AvroRecordSerializationSchema.of(schema());

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
                                TableFieldSchema.Mode.NULLABLE),
                        org.assertj.core.groups.Tuple.tuple(
                                "note",
                                TableFieldSchema.Type.STRING,
                                TableFieldSchema.Mode.NULLABLE));
    }

    @Test
    void schemaObjectAndSchemaTextFactoriesAgree() {
        assertThat(AvroRecordSerializationSchema.of(SCHEMA_JSON).getTableSchema(DESTINATION))
                .isEqualTo(AvroRecordSerializationSchema.of(schema()).getTableSchema(DESTINATION));
    }

    @Test
    void optionsReachSchemaDerivation() {
        // Asserted through the opt-in, not the default: NULLABLE is now what a *lost* options
        // object would produce too, so only the tightening direction distinguishes the two.
        AvroRecordSerializationSchema serializer =
                AvroRecordSerializationSchema.of(
                        schema(), AvroSchemaOptions.builder().deriveRequiredColumns().build());

        assertThat(serializer.getTableSchema(DESTINATION).getFields(0).getMode())
                .isEqualTo(TableFieldSchema.Mode.REQUIRED);
    }

    @Test
    void cachesTheDerivedDescriptor() {
        AvroRecordSerializationSchema serializer = AvroRecordSerializationSchema.of(schema());

        assertThat(serializer.getDescriptor(DESTINATION))
                .isSameAs(serializer.getDescriptor(DESTINATION));
    }

    @Test
    void schemaIsStaticSoThereIsNoFingerprint() {
        assertThat(AvroRecordSerializationSchema.of(schema()).getSchemaFingerprint(DESTINATION))
                .isNull();
    }

    @Test
    void serializesRowsMatchingTheDerivedDescriptor() throws Exception {
        AvroRecordSerializationSchema serializer = AvroRecordSerializationSchema.of(schema());

        DynamicMessage row =
                DynamicMessage.parseFrom(
                        serializer.getDescriptor(DESTINATION),
                        serializer.serialize(record("a", 3L)));

        assertThat(row.getField(row.getDescriptorForType().findFieldByName("name"))).isEqualTo("a");
        assertThat(row.getField(row.getDescriptorForType().findFieldByName("count"))).isEqualTo(3L);
    }

    @Test
    void survivesJobGraphSerializationCarryingItsOptions() throws Exception {
        AvroRecordSerializationSchema original =
                AvroRecordSerializationSchema.of(
                        schema(),
                        AvroSchemaOptions.builder()
                                .jsonFieldPath("name")
                                .deriveRequiredColumns()
                                .build());
        // Use it first, so the transient conversion state exists and has to be rebuilt.
        original.serialize(record("a", 1L));

        AvroRecordSerializationSchema copy = InstantiationUtil.clone(original);

        assertThat(copy.getTableSchema(DESTINATION))
                .isEqualTo(original.getTableSchema(DESTINATION));
        // The JSON path survived (field 0 is JSON rather than STRING) and so did the mode option
        // (field 2 is tightened). The mode has to be read off a *different* field than the JSON
        // one:
        // a JSON column is never REQUIRED, and NULLABLE is what a copy that lost its options would
        // derive anyway, so field 0's mode could not discriminate.
        assertThat(copy.getTableSchema(DESTINATION).getFields(0).getType())
                .isEqualTo(TableFieldSchema.Type.JSON);
        assertThat(copy.getTableSchema(DESTINATION).getFields(0).getMode())
                .isEqualTo(TableFieldSchema.Mode.NULLABLE);
        assertThat(copy.getTableSchema(DESTINATION).getFields(2).getMode())
                .isEqualTo(TableFieldSchema.Mode.REQUIRED);
        assertThat(copy.serialize(record("b", 2L))).isEqualTo(original.serialize(record("b", 2L)));
    }

    /**
     * The geography marker has to reach both conversion sides, exactly as the JSON one does: the
     * derived schema says {@code GEOGRAPHY} while the value is still written through as a string.
     */
    @Test
    void carriesTheGeographyMarkerThroughToBothConversionSides() throws Exception {
        AvroRecordSerializationSchema serializer =
                AvroRecordSerializationSchema.of(
                        schema(), AvroSchemaOptions.builder().geographyFieldPath("note").build());

        assertThat(serializer.getTableSchema(DESTINATION).getFields(2).getType())
                .isEqualTo(TableFieldSchema.Type.GEOGRAPHY);

        GenericRecord record = record("a", 1L);
        record.put("note", "POINT(1 2)");
        DynamicMessage row =
                DynamicMessage.parseFrom(
                        serializer.getDescriptor(DESTINATION), serializer.serialize(record));

        assertThat(row.getField(row.getDescriptorForType().findFieldByName("note")))
                .isEqualTo("POINT(1 2)");
    }

    @Test
    void schemaMappingProblemsFailWhenTheSerializerIsCreated() {
        // Not on the first record: serialize() runs inside the writers'
        // FailureHandler<BigQueryFailure>
        // catch, so
        // a lazily derived schema would make one misconfiguration look like a poison record —
        // and a log-and-drop policy would swallow the whole stream.
        assertThatThrownBy(
                        () ->
                                AvroRecordSerializationSchema.of(
                                        "{\"type\":\"record\",\"name\":\"Bad\",\"fields\":"
                                                + "[{\"name\":\"f\",\"type\":"
                                                + "[\"null\",\"string\",\"long\"]}]}"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("more than one non-null branch");
    }

    @Test
    void malformedSchemaTextFailsWhereItIsSupplied() {
        assertThatThrownBy(() -> AvroRecordSerializationSchema.of("{not a schema"))
                .isInstanceOf(org.apache.avro.SchemaParseException.class);
    }

    @Test
    void rejectsNullArguments() {
        assertThatThrownBy(() -> AvroRecordSerializationSchema.of((Schema) null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> AvroRecordSerializationSchema.of((String) null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> AvroRecordSerializationSchema.of(schema(), null))
                .isInstanceOf(NullPointerException.class);
    }
}
