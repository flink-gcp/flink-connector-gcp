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

import com.google.cloud.bigquery.storage.v1.TableFieldSchema;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.protobuf.Descriptors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link ProtoToTableSchemaConverter}. */
class ProtoToTableSchemaConverterTest {

    private static final ProtoSchemaOptions DERIVE_REQUIRED =
            ProtoSchemaOptions.builder().deriveRequiredColumns().build();

    @Test
    void mapsTheFullTypeMatrix() {
        TableSchema schema =
                ProtoToTableSchemaConverter.convert(
                        TestProtos.allTypes(), ProtoSchemaOptions.defaults());
        Map<String, TableFieldSchema> fields = byName(schema);

        assertThat(fields.get("f_int32").getType()).isEqualTo(TableFieldSchema.Type.INT64);
        assertThat(fields.get("f_int64").getType()).isEqualTo(TableFieldSchema.Type.INT64);
        assertThat(fields.get("f_uint32").getType()).isEqualTo(TableFieldSchema.Type.INT64);
        assertThat(fields.get("f_uint64").getType()).isEqualTo(TableFieldSchema.Type.INT64);
        assertThat(fields.get("f_float").getType()).isEqualTo(TableFieldSchema.Type.DOUBLE);
        assertThat(fields.get("f_double").getType()).isEqualTo(TableFieldSchema.Type.DOUBLE);
        assertThat(fields.get("f_bool").getType()).isEqualTo(TableFieldSchema.Type.BOOL);
        assertThat(fields.get("f_string").getType()).isEqualTo(TableFieldSchema.Type.STRING);
        assertThat(fields.get("f_bytes").getType()).isEqualTo(TableFieldSchema.Type.BYTES);
        assertThat(fields.get("f_enum").getType()).isEqualTo(TableFieldSchema.Type.STRING);
        assertThat(fields.get("f_ts").getType()).isEqualTo(TableFieldSchema.Type.TIMESTAMP);

        assertThat(fields.values())
                .filteredOn(f -> f.getMode() != TableFieldSchema.Mode.REPEATED)
                .allSatisfy(f -> assertThat(f.getMode()).isEqualTo(TableFieldSchema.Mode.NULLABLE));
        assertThat(fields.get("f_rep_ts").getMode()).isEqualTo(TableFieldSchema.Mode.REPEATED);
        assertThat(fields.get("f_rep_ts").getType()).isEqualTo(TableFieldSchema.Type.TIMESTAMP);
    }

    /**
     * By default the converter looks at no presence shape at all — a field is REPEATED, or it is
     * NULLABLE. That default is deliberate: proto3's presence-less form is the spelling you get by
     * not thinking about nullability, so deriving REQUIRED from it would make nearly every scalar
     * column of an auto-created table REQUIRED on the strength of a syntax default.
     */
    @Test
    void mapsEveryProto3PresenceShapeToNullableByDefault() {
        TableSchema schema =
                ProtoToTableSchemaConverter.convert(
                        TestProtos.presence(), ProtoSchemaOptions.defaults());
        Map<String, TableFieldSchema> fields = byName(schema);

        assertThat(fields.get("p_implicit").getMode()).isEqualTo(TableFieldSchema.Mode.NULLABLE);
        assertThat(fields.get("p_implicit_int").getMode())
                .isEqualTo(TableFieldSchema.Mode.NULLABLE);
        assertThat(fields.get("p_choice_a").getMode()).isEqualTo(TableFieldSchema.Mode.NULLABLE);
        assertThat(fields.get("p_choice_b").getMode()).isEqualTo(TableFieldSchema.Mode.NULLABLE);
        assertThat(fields.get("p_optional").getMode()).isEqualTo(TableFieldSchema.Mode.NULLABLE);
        assertThat(fields.get("p_nested").getMode()).isEqualTo(TableFieldSchema.Mode.NULLABLE);
        assertThat(fields.get("p_rep").getMode()).isEqualTo(TableFieldSchema.Mode.REPEATED);

        // The fixture is worth nothing unless protoc really spelled the shapes it claims to, and
        // the proto3 optional encoding is the one a hand-built descriptor could get wrong in
        // silence: protobuf-java enforces only that synthetic oneofs come last.
        Descriptors.Descriptor presence = TestProtos.presence();
        assertThat(presence.findFieldByName("p_implicit").hasPresence()).isFalse();
        assertThat(presence.findFieldByName("p_choice_a").hasPresence()).isTrue();
        assertThat(presence.findFieldByName("p_optional").hasPresence()).isTrue();
        assertThat(presence.findFieldByName("p_optional").toProto().getProto3Optional()).isTrue();
        assertThat(presence.findFieldByName("p_nested").hasPresence()).isTrue();
    }

    /**
     * The same for proto2, where every singular field has presence — including {@code required},
     * which is the case a presence check alone gets wrong once modes are derived from presence.
     */
    @Test
    void mapsProto2RequiredFieldsToNullableByDefaultToo() {
        TableSchema schema =
                ProtoToTableSchemaConverter.convert(
                        TestProtos.proto2Presence(), ProtoSchemaOptions.defaults());
        Map<String, TableFieldSchema> fields = byName(schema);

        assertThat(fields.get("q_required").getMode()).isEqualTo(TableFieldSchema.Mode.NULLABLE);
        assertThat(fields.get("q_optional").getMode()).isEqualTo(TableFieldSchema.Mode.NULLABLE);
        assertThat(fields.get("q_rep").getMode()).isEqualTo(TableFieldSchema.Mode.REPEATED);

        Descriptors.Descriptor proto2 = TestProtos.proto2Presence();
        assertThat(proto2.findFieldByName("q_required").isRequired()).isTrue();
        assertThat(proto2.findFieldByName("q_required").hasPresence()).isTrue();
        assertThat(proto2.findFieldByName("q_optional").isRequired()).isFalse();
    }

    @Test
    void deriveRequiredColumnsMapsPresencelessProto3FieldsToRequired() {
        TableSchema schema =
                ProtoToTableSchemaConverter.convert(TestProtos.presence(), DERIVE_REQUIRED);
        Map<String, TableFieldSchema> fields = byName(schema);

        // No presence: an unset value reaches the column as "" / 0, never as NULL, so REQUIRED is
        // both faithful and always satisfied.
        assertThat(fields.get("p_implicit").getMode()).isEqualTo(TableFieldSchema.Mode.REQUIRED);
        assertThat(fields.get("p_implicit_int").getMode())
                .isEqualTo(TableFieldSchema.Mode.REQUIRED);
        // Presence, from the oneof, from the optional keyword, and inherent to a message field.
        assertThat(fields.get("p_choice_a").getMode()).isEqualTo(TableFieldSchema.Mode.NULLABLE);
        assertThat(fields.get("p_choice_b").getMode()).isEqualTo(TableFieldSchema.Mode.NULLABLE);
        assertThat(fields.get("p_optional").getMode()).isEqualTo(TableFieldSchema.Mode.NULLABLE);
        assertThat(fields.get("p_nested").getMode()).isEqualTo(TableFieldSchema.Mode.NULLABLE);
        // Repeated has no presence either, so the mode decision has to test it first.
        assertThat(fields.get("p_rep").getMode()).isEqualTo(TableFieldSchema.Mode.REPEATED);

        // And it recurses: the nested struct's own children are derived the same way.
        assertThat(fields.get("p_nested").getFieldsList())
                .extracting(TableFieldSchema::getName, TableFieldSchema::getMode)
                .containsExactly(
                        org.assertj.core.groups.Tuple.tuple(
                                "c_implicit", TableFieldSchema.Mode.REQUIRED),
                        org.assertj.core.groups.Tuple.tuple(
                                "c_optional", TableFieldSchema.Mode.NULLABLE));
    }

    /**
     * proto2 {@code required} is the case {@code hasPresence()} alone gets wrong: it has presence
     * and is mandatory all the same, so the predicate needs its second clause.
     */
    @Test
    void deriveRequiredColumnsKeepsProto2RequiredRequired() {
        TableSchema schema =
                ProtoToTableSchemaConverter.convert(TestProtos.proto2Presence(), DERIVE_REQUIRED);
        Map<String, TableFieldSchema> fields = byName(schema);

        assertThat(fields.get("q_required").getMode()).isEqualTo(TableFieldSchema.Mode.REQUIRED);
        assertThat(fields.get("q_required_child").getMode())
                .isEqualTo(TableFieldSchema.Mode.REQUIRED);
        assertThat(fields.get("q_optional").getMode()).isEqualTo(TableFieldSchema.Mode.NULLABLE);
        assertThat(fields.get("q_optional_child").getMode())
                .isEqualTo(TableFieldSchema.Mode.NULLABLE);
        assertThat(fields.get("q_rep").getMode()).isEqualTo(TableFieldSchema.Mode.REPEATED);
        // That q_required has presence — so presence alone would say NULLABLE here — is asserted by
        // mapsProto2RequiredFieldsToNullableByDefaultToo rather than repeated.
    }

    /**
     * A proto3 map entry's synthetic {@code key} and {@code value} have implicit presence, so they
     * become {@code REQUIRED} — which converges with the Avro path, where a map key is {@code
     * REQUIRED} by default. An entry always carries both, so nothing can leave them unset.
     */
    @Test
    void deriveRequiredColumnsMakesMapKeyAndValueRequired() {
        TableSchema schema =
                ProtoToTableSchemaConverter.convert(TestProtos.allTypes(), DERIVE_REQUIRED);
        Map<String, TableFieldSchema> fields = byName(schema);

        TableFieldSchema map = fields.get("f_map");
        assertThat(map.getMode()).isEqualTo(TableFieldSchema.Mode.REPEATED);
        assertThat(map.getFieldsList())
                .extracting(TableFieldSchema::getName, TableFieldSchema::getMode)
                .containsExactly(
                        org.assertj.core.groups.Tuple.tuple("key", TableFieldSchema.Mode.REQUIRED),
                        org.assertj.core.groups.Tuple.tuple(
                                "value", TableFieldSchema.Mode.REQUIRED));
        // An enum has no presence; a message field always has it, Timestamp included.
        assertThat(fields.get("f_enum").getMode()).isEqualTo(TableFieldSchema.Mode.REQUIRED);
        assertThat(fields.get("f_ts").getMode()).isEqualTo(TableFieldSchema.Mode.NULLABLE);
        assertThat(fields.get("f_nested").getMode()).isEqualTo(TableFieldSchema.Mode.NULLABLE);
    }

    /**
     * A singular JSON column is never REQUIRED. {@code ProtoRowConverter} leaves an unset
     * presence-less JSON string unset rather than writing {@code ""} — and "no presence" is exactly
     * what would make the column REQUIRED, so the two together would fail every record that
     * legitimately omits the field.
     */
    @Test
    void deriveRequiredColumnsLeavesJsonColumnsNullable() {
        ProtoSchemaOptions options =
                ProtoSchemaOptions.builder()
                        .jsonFieldOptionNumber(TestProtos.JSON_OPTION_NUMBER)
                        .deriveRequiredColumns()
                        .build();
        Map<String, TableFieldSchema> fields =
                byName(ProtoToTableSchemaConverter.convert(TestProtos.annotated(), options));

        assertThat(fields.get("a_string").getType()).isEqualTo(TableFieldSchema.Type.JSON);
        assertThat(fields.get("a_string").getMode()).isEqualTo(TableFieldSchema.Mode.NULLABLE);
        assertThat(fields.get("a_message").getMode()).isEqualTo(TableFieldSchema.Mode.NULLABLE);
        // A repeated JSON field stays REPEATED: the mode decision tests repeated before JSON.
        assertThat(fields.get("a_rep_string").getType()).isEqualTo(TableFieldSchema.Type.JSON);
        assertThat(fields.get("a_rep_string").getMode()).isEqualTo(TableFieldSchema.Mode.REPEATED);
        // The exception is about JSON, not about the option: an identically shaped plain string
        // field in the same message is REQUIRED.
        assertThat(fields.get("a_plain").getType()).isEqualTo(TableFieldSchema.Type.STRING);
        assertThat(fields.get("a_plain").getMode()).isEqualTo(TableFieldSchema.Mode.REQUIRED);
        assertThat(TestProtos.annotated().findFieldByName("a_string").hasPresence()).isFalse();
    }

    @Test
    void mapsNestedMessagesToStructs() {
        TableSchema schema =
                ProtoToTableSchemaConverter.convert(
                        TestProtos.allTypes(), ProtoSchemaOptions.defaults());
        TableFieldSchema nested = byName(schema).get("f_nested");

        assertThat(nested.getType()).isEqualTo(TableFieldSchema.Type.STRUCT);
        assertThat(nested.getFieldsList())
                .extracting(TableFieldSchema::getName, TableFieldSchema::getType)
                .containsExactly(
                        org.assertj.core.groups.Tuple.tuple("s", TableFieldSchema.Type.STRING),
                        org.assertj.core.groups.Tuple.tuple("n", TableFieldSchema.Type.INT64));
    }

    @Test
    void mapsRepeatedAndMapFields() {
        TableSchema schema =
                ProtoToTableSchemaConverter.convert(
                        TestProtos.allTypes(), ProtoSchemaOptions.defaults());
        Map<String, TableFieldSchema> fields = byName(schema);

        TableFieldSchema repeated = fields.get("f_rep_string");
        assertThat(repeated.getMode()).isEqualTo(TableFieldSchema.Mode.REPEATED);
        assertThat(repeated.getType()).isEqualTo(TableFieldSchema.Type.STRING);

        TableFieldSchema map = fields.get("f_map");
        assertThat(map.getMode()).isEqualTo(TableFieldSchema.Mode.REPEATED);
        assertThat(map.getType()).isEqualTo(TableFieldSchema.Type.STRUCT);
        assertThat(map.getFieldsList())
                .extracting(TableFieldSchema::getName, TableFieldSchema::getType)
                .containsExactly(
                        org.assertj.core.groups.Tuple.tuple("key", TableFieldSchema.Type.STRING),
                        org.assertj.core.groups.Tuple.tuple("value", TableFieldSchema.Type.INT64));
    }

    @Test
    void mapsConfiguredMessageFieldsToJson() {
        ProtoSchemaOptions options = ProtoSchemaOptions.builder().jsonFieldPath("f_json").build();
        TableSchema schema = ProtoToTableSchemaConverter.convert(TestProtos.allTypes(), options);

        TableFieldSchema json = byName(schema).get("f_json");
        assertThat(json.getType()).isEqualTo(TableFieldSchema.Type.JSON);
        assertThat(json.getFieldsList()).isEmpty();
    }

    @Test
    void mapsConfiguredStringFieldsToJson() {
        ProtoSchemaOptions options = ProtoSchemaOptions.builder().jsonFieldPath("f_string").build();
        TableSchema schema = ProtoToTableSchemaConverter.convert(TestProtos.allTypes(), options);

        TableFieldSchema json = byName(schema).get("f_string");
        assertThat(json.getType()).isEqualTo(TableFieldSchema.Type.JSON);
        assertThat(json.getMode()).isEqualTo(TableFieldSchema.Mode.NULLABLE);
    }

    @Test
    void mapsConfiguredNestedFieldsToJson() {
        ProtoSchemaOptions options =
                ProtoSchemaOptions.builder().jsonFieldPath("f_nested.s").build();
        TableSchema schema = ProtoToTableSchemaConverter.convert(TestProtos.allTypes(), options);

        TableFieldSchema nested = byName(schema).get("f_nested");
        assertThat(nested.getType()).isEqualTo(TableFieldSchema.Type.STRUCT);
        assertThat(nested.getFieldsList())
                .extracting(TableFieldSchema::getName, TableFieldSchema::getType)
                .containsExactly(
                        org.assertj.core.groups.Tuple.tuple("s", TableFieldSchema.Type.JSON),
                        org.assertj.core.groups.Tuple.tuple("n", TableFieldSchema.Type.INT64));
    }

    @Test
    void rejectsJsonMappingOnFieldsThatAreNeitherMessageNorString() {
        ProtoSchemaOptions options = ProtoSchemaOptions.builder().jsonFieldPath("f_int32").build();

        assertThatThrownBy(
                        () -> ProtoToTableSchemaConverter.convert(TestProtos.allTypes(), options))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("f_int32");
    }

    @Test
    void rejectsJsonMappingOnMapFields() {
        ProtoSchemaOptions options = ProtoSchemaOptions.builder().jsonFieldPath("f_map").build();

        assertThatThrownBy(
                        () -> ProtoToTableSchemaConverter.convert(TestProtos.allTypes(), options))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("f_map");
    }

    @Test
    void rejectsCaseInsensitiveDuplicateFieldNames() {
        assertThatThrownBy(
                        () ->
                                ProtoToTableSchemaConverter.convert(
                                        TestProtos.caseCollision(), ProtoSchemaOptions.defaults()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("case");
    }

    @Test
    void rejectsJsonPathsMatchingNoField() {
        ProtoSchemaOptions options = ProtoSchemaOptions.builder().jsonFieldPath("f_jsonn").build();

        assertThatThrownBy(
                        () -> ProtoToTableSchemaConverter.convert(TestProtos.allTypes(), options))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("f_jsonn");
    }

    @Test
    void rejectsRecursiveMessages() {
        assertThatThrownBy(
                        () ->
                                ProtoToTableSchemaConverter.convert(
                                        TestProtos.recursive(), ProtoSchemaOptions.defaults()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Recursive");
    }

    @ParameterizedTest(name = "throughBytes={0}")
    @ValueSource(booleans = {false, true})
    void mapsOptionMarkedFieldsToJson(boolean throughBytes) {
        ProtoSchemaOptions options =
                ProtoSchemaOptions.builder()
                        .jsonFieldOptionNumber(TestProtos.JSON_OPTION_NUMBER)
                        .build();
        TableSchema schema = ProtoToTableSchemaConverter.convert(annotated(throughBytes), options);
        Map<String, TableFieldSchema> fields = byName(schema);

        assertThat(fields.get("a_string").getType()).isEqualTo(TableFieldSchema.Type.JSON);
        assertThat(fields.get("a_string").getMode()).isEqualTo(TableFieldSchema.Mode.NULLABLE);
        assertThat(fields.get("a_message").getType()).isEqualTo(TableFieldSchema.Type.JSON);
        assertThat(fields.get("a_message").getMode()).isEqualTo(TableFieldSchema.Mode.NULLABLE);
        assertThat(fields.get("a_message").getFieldsList()).isEmpty();

        assertThat(fields.get("a_plain").getType()).isEqualTo(TableFieldSchema.Type.STRING);
        assertThat(fields.get("a_false").getType()).isEqualTo(TableFieldSchema.Type.STRING);
        assertThat(fields.get("a_other").getType()).isEqualTo(TableFieldSchema.Type.STRING);

        for (String repeated : new String[] {"a_rep_string", "a_rep_message"}) {
            assertThat(fields.get(repeated).getType())
                    .as(repeated)
                    .isEqualTo(TableFieldSchema.Type.JSON);
            assertThat(fields.get(repeated).getMode())
                    .as(repeated)
                    .isEqualTo(TableFieldSchema.Mode.REPEATED);
        }
    }

    @ParameterizedTest(name = "throughBytes={0}")
    @ValueSource(booleans = {false, true})
    void findsOptionMarkedFieldsAtAnyDepth(boolean throughBytes) {
        ProtoSchemaOptions options =
                ProtoSchemaOptions.builder()
                        .jsonFieldOptionNumber(TestProtos.JSON_OPTION_NUMBER)
                        .build();
        TableSchema schema = ProtoToTableSchemaConverter.convert(annotated(throughBytes), options);

        TableFieldSchema nested = byName(schema).get("a_nested");
        assertThat(nested.getType()).isEqualTo(TableFieldSchema.Type.STRUCT);
        assertThat(nested.getFieldsList())
                .extracting(TableFieldSchema::getName, TableFieldSchema::getType)
                .containsExactly(
                        org.assertj.core.groups.Tuple.tuple("n_json", TableFieldSchema.Type.JSON),
                        org.assertj.core.groups.Tuple.tuple(
                                "n_plain", TableFieldSchema.Type.STRING));
    }

    @Test
    void aRivalAnnotationAtTheSameNumberDoesNotProduceAJsonColumn() {
        // The whole point of passing the generated extension: `colliding.proto` is annotated by a
        // different annotations proto that also claims number 50000, and the resulting column must
        // stay STRING. Configured by number alone the same field would become JSON.
        ProtoSchemaOptions byExtension =
                ProtoSchemaOptions.builder()
                        .jsonFieldOption(TestProtos.jsonOptionExtension())
                        .build();
        ProtoSchemaOptions byNumber =
                ProtoSchemaOptions.builder()
                        .jsonFieldOptionNumber(TestProtos.JSON_OPTION_NUMBER)
                        .build();

        assertThat(
                        byName(
                                        ProtoToTableSchemaConverter.convert(
                                                TestProtos.collidingAnnotated(), byExtension))
                                .get("c_string")
                                .getType())
                .isEqualTo(TableFieldSchema.Type.STRING);
        assertThat(
                        byName(
                                        ProtoToTableSchemaConverter.convert(
                                                TestProtos.collidingAnnotated(), byNumber))
                                .get("c_string")
                                .getType())
                .as("by number alone there is nothing to tell the two options apart")
                .isEqualTo(TableFieldSchema.Type.JSON);
    }

    @ParameterizedTest(name = "throughBytes={0}")
    @ValueSource(booleans = {false, true})
    void unionsSeveralFieldOptions(boolean throughBytes) {
        // Two annotation vocabularies in one job: a_string carries the JSON option, a_other carries
        // the second one, and configuring both must map both. With a single-valued option the
        // second registration would have replaced the first and a_string would stay STRING.
        ProtoSchemaOptions options =
                ProtoSchemaOptions.builder()
                        .jsonFieldOption(TestProtos.jsonOptionExtension())
                        .jsonFieldOptionNumber(TestProtos.OTHER_OPTION_NUMBER)
                        .build();
        Map<String, TableFieldSchema> fields =
                byName(ProtoToTableSchemaConverter.convert(annotated(throughBytes), options));

        assertThat(fields.get("a_string").getType()).isEqualTo(TableFieldSchema.Type.JSON);
        assertThat(fields.get("a_other").getType()).isEqualTo(TableFieldSchema.Type.JSON);
        assertThat(fields.get("a_plain").getType()).isEqualTo(TableFieldSchema.Type.STRING);
    }

    @Test
    void unionsFieldPathsAndFieldOptions() {
        ProtoSchemaOptions options =
                ProtoSchemaOptions.builder()
                        .jsonFieldOptionNumber(TestProtos.JSON_OPTION_NUMBER)
                        .jsonFieldPath("a_plain")
                        // Also naming a field the option already marks must not double-count it in
                        // the matched-path bookkeeping, nor make it any less of a JSON column.
                        .jsonFieldPath("a_string")
                        .build();
        Map<String, TableFieldSchema> fields =
                byName(ProtoToTableSchemaConverter.convert(TestProtos.annotated(), options));

        assertThat(fields.get("a_plain").getType()).isEqualTo(TableFieldSchema.Type.JSON);
        assertThat(fields.get("a_string").getType()).isEqualTo(TableFieldSchema.Type.JSON);
    }

    @Test
    void acceptsAFieldOptionNumberMatchingNoField() {
        // Unlike a path, a number matching nothing is legitimate: a message need not have JSON
        // columns. The cost is that a mistyped number is silent, which the javadoc calls out.
        ProtoSchemaOptions options =
                ProtoSchemaOptions.builder()
                        .jsonFieldOptionNumber(TestProtos.JSON_OPTION_NUMBER)
                        .build();
        TableSchema schema = ProtoToTableSchemaConverter.convert(TestProtos.allTypes(), options);

        assertThat(schema.getFieldsList())
                .noneMatch(field -> field.getType() == TableFieldSchema.Type.JSON);
    }

    @Test
    void rejectsOptionMarkedFieldsThatAreNeitherMessageNorString() {
        ProtoSchemaOptions options =
                ProtoSchemaOptions.builder()
                        .jsonFieldOptionNumber(TestProtos.JSON_OPTION_NUMBER)
                        .build();

        assertThatThrownBy(
                        () ->
                                ProtoToTableSchemaConverter.convert(
                                        TestProtos.annotatedBadType(), options))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("b_int")
                // Not just "it threw": the message must name why, or a change reporting every
                // rejection as a map field would go unnoticed.
                .hasMessageContaining("LONG");
    }

    @Test
    void mapsWrapperTypesToTheWrappedScalar() {
        Map<String, TableFieldSchema> fields =
                byName(wellKnownTypes(ProtoSchemaOptions.defaults()));

        assertThat(fields.get("w_int32").getType()).isEqualTo(TableFieldSchema.Type.INT64);
        assertThat(fields.get("w_uint32").getType()).isEqualTo(TableFieldSchema.Type.INT64);
        assertThat(fields.get("w_int64").getType()).isEqualTo(TableFieldSchema.Type.INT64);
        assertThat(fields.get("w_uint64").getType()).isEqualTo(TableFieldSchema.Type.INT64);
        assertThat(fields.get("w_float").getType()).isEqualTo(TableFieldSchema.Type.DOUBLE);
        assertThat(fields.get("w_double").getType()).isEqualTo(TableFieldSchema.Type.DOUBLE);
        assertThat(fields.get("w_bool").getType()).isEqualTo(TableFieldSchema.Type.BOOL);
        assertThat(fields.get("w_string").getType()).isEqualTo(TableFieldSchema.Type.STRING);
        assertThat(fields.get("w_bytes").getType()).isEqualTo(TableFieldSchema.Type.BYTES);

        // Flattened, not expanded: a STRUCT<value> leaves every query saying `w_int64.value`.
        assertThat(fields.get("w_int64").getFieldsList()).isEmpty();
    }

    /**
     * A wrapper is a message field, so it has presence and stays {@code NULLABLE} — the very
     * distinction the type exists to express. The bare scalar beside it shows the option is on.
     */
    @Test
    void mapsWrapperTypesToNullableEvenWhenRequiredColumnsAreDerived() {
        Map<String, TableFieldSchema> fields = byName(wellKnownTypes(DERIVE_REQUIRED));

        assertThat(fields.get("w_int64").getMode()).isEqualTo(TableFieldSchema.Mode.NULLABLE);
        assertThat(fields.get("w_string").getMode()).isEqualTo(TableFieldSchema.Mode.NULLABLE);
        assertThat(fields.get("w_duration").getMode()).isEqualTo(TableFieldSchema.Mode.NULLABLE);
        assertThat(fields.get("w_mask").getMode()).isEqualTo(TableFieldSchema.Mode.NULLABLE);
        assertThat(fields.get("w_struct").getMode()).isEqualTo(TableFieldSchema.Mode.NULLABLE);

        Map<String, TableFieldSchema> presence =
                byName(ProtoToTableSchemaConverter.convert(TestProtos.presence(), DERIVE_REQUIRED));
        assertThat(presence.get("p_implicit").getMode()).isEqualTo(TableFieldSchema.Mode.REQUIRED);
    }

    /**
     * The one documented deviation from "a well-known type column is always NULLABLE". A proto2
     * {@code required} wrapper is mandatory, so {@code REQUIRED} is faithful; the {@code optional}
     * sibling shows the deviation is about {@code required} and not about proto2.
     */
    @Test
    void derivesRequiredForAProto2RequiredWrapper() {
        Map<String, TableFieldSchema> fields =
                byName(
                        ProtoToTableSchemaConverter.convert(
                                TestProtos.proto2WellKnownTypes(), DERIVE_REQUIRED));

        assertThat(fields.get("r_required").getType()).isEqualTo(TableFieldSchema.Type.INT64);
        assertThat(fields.get("r_required").getMode()).isEqualTo(TableFieldSchema.Mode.REQUIRED);
        assertThat(fields.get("r_optional").getMode()).isEqualTo(TableFieldSchema.Mode.NULLABLE);

        // Without the option both are NULLABLE, so the assertion above is about the option and not
        // about proto2.
        assertThat(
                        byName(
                                        ProtoToTableSchemaConverter.convert(
                                                TestProtos.proto2WellKnownTypes(),
                                                ProtoSchemaOptions.defaults()))
                                .get("r_required")
                                .getMode())
                .isEqualTo(TableFieldSchema.Mode.NULLABLE);
    }

    /**
     * Struct, Value and ListValue are mutually recursive, so before they were mapped to JSON the
     * recursion guard failed the whole job at schema derivation. Nothing here may throw.
     */
    @Test
    void mapsStructValueAndListValueToJsonColumns() {
        Map<String, TableFieldSchema> fields =
                byName(wellKnownTypes(ProtoSchemaOptions.defaults()));

        assertThat(fields.get("w_struct").getType()).isEqualTo(TableFieldSchema.Type.JSON);
        assertThat(fields.get("w_value").getType()).isEqualTo(TableFieldSchema.Type.JSON);
        assertThat(fields.get("w_list").getType()).isEqualTo(TableFieldSchema.Type.JSON);
        assertThat(fields.get("w_struct").getFieldsList()).isEmpty();
    }

    @Test
    void mapsDurationToInt64AndFieldMaskToString() {
        Map<String, TableFieldSchema> fields =
                byName(wellKnownTypes(ProtoSchemaOptions.defaults()));

        assertThat(fields.get("w_duration").getType()).isEqualTo(TableFieldSchema.Type.INT64);
        assertThat(fields.get("w_duration").getFieldsList()).isEmpty();
        assertThat(fields.get("w_mask").getType()).isEqualTo(TableFieldSchema.Type.STRING);
        assertThat(fields.get("w_mask").getFieldsList()).isEmpty();
        assertThat(fields.get("w_ts").getType()).isEqualTo(TableFieldSchema.Type.TIMESTAMP);
    }

    /**
     * Any is deliberately not recognised: its payload cannot be expanded without the descriptor its
     * type URL names. Pinned so that mapping it has to be a deliberate edit here.
     */
    @Test
    void leavesAnyAsAStruct() {
        TableFieldSchema any = byName(wellKnownTypes(ProtoSchemaOptions.defaults())).get("w_any");

        assertThat(any.getType()).isEqualTo(TableFieldSchema.Type.STRUCT);
        assertThat(any.getFieldsList())
                .extracting(TableFieldSchema::getName, TableFieldSchema::getType)
                .containsExactly(
                        org.assertj.core.groups.Tuple.tuple(
                                "type_url", TableFieldSchema.Type.STRING),
                        org.assertj.core.groups.Tuple.tuple("value", TableFieldSchema.Type.BYTES));
    }

    @Test
    void mapsRepeatedWellKnownTypesToRepeatedColumns() {
        Map<String, TableFieldSchema> fields = byName(wellKnownTypes(DERIVE_REQUIRED));

        assertThat(fields.get("w_rep_int64").getType()).isEqualTo(TableFieldSchema.Type.INT64);
        assertThat(fields.get("w_rep_int64").getMode()).isEqualTo(TableFieldSchema.Mode.REPEATED);
        assertThat(fields.get("w_rep_struct").getType()).isEqualTo(TableFieldSchema.Type.JSON);
        assertThat(fields.get("w_rep_struct").getMode()).isEqualTo(TableFieldSchema.Mode.REPEATED);
        assertThat(fields.get("w_rep_duration").getType()).isEqualTo(TableFieldSchema.Type.INT64);
        assertThat(fields.get("w_rep_duration").getMode())
                .isEqualTo(TableFieldSchema.Mode.REPEATED);
    }

    /**
     * A map's value is an ordinary message field of the synthesized entry, so recognition reaches
     * it with no map-specific rule — and it keeps presence, so it stays {@code NULLABLE} where the
     * entry's {@code key} becomes {@code REQUIRED}.
     */
    @Test
    void mapsWellKnownTypesInsideMapValues() {
        Map<String, TableFieldSchema> fields = byName(wellKnownTypes(DERIVE_REQUIRED));
        Map<String, TableFieldSchema> intEntry = subFieldsByName(fields.get("w_map_int64"));

        assertThat(intEntry.get("value").getType()).isEqualTo(TableFieldSchema.Type.INT64);
        assertThat(intEntry.get("value").getMode()).isEqualTo(TableFieldSchema.Mode.NULLABLE);
        assertThat(intEntry.get("key").getMode()).isEqualTo(TableFieldSchema.Mode.REQUIRED);
        assertThat(subFieldsByName(fields.get("w_map_struct")).get("value").getType())
                .isEqualTo(TableFieldSchema.Type.JSON);
    }

    @Test
    void recognisesWellKnownTypesBelowTheRootMessage() {
        TableFieldSchema child =
                byName(wellKnownTypes(ProtoSchemaOptions.defaults())).get("w_child");

        assertThat(child.getType()).isEqualTo(TableFieldSchema.Type.STRUCT);
        assertThat(child.getFieldsList())
                .extracting(TableFieldSchema::getName, TableFieldSchema::getType)
                .containsExactly(
                        org.assertj.core.groups.Tuple.tuple(
                                "c_string", TableFieldSchema.Type.STRING),
                        org.assertj.core.groups.Tuple.tuple(
                                "c_duration", TableFieldSchema.Type.INT64));
    }

    /**
     * Explicit configuration wins over every well-known-type mapping, because the JSON branch
     * returns before the message type is ever inspected. Reordering the two would unwrap a field
     * the user asked to keep whole.
     */
    @Test
    void aConfiguredJsonPathWinsOverWellKnownTypeRecognition() {
        Map<String, TableFieldSchema> fields =
                byName(
                        wellKnownTypes(
                                ProtoSchemaOptions.builder()
                                        .jsonFieldPath("w_int64")
                                        .jsonFieldPath("w_ts")
                                        .build()));

        assertThat(fields.get("w_int64").getType()).isEqualTo(TableFieldSchema.Type.JSON);
        assertThat(fields.get("w_ts").getType()).isEqualTo(TableFieldSchema.Type.JSON);
    }

    /**
     * Automatic JSON columns record themselves as matched paths, which must not let a genuinely
     * unmatched configured path pass as matched.
     */
    @Test
    void stillRejectsJsonPathsMatchingNoFieldBesideAutomaticJsonColumns() {
        ProtoSchemaOptions options =
                ProtoSchemaOptions.builder().jsonFieldPath("w_nonexistent").build();

        assertThatThrownBy(() -> wellKnownTypes(options))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("w_nonexistent");
    }

    /** A Struct is one column, so it has no navigable sub-paths left to configure. */
    @Test
    void rejectsJsonPathsPointingInsideAJsonMappedWellKnownType() {
        ProtoSchemaOptions options =
                ProtoSchemaOptions.builder().jsonFieldPath("w_struct.fields").build();

        assertThatThrownBy(() -> wellKnownTypes(options))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("w_struct.fields");
    }

    /**
     * Measured: the BigQuery client library rejects a zero-sub-field RECORD itself, before a
     * request is ever sent, with a message naming no field. Rejecting here says which field it was.
     */
    @Test
    void rejectsMessagesWithNoFields() {
        assertThatThrownBy(
                        () ->
                                ProtoToTableSchemaConverter.convert(
                                        TestProtos.emptyWellKnownType(),
                                        ProtoSchemaOptions.defaults()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("google.protobuf.Empty")
                .hasMessageContaining("w_empty");
    }

    private static TableSchema wellKnownTypes(ProtoSchemaOptions options) {
        return ProtoToTableSchemaConverter.convert(TestProtos.wellKnownTypes(), options);
    }

    private static Descriptors.Descriptor annotated(boolean throughBytes) {
        return throughBytes ? TestProtos.annotatedFromBytes() : TestProtos.annotated();
    }

    private static Map<String, TableFieldSchema> byName(TableSchema schema) {
        return schema.getFieldsList().stream()
                .collect(Collectors.toMap(TableFieldSchema::getName, Function.identity()));
    }

    private static Map<String, TableFieldSchema> subFieldsByName(TableFieldSchema field) {
        return field.getFieldsList().stream()
                .collect(Collectors.toMap(TableFieldSchema::getName, Function.identity()));
    }
}
