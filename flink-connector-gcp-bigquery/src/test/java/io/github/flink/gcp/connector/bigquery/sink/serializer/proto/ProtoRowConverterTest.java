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

import com.google.cloud.bigquery.storage.v1.BQTableSchemaToProtoDescriptor;
import com.google.protobuf.Any;
import com.google.protobuf.BoolValue;
import com.google.protobuf.ByteString;
import com.google.protobuf.BytesValue;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DoubleValue;
import com.google.protobuf.Duration;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.FieldMask;
import com.google.protobuf.FloatValue;
import com.google.protobuf.Int32Value;
import com.google.protobuf.Int64Value;
import com.google.protobuf.ListValue;
import com.google.protobuf.NullValue;
import com.google.protobuf.StringValue;
import com.google.protobuf.Struct;
import com.google.protobuf.Timestamp;
import com.google.protobuf.UInt32Value;
import com.google.protobuf.UInt64Value;
import com.google.protobuf.UninitializedMessageException;
import com.google.protobuf.Value;
import com.google.protobuf.util.Timestamps;
import io.github.flink.gcp.connector.bigquery.testproto.Presence;
import io.github.flink.gcp.connector.bigquery.testproto.PresenceChild;
import io.github.flink.gcp.connector.bigquery.testproto.Proto2Child;
import io.github.flink.gcp.connector.bigquery.testproto.Proto2Presence;
import io.github.flink.gcp.connector.bigquery.testproto.WellKnown;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link ProtoRowConverter}. */
class ProtoRowConverterTest {

    private static final ProtoSchemaOptions OPTIONS =
            ProtoSchemaOptions.builder().jsonFieldPath("f_json").build();

    @Test
    void convertsTheFullTypeMatrix() throws Exception {
        Descriptors.Descriptor source = TestProtos.allTypes();
        ProtoRowConverter converter = converter(source, OPTIONS);

        Instant instant = Instant.parse("2026-01-02T03:04:05.123456789Z");
        Timestamp ts =
                Timestamp.newBuilder()
                        .setSeconds(instant.getEpochSecond())
                        .setNanos(instant.getNano())
                        .build();
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(source);
        set(builder, source, "f_int32", -42);
        set(builder, source, "f_int64", 43L);
        set(builder, source, "f_uint32", (int) 3_000_000_000L);
        set(builder, source, "f_uint64", Long.MAX_VALUE);
        set(builder, source, "f_float", 1.5f);
        set(builder, source, "f_double", 2.5d);
        set(builder, source, "f_bool", true);
        set(builder, source, "f_string", "hello");
        set(builder, source, "f_bytes", ByteString.copyFromUtf8("raw"));
        set(
                builder,
                source,
                "f_enum",
                source.getFile().findEnumTypeByName("Color").findValueByName("RED"));
        set(builder, source, "f_ts", ts);
        Descriptors.Descriptor nestedType = source.getFile().findMessageTypeByName("Nested");
        set(
                builder,
                source,
                "f_nested",
                DynamicMessage.newBuilder(nestedType)
                        .setField(nestedType.findFieldByName("s"), "x")
                        .setField(nestedType.findFieldByName("n"), 7L)
                        .build());
        builder.addRepeatedField(source.findFieldByName("f_rep_string"), "a");
        builder.addRepeatedField(source.findFieldByName("f_rep_string"), "b");
        Descriptors.Descriptor entryType = source.findFieldByName("f_map").getMessageType();
        builder.addRepeatedField(
                source.findFieldByName("f_map"),
                DynamicMessage.newBuilder(entryType)
                        .setField(entryType.findFieldByName("key"), "k1")
                        .setField(entryType.findFieldByName("value"), 100L)
                        .build());
        set(
                builder,
                source,
                "f_json",
                DynamicMessage.newBuilder(nestedType)
                        .setField(nestedType.findFieldByName("s"), "jsonvalue")
                        .setField(nestedType.findFieldByName("n"), 9L)
                        .build());
        builder.addRepeatedField(source.findFieldByName("f_rep_ts"), ts);
        builder.addRepeatedField(source.findFieldByName("f_rep_ts"), ts);

        DynamicMessage row = converter.convert(builder.build());

        assertThat(get(row, "f_int32")).isEqualTo(-42L);
        assertThat(get(row, "f_int64")).isEqualTo(43L);
        assertThat(get(row, "f_uint32")).isEqualTo(3_000_000_000L);
        assertThat(get(row, "f_uint64")).isEqualTo(Long.MAX_VALUE);
        assertThat(get(row, "f_float")).isEqualTo(1.5d);
        assertThat(get(row, "f_double")).isEqualTo(2.5d);
        assertThat(get(row, "f_bool")).isEqualTo(true);
        assertThat(get(row, "f_string")).isEqualTo("hello");
        assertThat(get(row, "f_bytes")).isEqualTo(ByteString.copyFromUtf8("raw"));
        assertThat(get(row, "f_enum")).isEqualTo("RED");
        long expectedMicros = Timestamps.toMicros(ts);
        assertThat(get(row, "f_ts")).isEqualTo(expectedMicros);

        DynamicMessage nestedRow = (DynamicMessage) get(row, "f_nested");
        assertThat(get(nestedRow, "s")).isEqualTo("x");
        assertThat(get(nestedRow, "n")).isEqualTo(7L);

        assertThat(get(row, "f_rep_string")).isEqualTo(Arrays.asList("a", "b"));

        List<?> mapEntries = (List<?>) get(row, "f_map");
        assertThat(mapEntries).hasSize(1);
        DynamicMessage entryRow = (DynamicMessage) mapEntries.get(0);
        assertThat(get(entryRow, "key")).isEqualTo("k1");
        assertThat(get(entryRow, "value")).isEqualTo(100L);

        assertThat((String) get(row, "f_json"))
                .contains("\"s\":\"jsonvalue\"")
                .contains("\"n\":\"9\"");

        assertThat(get(row, "f_rep_ts")).isEqualTo(Arrays.asList(expectedMicros, expectedMicros));
    }

    @Test
    void rejectsUnrepresentableUint64Values() throws Exception {
        Descriptors.Descriptor source = TestProtos.allTypes();
        ProtoRowConverter converter = converter(source, OPTIONS);

        DynamicMessage message =
                DynamicMessage.newBuilder(source)
                        .setField(source.findFieldByName("f_uint64"), 0xF000000000000000L)
                        .build();

        assertThatThrownBy(() -> converter.convert(message))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("f_uint64")
                .hasMessageContaining("17293822569102704640");
    }

    @Test
    void leavesUnsetMessageFieldsUnset() throws Exception {
        Descriptors.Descriptor source = TestProtos.allTypes();
        ProtoRowConverter converter = converter(source, OPTIONS);

        DynamicMessage row =
                converter.convert(
                        DynamicMessage.newBuilder(source)
                                .setField(source.findFieldByName("f_string"), "only")
                                .build());

        assertThat(row.hasField(row.getDescriptorForType().findFieldByName("f_nested"))).isFalse();
        assertThat(row.hasField(row.getDescriptorForType().findFieldByName("f_ts"))).isFalse();
        assertThat(get(row, "f_string")).isEqualTo("only");
    }

    @Test
    void writesJsonMappedStringsThroughVerbatim() throws Exception {
        Descriptors.Descriptor source = TestProtos.annotated();
        ProtoSchemaOptions options =
                ProtoSchemaOptions.builder()
                        .jsonFieldOptionNumber(TestProtos.JSON_OPTION_NUMBER)
                        .build();
        ProtoRowConverter converter = converter(source, options);

        Descriptors.Descriptor payloadType = source.getFile().findMessageTypeByName("APayload");
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(source);
        set(builder, source, "a_string", "{\"k\":1}");
        set(
                builder,
                source,
                "a_message",
                DynamicMessage.newBuilder(payloadType)
                        .setField(payloadType.findFieldByName("s"), "printed")
                        .build());
        builder.addRepeatedField(source.findFieldByName("a_rep_string"), "[1,2]");
        builder.addRepeatedField(source.findFieldByName("a_rep_string"), "{}");
        // An element is explicit even when empty, so unlike a no-presence singular field it is not
        // dropped — the other half of the empty-string rule.
        builder.addRepeatedField(source.findFieldByName("a_rep_string"), "");
        // Malformed JSON is BigQuery's to reject as a row-level error; validating every record
        // client-side would defeat the point of a passthrough, so it must survive unchanged.
        builder.addRepeatedField(source.findFieldByName("a_rep_string"), "not json at all");
        builder.addRepeatedField(
                source.findFieldByName("a_rep_message"),
                DynamicMessage.newBuilder(payloadType)
                        .setField(payloadType.findFieldByName("s"), "element")
                        .build());

        DynamicMessage row = converter.convert(builder.build());

        // A JSON column travels as a string, so a JSON-mapped string needs no conversion at all:
        // byte-for-byte what the record carried, not a re-serialized form.
        assertThat(get(row, "a_string")).isEqualTo("{\"k\":1}");
        assertThat(get(row, "a_rep_string"))
                .isEqualTo(Arrays.asList("[1,2]", "{}", "", "not json at all"));
        // A JSON-mapped message is still printed as canonical protobuf JSON, singular or repeated.
        assertThat((String) get(row, "a_message")).contains("\"s\":\"printed\"");
        assertThat((List<?>) get(row, "a_rep_message"))
                .singleElement(org.assertj.core.api.InstanceOfAssertFactories.STRING)
                .contains("\"s\":\"element\"");
    }

    @Test
    void leavesUnsetJsonMappedStringsUnsetRatherThanWritingAnEmptyString() throws Exception {
        // a_string is a plain proto3 scalar, so an unset value arrives as "" — which is not valid
        // JSON. Writing it would fail every record that legitimately omits the field, since the
        // row descriptor's JSON field does have presence and would carry the empty string.
        Descriptors.Descriptor source = TestProtos.annotated();
        ProtoSchemaOptions options =
                ProtoSchemaOptions.builder()
                        .jsonFieldOptionNumber(TestProtos.JSON_OPTION_NUMBER)
                        .build();
        ProtoRowConverter converter = converter(source, options);

        DynamicMessage row = converter.convert(DynamicMessage.newBuilder(source).build());

        Descriptors.Descriptor rowType = row.getDescriptorForType();
        assertThat(rowType.findFieldByName("a_string").hasPresence()).isTrue();
        assertThat(row.hasField(rowType.findFieldByName("a_string"))).isFalse();
        // A plain string column is unaffected: "" stays a legitimate value there. Asserted through
        // hasField, since getField returns "" for a set and an unset field alike.
        assertThat(row.hasField(rowType.findFieldByName("a_plain"))).isTrue();
        assertThat(row.getField(rowType.findFieldByName("a_plain"))).isEqualTo("");
    }

    /**
     * A {@code oneof} has no BigQuery counterpart, so each member becomes its own column and the
     * branch not taken must read back as NULL. That falls out of the presence check in {@code
     * MessagePlan.convert}: a oneof member always has explicit presence. Delete that check and an
     * unselected {@code int64} branch silently writes 0 and a {@code string} branch "" — which is
     * why the presence-less field is asserted alongside, as the contrast.
     */
    @Test
    void leavesTheUnselectedOneofBranchUnset() throws Exception {
        ProtoRowConverter converter =
                converter(TestProtos.presence(), ProtoSchemaOptions.defaults());

        DynamicMessage row = converter.convert(Presence.newBuilder().setPChoiceB(7L).build());

        Descriptors.Descriptor rowType = row.getDescriptorForType();
        assertThat(row.hasField(rowType.findFieldByName("p_choice_b"))).isTrue();
        assertThat(get(row, "p_choice_b")).isEqualTo(7L);
        assertThat(row.hasField(rowType.findFieldByName("p_choice_a"))).isFalse();
        assertThat(row.hasField(rowType.findFieldByName("p_optional"))).isFalse();
        // The contrast: a field without presence cannot say "unset", so it is written as its type
        // default rather than skipped.
        assertThat(row.hasField(rowType.findFieldByName("p_implicit"))).isTrue();
        assertThat(get(row, "p_implicit")).isEqualTo("");
    }

    /**
     * Deriving REQUIRED must never poison an ordinary record. The row descriptor is built without a
     * syntax, so {@code BQTableSchemaToProtoDescriptor} maps REQUIRED to a proto2 {@code
     * LABEL_REQUIRED} field that {@code build()} enforces — and every column the predicate makes
     * REQUIRED is one the value path always writes. Reaching this test at all is the assertion:
     * {@code build()} throws rather than returning a partial message.
     */
    @Test
    void writesEveryDerivedRequiredColumn() throws Exception {
        ProtoSchemaOptions options = ProtoSchemaOptions.builder().deriveRequiredColumns().build();
        ProtoRowConverter converter = converter(TestProtos.presence(), options);

        DynamicMessage row = converter.convert(Presence.getDefaultInstance());

        Descriptors.Descriptor rowType = row.getDescriptorForType();
        assertThat(rowType.findFieldByName("p_implicit").isRequired()).isTrue();
        assertThat(get(row, "p_implicit")).isEqualTo("");
        assertThat(get(row, "p_implicit_int")).isEqualTo(0L);
        assertThat(row.hasField(rowType.findFieldByName("p_nested"))).isFalse();
    }

    /**
     * The same, one level down: a REQUIRED column inside a NULLABLE {@code STRUCT}. An unset nested
     * message is skipped whole, so the case only arises when the struct is present but empty — and
     * then its presence-less children have to be written for the nested {@code build()} to succeed,
     * which is the half an all-unset record never exercises.
     */
    @Test
    void writesRequiredColumnsInsideAPresentButEmptyStruct() throws Exception {
        ProtoSchemaOptions options = ProtoSchemaOptions.builder().deriveRequiredColumns().build();
        ProtoRowConverter converter = converter(TestProtos.presence(), options);

        DynamicMessage row =
                converter.convert(
                        Presence.newBuilder()
                                .setPNested(PresenceChild.getDefaultInstance())
                                .build());

        Descriptors.Descriptor rowType = row.getDescriptorForType();
        DynamicMessage nested = (DynamicMessage) get(row, "p_nested");
        assertThat(rowType.findFieldByName("p_nested").isRequired()).isFalse();
        assertThat(nested.getDescriptorForType().findFieldByName("c_implicit").isRequired())
                .isTrue();
        assertThat(get(nested, "c_implicit")).isEqualTo("");
        assertThat(nested.hasField(nested.getDescriptorForType().findFieldByName("c_optional")))
                .isFalse();
    }

    /**
     * proto2 {@code required} is the only way this option derives a {@code REQUIRED}
     * <em>message</em> column — a singular message always has presence, so before the option the
     * proto path could not produce one at all. Both halves are newly reachable: {@code
     * BQTableSchemaToProtoDescriptor} emitting a required message field, and the row converter
     * populating it.
     */
    @Test
    void writesRequiredMessageColumnsDerivedFromProto2Required() throws Exception {
        ProtoSchemaOptions options = ProtoSchemaOptions.builder().deriveRequiredColumns().build();
        ProtoRowConverter converter = converter(TestProtos.proto2Presence(), options);

        DynamicMessage row =
                converter.convert(
                        Proto2Presence.newBuilder()
                                .setQRequired("here")
                                .setQRequiredChild(
                                        Proto2Child.newBuilder().setCOptional("deep").build())
                                .build());

        Descriptors.Descriptor rowType = row.getDescriptorForType();
        assertThat(rowType.findFieldByName("q_required").isRequired()).isTrue();
        assertThat(rowType.findFieldByName("q_required_child").isRequired()).isTrue();
        assertThat(get(row, "q_required")).isEqualTo("here");
        DynamicMessage child = (DynamicMessage) get(row, "q_required_child");
        assertThat(get(child, "c_optional")).isEqualTo("deep");
        assertThat(row.hasField(rowType.findFieldByName("q_optional_child"))).isFalse();
    }

    /**
     * The one failure mode the option introduces, which the option's javadoc and the docs both
     * promise. A proto2 {@code required} field has presence, so the converter skips it when the
     * source omits it — and the target column is {@code REQUIRED}, so {@code build()} refuses.
     * Reaching it needs a source message that violates its own contract, which only {@code
     * buildPartial} can produce; the writers catch this as a row-level failure and route it to the
     * configured {@code FailedRowHandler}.
     */
    @Test
    void aMissingProto2RequiredFieldIsARowLevelFailure() throws Exception {
        Descriptors.Descriptor source = TestProtos.proto2Presence();
        ProtoSchemaOptions options = ProtoSchemaOptions.builder().deriveRequiredColumns().build();
        ProtoRowConverter converter = converter(source, options);
        DynamicMessage partial =
                DynamicMessage.newBuilder(source)
                        .setField(source.findFieldByName("q_optional"), "only")
                        .buildPartial();

        assertThatThrownBy(() -> converter.convert(partial))
                .isInstanceOf(UninitializedMessageException.class)
                .hasMessageContaining("q_required");
    }

    /**
     * A map entry's {@code key} and {@code value} become {@code REQUIRED} under the option, so the
     * entry has to be populated for {@code build()} to succeed. It is — an entry always
     * materializes both — but that is a property of protobuf's synthetic map entries rather than of
     * this code, so it is pinned rather than assumed.
     */
    @Test
    void writesRequiredMapEntryColumns() throws Exception {
        Descriptors.Descriptor source = TestProtos.allTypes();
        ProtoSchemaOptions options = ProtoSchemaOptions.builder().deriveRequiredColumns().build();
        ProtoRowConverter converter = converter(source, options);
        Descriptors.Descriptor entryType = source.findFieldByName("f_map").getMessageType();
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(source);
        builder.addRepeatedField(
                source.findFieldByName("f_map"),
                DynamicMessage.newBuilder(entryType)
                        .setField(entryType.findFieldByName("key"), "k")
                        .setField(entryType.findFieldByName("value"), 5L)
                        .build());

        DynamicMessage row = converter.convert(builder.build());

        DynamicMessage entry = (DynamicMessage) ((List<?>) get(row, "f_map")).get(0);
        Descriptors.Descriptor entryRowType = entry.getDescriptorForType();
        assertThat(entryRowType.findFieldByName("key").isRequired()).isTrue();
        assertThat(entryRowType.findFieldByName("value").isRequired()).isTrue();
        assertThat(get(entry, "key")).isEqualTo("k");
        assertThat(get(entry, "value")).isEqualTo(5L);
    }

    /**
     * The JSON exception, from the value side: were a JSON-mapped presence-less string derived as
     * REQUIRED, the rule that leaves it unset when empty would fail {@code build()} on every record
     * omitting it — a poison record on every record, routed to the FailedRowHandler.
     */
    @Test
    void keepsJsonColumnsWritableWhenRequiredIsDerived() throws Exception {
        Descriptors.Descriptor source = TestProtos.annotated();
        ProtoSchemaOptions options =
                ProtoSchemaOptions.builder()
                        .jsonFieldOptionNumber(TestProtos.JSON_OPTION_NUMBER)
                        .deriveRequiredColumns()
                        .build();
        ProtoRowConverter converter = converter(source, options);

        DynamicMessage row = converter.convert(DynamicMessage.newBuilder(source).build());

        Descriptors.Descriptor rowType = row.getDescriptorForType();
        assertThat(rowType.findFieldByName("a_string").isRequired()).isFalse();
        assertThat(row.hasField(rowType.findFieldByName("a_string"))).isFalse();
        // The plain string beside it is REQUIRED and carries the protobuf default.
        assertThat(rowType.findFieldByName("a_plain").isRequired()).isTrue();
        assertThat(row.getField(rowType.findFieldByName("a_plain"))).isEqualTo("");
    }

    @Test
    void unwrapsWrapperTypesToTheirScalarValues() throws Exception {
        DynamicMessage row =
                wellKnownConverter(ProtoSchemaOptions.defaults())
                        .convert(
                                WellKnown.newBuilder()
                                        .setWInt32(Int32Value.of(-7))
                                        .setWUint32(UInt32Value.of(-1)) // 4294967295 unsigned
                                        .setWInt64(Int64Value.of(9L))
                                        .setWUint64(UInt64Value.of(11L))
                                        .setWFloat(FloatValue.of(0.5f))
                                        .setWDouble(DoubleValue.of(2.5d))
                                        .setWBool(BoolValue.of(true))
                                        .setWString(StringValue.of("s"))
                                        .setWBytes(BytesValue.of(ByteString.copyFromUtf8("b")))
                                        .build());

        assertThat(get(row, "w_int32")).isEqualTo(-7L);
        assertThat(get(row, "w_uint32")).isEqualTo(4294967295L);
        assertThat(get(row, "w_int64")).isEqualTo(9L);
        assertThat(get(row, "w_uint64")).isEqualTo(11L);
        assertThat(get(row, "w_float")).isEqualTo(0.5d);
        assertThat(get(row, "w_double")).isEqualTo(2.5d);
        assertThat(get(row, "w_bool")).isEqualTo(true);
        assertThat(get(row, "w_string")).isEqualTo("s");
        assertThat(get(row, "w_bytes")).isEqualTo(ByteString.copyFromUtf8("b"));
    }

    /**
     * The pair the whole mapping exists for. A wrapper left unset is NULL; a wrapper explicitly set
     * to the type default is that default, not NULL — which is exactly what a bare scalar cannot
     * say.
     */
    @Test
    void distinguishesAnUnsetWrapperFromOneExplicitlySetToZero() throws Exception {
        ProtoRowConverter converter = wellKnownConverter(ProtoSchemaOptions.defaults());

        DynamicMessage unset = converter.convert(WellKnown.newBuilder().build());
        assertThat(has(unset, "w_int64")).isFalse();
        assertThat(has(unset, "w_bool")).isFalse();
        assertThat(has(unset, "w_string")).isFalse();

        DynamicMessage zero =
                converter.convert(
                        WellKnown.newBuilder()
                                .setWInt64(Int64Value.of(0L))
                                .setWBool(BoolValue.of(false))
                                .setWString(StringValue.of(""))
                                .build());
        assertThat(has(zero, "w_int64")).isTrue();
        assertThat(get(zero, "w_int64")).isEqualTo(0L);
        assertThat(get(zero, "w_bool")).isEqualTo(false);
        assertThat(get(zero, "w_string")).isEqualTo("");
    }

    /** A wrapper inherits the range check of the scalar it wraps, from the very same code. */
    @Test
    void rejectsUnrepresentableUint64WrapperValues() throws Exception {
        ProtoRowConverter converter = wellKnownConverter(ProtoSchemaOptions.defaults());
        WellKnown source =
                WellKnown.newBuilder().setWUint64(UInt64Value.of(Long.MIN_VALUE)).build();

        assertThatThrownBy(() -> converter.convert(source))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("w_uint64")
                .hasMessageContaining("9223372036854775808");
    }

    /**
     * The canonical protobuf JSON of a well-known type, not its field structure: a fallback to
     * ordinary message printing would give {@code {"fields":{...}}} here.
     */
    @Test
    void printsStructValueAndListValueAsJsonText() throws Exception {
        DynamicMessage row =
                wellKnownConverter(ProtoSchemaOptions.defaults())
                        .convert(
                                WellKnown.newBuilder()
                                        .setWStruct(
                                                Struct.newBuilder()
                                                        .putFields(
                                                                "k",
                                                                Value.newBuilder()
                                                                        .setNumberValue(1)
                                                                        .build()))
                                        .setWValue(Value.newBuilder().setStringValue("abc").build())
                                        .setWList(
                                                ListValue.newBuilder()
                                                        .addValues(
                                                                Value.newBuilder()
                                                                        .setNumberValue(1)
                                                                        .build())
                                                        .addValues(
                                                                Value.newBuilder()
                                                                        .setBoolValue(true)
                                                                        .build()))
                                        .build());

        assertThat(get(row, "w_struct")).isEqualTo("{\"k\":1.0}");
        assertThat(get(row, "w_value")).isEqualTo("\"abc\"");
        assertThat(get(row, "w_list")).isEqualTo("[1.0,true]");
    }

    /**
     * A Value whose kind is null_value prints the JSON literal {@code null} — distinct from the
     * field being unset, which leaves the column itself NULL.
     */
    @Test
    void printsANullKindValueAsJsonNull() throws Exception {
        DynamicMessage row =
                wellKnownConverter(ProtoSchemaOptions.defaults())
                        .convert(
                                WellKnown.newBuilder()
                                        .setWValue(
                                                Value.newBuilder()
                                                        .setNullValue(NullValue.NULL_VALUE)
                                                        .build())
                                        .build());

        assertThat(has(row, "w_value")).isTrue();
        assertThat(get(row, "w_value")).isEqualTo("null");
    }

    @Test
    void convertsDurationToMicroseconds() throws Exception {
        ProtoRowConverter converter = wellKnownConverter(ProtoSchemaOptions.defaults());

        assertThat(durationMicros(converter, 1L, 500_000_000)).isEqualTo(1_500_000L);
        assertThat(durationMicros(converter, -1L, -500_000_000)).isEqualTo(-1_500_000L);
        // Sub-microsecond digits are truncated toward zero, as they already are for TIMESTAMP.
        assertThat(durationMicros(converter, 0L, 1_500)).isEqualTo(1L);
        assertThat(durationMicros(converter, 0L, -1_500)).isEqualTo(-1L);
    }

    /**
     * A row-level failure like the uint64 case, and rewrapped so the message names the field —
     * protobuf's own names none, which for a record with several Duration columns leaves nothing to
     * act on.
     */
    @Test
    void rejectsOutOfRangeDurations() throws Exception {
        ProtoRowConverter converter = wellKnownConverter(ProtoSchemaOptions.defaults());
        WellKnown source =
                WellKnown.newBuilder()
                        .setWDuration(Duration.newBuilder().setSeconds(Long.MAX_VALUE))
                        .build();

        assertThatThrownBy(() -> converter.convert(source))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("w_duration")
                .hasMessageContaining("out of range");
    }

    /** Paths verbatim, not lowerCamelCased as protobuf's canonical JSON form would render them. */
    @Test
    void joinsFieldMaskPathsWithCommas() throws Exception {
        ProtoRowConverter converter = wellKnownConverter(ProtoSchemaOptions.defaults());

        DynamicMessage row =
                converter.convert(
                        WellKnown.newBuilder()
                                .setWMask(
                                        FieldMask.newBuilder()
                                                .addPaths("user.display_name")
                                                .addPaths("photo"))
                                .build());
        assertThat(get(row, "w_mask")).isEqualTo("user.display_name,photo");

        DynamicMessage empty =
                converter.convert(
                        WellKnown.newBuilder().setWMask(FieldMask.getDefaultInstance()).build());
        assertThat(has(empty, "w_mask")).isTrue();
        assertThat(get(empty, "w_mask")).isEqualTo("");
    }

    @Test
    void convertsWellKnownTypesInsideRepeatedAndMapFields() throws Exception {
        DynamicMessage row =
                wellKnownConverter(ProtoSchemaOptions.defaults())
                        .convert(
                                WellKnown.newBuilder()
                                        .addWRepInt64(Int64Value.of(1L))
                                        .addWRepInt64(Int64Value.of(2L))
                                        .addWRepDuration(Duration.newBuilder().setSeconds(3L))
                                        .addWRepStruct(
                                                Struct.newBuilder()
                                                        .putFields(
                                                                "a",
                                                                Value.newBuilder()
                                                                        .setBoolValue(true)
                                                                        .build()))
                                        .putWMapInt64("k", Int64Value.of(4L))
                                        .putWMapStruct("j", Struct.getDefaultInstance())
                                        .build());

        assertThat(get(row, "w_rep_int64")).isEqualTo(Arrays.asList(1L, 2L));
        assertThat(get(row, "w_rep_duration")).isEqualTo(Arrays.asList(3_000_000L));
        assertThat(get(row, "w_rep_struct")).isEqualTo(Arrays.asList("{\"a\":true}"));
        assertThat(entryValue(row, "w_map_int64")).isEqualTo(4L);
        assertThat(entryValue(row, "w_map_struct")).isEqualTo("{}");
    }

    /**
     * The same matrix reached the way a descriptor arriving as a serialized {@code
     * FileDescriptorSet} does: an independent pool, in which every well-known type is a fresh
     * {@code Descriptor} instance and every value a {@link DynamicMessage}. Two things ride on
     * this. Recognition is keyed on the type's full name, so an identity comparison would see
     * nothing here; and the conversions that construct a well-known type to hand to {@code
     * Durations} or {@code FieldMaskUtil} must rebuild it from sub-fields, since the value is not a
     * generated instance to cast.
     */
    @Test
    void convertsWellKnownTypesFromAnIndependentDescriptorPool() throws Exception {
        Descriptors.Descriptor source =
                rebuild(TestProtos.wellKnown().getFile(), new HashMap<>())
                        .findMessageTypeByName("WellKnown");
        assertThat(source).isNotSameAs(TestProtos.wellKnown());
        assertThat(source.findFieldByName("w_duration").getMessageType())
                .isNotSameAs(Duration.getDescriptor());

        DynamicMessage.Builder builder = DynamicMessage.newBuilder(source);
        set(builder, source, "w_int64", dynamic(source, "w_int64", "value", 5L));
        set(builder, source, "w_mask", dynamic(source, "w_mask", "paths", Arrays.asList("a", "b")));
        set(
                builder,
                source,
                "w_duration",
                dynamic(source, "w_duration", "seconds", 2L, "nanos", 250_000_000));
        set(
                builder,
                source,
                "w_struct",
                dynamic(
                        source,
                        "w_struct",
                        "fields",
                        Arrays.asList(structEntry(source, "k", "v"))));

        DynamicMessage row =
                new ProtoRowConverter(
                                source,
                                BQTableSchemaToProtoDescriptor
                                        .convertBQTableSchemaToProtoDescriptor(
                                                ProtoToTableSchemaConverter.convert(
                                                        source, ProtoSchemaOptions.defaults())),
                                ProtoSchemaOptions.defaults())
                        .convert(builder.build());

        assertThat(get(row, "w_int64")).isEqualTo(5L);
        assertThat(get(row, "w_mask")).isEqualTo("a,b");
        assertThat(get(row, "w_duration")).isEqualTo(2_250_000L);
        // The canonical Struct rendering, not the {"fields":{...}} an ordinary message would give.
        assertThat(get(row, "w_struct")).isEqualTo("{\"k\":\"v\"}");
    }

    /** Any keeps its two fields, since the payload cannot be expanded without its descriptor. */
    @Test
    void writesAnyAsAStruct() throws Exception {
        DynamicMessage row =
                wellKnownConverter(ProtoSchemaOptions.defaults())
                        .convert(
                                WellKnown.newBuilder()
                                        .setWAny(
                                                Any.newBuilder()
                                                        .setTypeUrl("type.googleapis.com/x.Y")
                                                        .setValue(ByteString.copyFromUtf8("p")))
                                        .build());

        DynamicMessage any = (DynamicMessage) get(row, "w_any");
        assertThat(get(any, "type_url")).isEqualTo("type.googleapis.com/x.Y");
        assertThat(get(any, "value")).isEqualTo(ByteString.copyFromUtf8("p"));
    }

    /** The value-side half of JSON-first precedence, matching the schema side. */
    @Test
    void printsAJsonMappedWrapperRatherThanUnwrappingIt() throws Exception {
        DynamicMessage row =
                wellKnownConverter(ProtoSchemaOptions.builder().jsonFieldPath("w_int64").build())
                        .convert(WellKnown.newBuilder().setWInt64(Int64Value.of(5L)).build());

        // Canonical protobuf JSON renders an int64 as a quoted string.
        assertThat(get(row, "w_int64")).isEqualTo("\"5\"");
    }

    private static ProtoRowConverter wellKnownConverter(ProtoSchemaOptions options)
            throws Exception {
        return converter(TestProtos.wellKnown(), options);
    }

    private static long durationMicros(ProtoRowConverter converter, long seconds, int nanos)
            throws Exception {
        return (Long)
                get(
                        converter.convert(
                                WellKnown.newBuilder()
                                        .setWDuration(
                                                Duration.newBuilder()
                                                        .setSeconds(seconds)
                                                        .setNanos(nanos))
                                        .build()),
                        "w_duration");
    }

    /**
     * Rebuilds a file descriptor and everything it depends on, producing a pool that shares no
     * {@code Descriptor} instance with the generated one — what a descriptor deserialized from a
     * {@code FileDescriptorSet} looks like. Memoised, because the dependency graph is a DAG and
     * {@code buildFrom} rejects two copies of one file.
     */
    private static Descriptors.FileDescriptor rebuild(
            Descriptors.FileDescriptor file, Map<String, Descriptors.FileDescriptor> built)
            throws Exception {
        Descriptors.FileDescriptor already = built.get(file.getFullName());
        if (already != null) {
            return already;
        }
        Descriptors.FileDescriptor[] dependencies =
                new Descriptors.FileDescriptor[file.getDependencies().size()];
        for (int i = 0; i < dependencies.length; i++) {
            dependencies[i] = rebuild(file.getDependencies().get(i), built);
        }
        Descriptors.FileDescriptor rebuilt =
                Descriptors.FileDescriptor.buildFrom(file.toProto(), dependencies);
        built.put(file.getFullName(), rebuilt);
        return rebuilt;
    }

    /** Builds a one-field DynamicMessage for the well-known type carried by {@code field}. */
    private static DynamicMessage dynamic(
            Descriptors.Descriptor source, String field, String subField, Object value) {
        Descriptors.Descriptor type = source.findFieldByName(field).getMessageType();
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(type);
        if (value instanceof List) {
            ((List<?>) value)
                    .forEach(v -> builder.addRepeatedField(type.findFieldByName(subField), v));
        } else {
            builder.setField(type.findFieldByName(subField), value);
        }
        return builder.build();
    }

    /** The two-field form, for the {@code seconds}/{@code nanos} pair. */
    private static DynamicMessage dynamic(
            Descriptors.Descriptor source,
            String field,
            String firstName,
            Object first,
            String secondName,
            Object second) {
        Descriptors.Descriptor type = source.findFieldByName(field).getMessageType();
        return DynamicMessage.newBuilder(type)
                .setField(type.findFieldByName(firstName), first)
                .setField(type.findFieldByName(secondName), second)
                .build();
    }

    /** A {@code Struct.fields} map entry holding a string value, in the given pool. */
    private static DynamicMessage structEntry(
            Descriptors.Descriptor source, String key, String value) {
        Descriptors.Descriptor structType = source.findFieldByName("w_struct").getMessageType();
        Descriptors.Descriptor entryType = structType.findFieldByName("fields").getMessageType();
        Descriptors.Descriptor valueType = entryType.findFieldByName("value").getMessageType();
        return DynamicMessage.newBuilder(entryType)
                .setField(entryType.findFieldByName("key"), key)
                .setField(
                        entryType.findFieldByName("value"),
                        DynamicMessage.newBuilder(valueType)
                                .setField(valueType.findFieldByName("string_value"), value)
                                .build())
                .build();
    }

    /** The {@code value} column of the single entry of a map column. */
    private static Object entryValue(DynamicMessage row, String field) {
        DynamicMessage entry =
                (DynamicMessage)
                        row.getRepeatedField(row.getDescriptorForType().findFieldByName(field), 0);
        return get(entry, "value");
    }

    private static boolean has(DynamicMessage message, String field) {
        return message.hasField(message.getDescriptorForType().findFieldByName(field));
    }

    private static ProtoRowConverter converter(
            Descriptors.Descriptor source, ProtoSchemaOptions options) throws Exception {
        Descriptors.Descriptor target =
                BQTableSchemaToProtoDescriptor.convertBQTableSchemaToProtoDescriptor(
                        ProtoToTableSchemaConverter.convert(source, options));
        return new ProtoRowConverter(source, target, options);
    }

    private static void set(
            DynamicMessage.Builder builder,
            Descriptors.Descriptor descriptor,
            String field,
            Object value) {
        builder.setField(descriptor.findFieldByName(field), value);
    }

    private static Object get(DynamicMessage message, String field) {
        return message.getField(message.getDescriptorForType().findFieldByName(field));
    }
}
