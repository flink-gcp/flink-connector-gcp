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
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;

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
