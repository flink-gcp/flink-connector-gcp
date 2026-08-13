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

package io.github.flink.gcp.connector.cloudtasks.table.form;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.SerializationFormatFactory;
import org.apache.flink.table.runtime.connector.sink.SinkRuntimeProviderContext;
import org.apache.flink.table.types.DataType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link FormUrlEncodedFormatFactory}. */
class FormUrlEncodedFormatFactoryTest {

    private static StringData str(String value) {
        return StringData.fromString(value);
    }

    private static SerializationSchema<RowData> encoder(DataType physicalType) {
        EncodingFormat<SerializationSchema<RowData>> format =
                new FormUrlEncodedFormatFactory().createEncodingFormat(null, new Configuration());
        return format.createRuntimeEncoder(new SinkRuntimeProviderContext(false), physicalType);
    }

    @Test
    void discoversTheFormatThroughFlinksFactorySpi() {
        SerializationFormatFactory factory =
                FactoryUtil.discoverFactory(
                        Thread.currentThread().getContextClassLoader(),
                        SerializationFormatFactory.class,
                        FormUrlEncodedFormatFactory.IDENTIFIER);

        assertThat(factory).isInstanceOf(FormUrlEncodedFormatFactory.class);
        assertThat(factory.requiredOptions()).isEmpty();
        assertThat(factory.optionalOptions()).isEmpty();
    }

    @Test
    void encodesFieldsAndRepeatedValuesInPhysicalSchemaOrder() throws IOException {
        DataType physicalType =
                DataTypes.ROW(
                        DataTypes.FIELD("表示 name +&=", DataTypes.STRING()),
                        DataTypes.FIELD("tags", DataTypes.ARRAY(DataTypes.STRING())),
                        DataTypes.FIELD("empty", DataTypes.STRING()),
                        DataTypes.FIELD("ignored", DataTypes.STRING()),
                        DataTypes.FIELD("absent_tags", DataTypes.ARRAY(DataTypes.STRING())));
        GenericRowData row =
                GenericRowData.of(
                        str("東京 +&="),
                        new GenericArrayData(
                                new Object[] {
                                    str("one two"), str("a+b"), str("x&y"), str("left=right")
                                }),
                        str(""),
                        null,
                        null);

        byte[] encoded = encoder(physicalType).serialize(row);

        assertThat(new String(encoded, StandardCharsets.UTF_8))
                .isEqualTo(
                        "%E8%A1%A8%E7%A4%BA+name+%2B%26%3D="
                                + "%E6%9D%B1%E4%BA%AC+%2B%26%3D"
                                + "&tags=one+two&tags=a%2Bb&tags=x%26y"
                                + "&tags=left%3Dright&empty=");
    }

    @Test
    void emptyArraysAndNullFieldsProduceAnEmptyForm() throws IOException {
        DataType physicalType =
                DataTypes.ROW(
                        DataTypes.FIELD("value", DataTypes.STRING()),
                        DataTypes.FIELD("values", DataTypes.ARRAY(DataTypes.STRING())));

        byte[] encoded =
                encoder(physicalType)
                        .serialize(GenericRowData.of(null, new GenericArrayData(new Object[0])));

        assertThat(encoded).isEmpty();
    }

    @Test
    void encodesTheDocumentedFormRequestExamples() throws IOException {
        DataType basicType =
                DataTypes.ROW(
                        DataTypes.FIELD("order_id", DataTypes.STRING()),
                        DataTypes.FIELD("note", DataTypes.STRING()),
                        DataTypes.FIELD("tags", DataTypes.ARRAY(DataTypes.STRING())),
                        DataTypes.FIELD("categories", DataTypes.STRING()));
        assertThat(
                        new String(
                                encoder(basicType)
                                        .serialize(
                                                GenericRowData.of(
                                                        str("42"),
                                                        str("東京 + pickup"),
                                                        new GenericArrayData(
                                                                new Object[] {
                                                                    str("urgent"), str("gift")
                                                                }),
                                                        str("books,sale"))),
                                StandardCharsets.UTF_8))
                .isEqualTo(
                        "order_id=42&note=%E6%9D%B1%E4%BA%AC+%2B+pickup"
                                + "&tags=urgent&tags=gift&categories=books%2Csale");

        DataType nestedNamesType =
                DataTypes.ROW(
                        DataTypes.FIELD("items[]", DataTypes.ARRAY(DataTypes.STRING())),
                        DataTypes.FIELD("customer.name", DataTypes.STRING()),
                        DataTypes.FIELD("customer[postalCode]", DataTypes.STRING()),
                        DataTypes.FIELD("attributes[priority]", DataTypes.STRING()));
        assertThat(
                        new String(
                                encoder(nestedNamesType)
                                        .serialize(
                                                GenericRowData.of(
                                                        new GenericArrayData(
                                                                new Object[] {
                                                                    str("book"), str("pen")
                                                                }),
                                                        str("Alice"),
                                                        str("100-0001"),
                                                        str("high"))),
                                StandardCharsets.UTF_8))
                .isEqualTo(
                        "items%5B%5D=book&items%5B%5D=pen&customer.name=Alice"
                                + "&customer%5BpostalCode%5D=100-0001"
                                + "&attributes%5Bpriority%5D=high");

        DataType jsonParameterType = DataTypes.ROW(DataTypes.FIELD("payload", DataTypes.STRING()));
        assertThat(
                        new String(
                                encoder(jsonParameterType)
                                        .serialize(
                                                GenericRowData.of(
                                                        str(
                                                                "{\"items\":[\"book\",\"pen\"],"
                                                                        + "\"name\":\"Alice\","
                                                                        + "\"postalCode\":\"100-0001\"}"))),
                                StandardCharsets.UTF_8))
                .isEqualTo(
                        "payload=%7B%22items%22%3A%5B%22book%22%2C%22pen%22%5D%2C"
                                + "%22name%22%3A%22Alice%22%2C%22postalCode%22%3A"
                                + "%22100-0001%22%7D");
    }

    @Test
    void rejectsANullArrayElementWithoutChangingTheForm() {
        DataType physicalType =
                DataTypes.ROW(DataTypes.FIELD("tags", DataTypes.ARRAY(DataTypes.STRING())));

        assertThatThrownBy(
                        () ->
                                encoder(physicalType)
                                        .serialize(
                                                GenericRowData.of(
                                                        new GenericArrayData(
                                                                new Object[] {
                                                                    str("first"), null, str("last")
                                                                }))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("tags")
                .hasMessageContaining("index 1")
                .hasMessageContaining("cannot represent");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("unsupportedTypes")
    void rejectsPhysicalTypesThatNeedAnExplicitStringCast(String summary, DataType type) {
        DataType physicalType = DataTypes.ROW(DataTypes.FIELD("unsupported", type));

        assertThatThrownBy(() -> encoder(physicalType))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("form-urlencoded")
                .hasMessageContaining("unsupported")
                .hasMessageContaining(summary)
                .hasMessageContaining("Cast the value to STRING explicitly in SQL");
    }

    private static Stream<Arguments> unsupportedTypes() {
        return Stream.of(
                Arguments.of("INT", DataTypes.INT()),
                Arguments.of("CHAR", DataTypes.CHAR(8)),
                Arguments.of("ARRAY<INT>", DataTypes.ARRAY(DataTypes.INT())),
                Arguments.of(
                        "ROW<`nested` STRING>",
                        DataTypes.ROW(DataTypes.FIELD("nested", DataTypes.STRING()))),
                Arguments.of(
                        "MAP<STRING, STRING>",
                        DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING())),
                Arguments.of(
                        "ARRAY<ARRAY<STRING>>",
                        DataTypes.ARRAY(DataTypes.ARRAY(DataTypes.STRING()))));
    }
}
