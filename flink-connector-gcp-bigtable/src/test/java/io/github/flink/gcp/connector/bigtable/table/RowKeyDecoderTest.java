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

package io.github.flink.gcp.connector.bigtable.table;

import org.apache.flink.table.api.ValidationException;

import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.charset.StandardCharsets;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RowKeyDecoderTest {

    private static Stream<Arguments> base64GoldenVectors() {
        return Stream.of(
                Arguments.of("YQ==", new byte[] {'a'}),
                Arguments.of("AAE=", new byte[] {0x00, 0x01}),
                Arguments.of("/4A=", new byte[] {(byte) 0xff, (byte) 0x80}));
    }

    @Test
    void utf8PreservesTheOriginalEncoding() {
        String value = "row-\u00e9";

        assertThat(
                        RowKeyDecoder.decode(
                                BigtableConnectorOptions.SCAN_ROW_RANGE_START_CLOSED,
                                RowKeyEncoding.UTF8,
                                value))
                .isEqualTo(ByteString.copyFrom(value, StandardCharsets.UTF_8));
    }

    @ParameterizedTest
    @MethodSource("base64GoldenVectors")
    void base64DecodesToTheExactBytes(String configured, byte[] expected) {
        assertThat(
                        RowKeyDecoder.decode(
                                BigtableConnectorOptions.SCAN_ROW_RANGE_START_CLOSED,
                                RowKeyEncoding.BASE64,
                                configured))
                .isEqualTo(ByteString.copyFrom(expected));
    }

    @ParameterizedTest
    @ValueSource(strings = {"_w==", "Y Q==", "YQ", "YR==", "YQ===", "!Q=="})
    void base64RejectsEveryNonCanonicalForm(String configured) {
        assertThatThrownBy(
                        () ->
                                RowKeyDecoder.decode(
                                        BigtableConnectorOptions.SCAN_ROW_PREFIX,
                                        RowKeyEncoding.BASE64,
                                        configured))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("'scan.row-prefix'")
                .hasMessageContaining("canonical padded RFC 4648 standard Base64");
    }

    @ParameterizedTest
    @MethodSource("encodings")
    void everyEncodingRejectsAnEmptyRowKey(RowKeyEncoding encoding) {
        assertThatThrownBy(
                        () ->
                                RowKeyDecoder.decode(
                                        BigtableConnectorOptions.SCAN_ROW_RANGE_END_OPEN,
                                        encoding,
                                        ""))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("'scan.row-range.end-open'")
                .hasMessageContaining("decodes to an empty row key")
                .hasMessageContaining("Remove the empty value")
                .hasMessageContaining("leave the option unset if no row-key bound is intended");
    }

    private static Stream<RowKeyEncoding> encodings() {
        return Stream.of(RowKeyEncoding.values());
    }
}
