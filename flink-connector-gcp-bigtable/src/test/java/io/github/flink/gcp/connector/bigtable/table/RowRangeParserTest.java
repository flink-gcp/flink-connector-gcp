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

package io.github.flink.gcp.connector.bigtable.table;

import org.apache.flink.table.api.ValidationException;

import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link RowRangeParser}. */
class RowRangeParserTest {

    @Test
    void parsesDisjointAndOneSidedUtf8Ranges() {
        assertThat(RowRangeParser.parse(RowKeyEncoding.UTF8, "[,m);[q,z);[zz,)"))
                .containsExactly(
                        ByteStringRange.unbounded().endOpen("m"),
                        ByteStringRange.unbounded().startClosed("q").endOpen("z"),
                        ByteStringRange.unbounded().startClosed("zz"));
    }

    @Test
    void unescapesEveryGrammarCharacterInsideEndpoints() {
        assertThat(
                        RowRangeParser.parse(
                                RowKeyEncoding.UTF8,
                                "[a\\,b,c\\;d);[x\\\\y,z\\)q);[\\[,\\]);[a\\(b,c)"))
                .containsExactly(
                        ByteStringRange.unbounded().startClosed("a,b").endOpen("c;d"),
                        ByteStringRange.unbounded().startClosed("x\\y").endOpen("z)q"),
                        ByteStringRange.unbounded().startClosed("[").endOpen("]"),
                        ByteStringRange.unbounded().startClosed("a(b").endOpen("c"));
    }

    @Test
    void decodesBase64BeforeComparingAndBuildingRanges() {
        assertThat(RowRangeParser.parse(RowKeyEncoding.BASE64, "[AAE=,AP8=);[/wA=,/wE=)"))
                .containsExactly(
                        ByteStringRange.unbounded()
                                .startClosed(ByteString.copyFrom(new byte[] {0x00, 0x01}))
                                .endOpen(ByteString.copyFrom(new byte[] {0x00, (byte) 0xff})),
                        ByteStringRange.unbounded()
                                .startClosed(ByteString.copyFrom(new byte[] {(byte) 0xff, 0x00}))
                                .endOpen(ByteString.copyFrom(new byte[] {(byte) 0xff, 0x01})));
    }

    private static Stream<Arguments> invalidRanges() {
        return Stream.of(
                Arguments.of("", 1, "is empty"),
                Arguments.of(";[a,b)", 1, "is empty"),
                Arguments.of("[a,b);", 2, "is empty"),
                Arguments.of("[a,b);[c,d);[z,a)", 3, "greater than its end"),
                Arguments.of("[,)", 1, "both endpoints unbounded"),
                Arguments.of("[a,a)", 1, "equal decoded endpoints"),
                Arguments.of("[z,a)", 1, "greater than its end"),
                Arguments.of("(a,b)", 1, "must start with '['"),
                Arguments.of("[a,b]", 1, "end with ')'"),
                Arguments.of("[a)", 1, "one unescaped comma"),
                Arguments.of("[a,b,c)", 1, "more than one unescaped comma"),
                Arguments.of("[a(b,c)", 1, "unescaped grammar character '('"),
                Arguments.of("[a\\q,b)", 1, "unsupported escape"),
                Arguments.of("[a\\,b)", 1, "one unescaped comma"),
                // Two distinct backslash failures, asserted apart: an escaped terminator (see the
                // comment on that throw in RowRangeParser) and a value whose last character is a
                // bare backslash, which splitEntries refuses before an entry is parsed at all.
                Arguments.of("[a,b\\)", 1, "escapes the ')' that must end it"),
                Arguments.of("[a,b);[c,d)\\", 2, "ends with an incomplete backslash escape"));
    }

    @ParameterizedTest
    @MethodSource("invalidRanges")
    void rejectsInvalidRangesWithTheOptionAndOneBasedEntry(
            String configured, int entry, String detail) {
        assertThatThrownBy(() -> RowRangeParser.parse(RowKeyEncoding.UTF8, configured))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("'scan.row-ranges' entry " + entry)
                .hasMessageContaining(detail);
    }

    @Test
    void reportsBase64FailuresAgainstTheirEntry() {
        assertThatThrownBy(
                        () -> RowRangeParser.parse(RowKeyEncoding.BASE64, "[YQ==,Yg==);[YQ,Yg==)"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("'scan.row-ranges' entry 2")
                .hasStackTraceContaining("canonical padded RFC 4648 standard Base64");
    }
}
