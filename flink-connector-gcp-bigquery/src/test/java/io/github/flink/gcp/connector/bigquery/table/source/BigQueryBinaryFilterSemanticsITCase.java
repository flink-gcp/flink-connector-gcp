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

package io.github.flink.gcp.connector.bigquery.table.source;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.runtime.functions.SqlFunctionUtils;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;

/** Pins fixed-length source values and Flink's generated binary predicates without GCP. */
@Timeout(120)
class BigQueryBinaryFilterSemanticsITCase {
    private static final String[] HEX = {
        "",
        "00",
        "0000",
        "000000",
        "00000000",
        "0000000000",
        "007f",
        "0080",
        "7f",
        "80",
        "ff",
        "27225c",
        "c328",
        "ffff"
    };
    private static final String[] OPERATORS = {"=", "<>", "<", "<=", ">", ">="};

    @Test
    void declaredLengthDoesNotNormalizeSourceBytesOrDirectComparisons() throws Exception {
        List<GenericRecord> records = fixture();
        for (int length : new int[] {1, 2, 4}) {
            Map<String, Set<Long>> expected = new LinkedHashMap<>();
            for (String hex : HEX) {
                for (int op = 0; op < OPERATORS.length; op++) {
                    for (boolean reverse : new boolean[] {false, true}) {
                        String literal = "X'" + hex + "'";
                        String predicate =
                                reverse
                                        ? literal + " " + OPERATORS[op] + " binary_value"
                                        : "binary_value " + OPERATORS[op] + " " + literal;
                        Set<Long> ids = new TreeSet<>();
                        for (int id = 0; id < HEX.length; id++) {
                            int comparison =
                                    SqlFunctionUtils.byteArrayCompare(bytes(HEX[id]), bytes(hex));
                            if (matches(reverse ? -comparison : comparison, op)) {
                                ids.add((long) id);
                            }
                        }
                        expected.put(predicate, ids);
                    }
                }
            }
            expected.put("binary_value IS NULL", Collections.singleton((long) HEX.length));
            Set<Long> nonNull = new TreeSet<>();
            for (long id = 0; id < HEX.length; id++) {
                nonNull.add(id);
            }
            expected.put("binary_value IS NOT NULL", nonNull);
            int castLength = length + 1;
            String paddedZero = "00".repeat(castLength);
            Set<Long> castMatches = new TreeSet<>();
            for (int id = 0; id < HEX.length; id++) {
                if (Arrays.equals(Arrays.copyOf(bytes(HEX[id]), castLength), bytes(paddedZero))) {
                    castMatches.add((long) id);
                }
            }
            expected.put(
                    "CAST(binary_value AS BINARY(" + castLength + ")) = X'" + paddedZero + "'",
                    castMatches);
            expected.put(
                    "binary_value = CAST(X'00' AS BINARY(" + length + "))",
                    Collections.singleton((long) Arrays.asList(HEX).indexOf("00".repeat(length))));
            expected.put(
                    "binary_value = CAST(X'0000000000' AS BINARY(" + length + "))",
                    Collections.singleton((long) Arrays.asList(HEX).indexOf("00".repeat(length))));
            assertThat(
                            BinaryFilterOracle.evaluate(
                                            Collections.singletonList(records),
                                            DataTypes.BINARY(length),
                                            new ArrayList<>(expected.keySet()))
                                    .get(0))
                    .as("BINARY(%s) SQL results", length)
                    .isEqualTo(expected);
        }
    }

    private static boolean matches(int compared, int operator) {
        switch (operator) {
            case 0:
                return compared == 0;
            case 1:
                return compared != 0;
            case 2:
                return compared < 0;
            case 3:
                return compared <= 0;
            case 4:
                return compared > 0;
            case 5:
                return compared >= 0;
            default:
                throw new AssertionError(operator);
        }
    }

    private static List<GenericRecord> fixture() {
        Schema schema =
                new Schema.Parser()
                        .parse(
                                "{\"type\":\"record\",\"name\":\"BinaryRows\",\"fields\":["
                                        + "{\"name\":\"id\",\"type\":\"long\"},"
                                        + "{\"name\":\"binary_value\",\"type\":[\"null\",\"bytes\"]}]}");
        List<GenericRecord> records = new ArrayList<>();
        for (int id = 0; id <= HEX.length; id++) {
            GenericRecord row = new GenericData.Record(schema);
            row.put("id", (long) id);
            row.put("binary_value", id == HEX.length ? null : ByteBuffer.wrap(bytes(HEX[id])));
            records.add(row);
        }
        return records;
    }

    private static byte[] bytes(String hex) {
        byte[] result = new byte[hex.length() / 2];
        for (int i = 0; i < result.length; i++) {
            result[i] = (byte) Integer.parseInt(hex.substring(i * 2, i * 2 + 2), 16);
        }
        return result;
    }
}
