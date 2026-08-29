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

package io.github.flink.gcp.connector.spanner.source.batch.reader;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import com.google.cloud.spanner.Value;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link StructSizeEstimator}. */
class StructSizeEstimatorTest {

    @Test
    void variableContentUsesDecodedByteLengthsAndNullAddsNothing() {
        assertEstimate(7, Struct.newBuilder().set("value").to("Aé😀").build());
        assertEstimate(
                3,
                Struct.newBuilder()
                        .set("value")
                        .to(ByteArray.copyFrom(new byte[] {1, 2, 3}))
                        .build());
        assertEstimate(
                2,
                Struct.newBuilder()
                        .set("value")
                        .to(Value.protoMessage(ByteArray.copyFrom(new byte[] {4, 5}), "x.Event"))
                        .build());
        assertEstimate(2, Struct.newBuilder().set("value").to(Value.json("{}")).build());
        assertEstimate(8, Struct.newBuilder().set("value").to(Value.pgJsonb("{\"é\":1}")).build());
        assertEstimate(0, Struct.newBuilder().set("value").to(Value.string(null)).build());
    }

    @Test
    void fixedWidthContentUsesStableLogicalWidths() {
        assertEstimate(1, Struct.newBuilder().set("value").to(true).build());
        assertEstimate(8, Struct.newBuilder().set("value").to(1L).build());
        assertEstimate(4, Struct.newBuilder().set("value").to(Value.float32(1.25F)).build());
        assertEstimate(8, Struct.newBuilder().set("value").to(2.5D).build());
        assertEstimate(16, Struct.newBuilder().set("value").to(BigDecimal.ONE).build());
        assertEstimate(
                12, Struct.newBuilder().set("value").to(Timestamp.ofTimeMicroseconds(1)).build());
        assertEstimate(
                12,
                Struct.newBuilder().set("value").to(Date.fromYearMonthDay(2026, 8, 29)).build());
        assertEstimate(
                16,
                Struct.newBuilder()
                        .set("value")
                        .to(Value.uuid(UUID.fromString("f81d4fae-7dec-11d0-a765-00a0c91e6bf6")))
                        .build());
    }

    @Test
    void arraysAndNestedStructsAreMeasuredRecursively() {
        Struct child = Struct.newBuilder().set("name").to("é").set("active").to(true).build();
        assertEstimate(
                16,
                Struct.newBuilder()
                        .set("value")
                        .to(Value.int64Array(Arrays.asList(1L, null, 2L)))
                        .build());
        assertEstimate(
                5,
                Struct.newBuilder()
                        .set("value")
                        .to(Value.stringArray(Arrays.asList("é", null, "abc")))
                        .build());
        assertEstimate(
                5,
                Struct.newBuilder()
                        .set("value")
                        .to(Value.jsonArray(Arrays.asList("{}", null, "[1]")))
                        .build());
        assertEstimate(
                2,
                Struct.newBuilder()
                        .set("value")
                        .to(Value.pgJsonbArray(Arrays.asList("é", null)))
                        .build());
        assertEstimate(
                3,
                Struct.newBuilder()
                        .set("value")
                        .to(
                                Value.bytesArray(
                                        Arrays.asList(
                                                ByteArray.copyFrom(new byte[] {1, 2}),
                                                null,
                                                ByteArray.copyFrom(new byte[] {3}))))
                        .build());
        assertEstimate(3, Struct.newBuilder().set("value").to(Value.struct(child)).build());
        assertEstimate(
                3,
                Struct.newBuilder()
                        .set("value")
                        .to(Value.structArray(child.getType(), Arrays.asList(child, null)))
                        .build());
    }

    @Test
    void anUnknownTypeHasAStableFallback() {
        assertThat(StructSizeEstimator.UNKNOWN_VALUE_BYTES).isEqualTo(64);
        assertThat(StructSizeEstimator.estimateScalar(null, 0, Type.Code.UNRECOGNIZED))
                .isEqualTo(64);
        assertThat(StructSizeEstimator.estimateArray(null, 0, Type.Code.UNRECOGNIZED))
                .isEqualTo(64);
    }

    @Test
    void additionSaturatesInsteadOfOverflowing() {
        assertThat(StructSizeEstimator.saturatedAdd(Long.MAX_VALUE - 2, 3))
                .isEqualTo(Long.MAX_VALUE);
    }

    private static void assertEstimate(long expected, Struct row) {
        assertThat(StructSizeEstimator.estimate(row)).isEqualTo(expected);
    }
}
