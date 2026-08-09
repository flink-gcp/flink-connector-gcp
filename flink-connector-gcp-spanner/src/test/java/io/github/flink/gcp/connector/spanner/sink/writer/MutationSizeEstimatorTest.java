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

package io.github.flink.gcp.connector.spanner.sink.writer;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.KeyRange;
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Value;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link MutationSizeEstimator}. */
class MutationSizeEstimatorTest {

    @Test
    void sizesScalarsByTheirType() {
        Mutation mutation =
                Mutation.newInsertBuilder("T")
                        .set("Flag")
                        .to(true)
                        .set("Count")
                        .to(1L)
                        .set("Ratio")
                        .to(1.5d)
                        .build();

        assertThat(MutationSizeEstimator.sizeOf(mutation)).isEqualTo(1 + 8 + 8);
    }

    @Test
    void sizesStringsAndBytesByTheirLength() {
        Mutation mutation =
                Mutation.newInsertBuilder("T")
                        .set("Note")
                        .to("x".repeat(300))
                        .set("Blob")
                        .to(ByteArray.copyFrom(new byte[64]))
                        .build();

        assertThat(MutationSizeEstimator.sizeOf(mutation)).isEqualTo(300 + 64);
    }

    @Test
    void sizesNumericByItsDecimalRendering() {
        Mutation mutation =
                Mutation.newInsertBuilder("T").set("Amount").to(new BigDecimal("1.25")).build();

        assertThat(MutationSizeEstimator.sizeOf(mutation)).isEqualTo(4);
    }

    @Test
    void sizesTemporalTypes() {
        Mutation mutation =
                Mutation.newInsertBuilder("T")
                        .set("At")
                        .to(Timestamp.ofTimeSecondsAndNanos(1, 0))
                        .set("On")
                        .to(Date.fromYearMonthDay(2026, 8, 9))
                        .build();

        assertThat(MutationSizeEstimator.sizeOf(mutation)).isEqualTo(16 + 12);
    }

    @Test
    void aNullValueCostsNothing() {
        Mutation mutation = Mutation.newInsertBuilder("T").set("Note").to((String) null).build();

        assertThat(MutationSizeEstimator.sizeOf(mutation)).isZero();
    }

    @Test
    void sumsArrayElements() {
        Mutation mutation =
                Mutation.newInsertBuilder("T")
                        .set("Counts")
                        .to(Value.int64Array(Arrays.asList(1L, 2L, 3L)))
                        .set("Notes")
                        .to(Value.stringArray(Arrays.asList("ab", "cde")))
                        .build();

        assertThat(MutationSizeEstimator.sizeOf(mutation)).isEqualTo(3 * 8 + 2 + 3);
    }

    @Test
    void aNullArrayElementCostsNothing() {
        Mutation mutation =
                Mutation.newInsertBuilder("T")
                        .set("Notes")
                        .to(Value.stringArray(Arrays.asList("ab", null)))
                        .build();

        assertThat(MutationSizeEstimator.sizeOf(mutation)).isEqualTo(2);
    }

    @Test
    void sizesADeleteByItsKeyParts() {
        assertThat(MutationSizeEstimator.sizeOf(Mutation.delete("T", Key.of(1L, "a"))))
                .isEqualTo(2 * 16);
    }

    @Test
    void sizesARangeDeleteByBothItsKeys() {
        Mutation mutation =
                Mutation.delete("T", KeySet.range(KeyRange.closedClosed(Key.of(1L), Key.of(2L))));

        assertThat(MutationSizeEstimator.sizeOf(mutation)).isEqualTo(16 + 16 + 16);
    }

    @Test
    void aValueWithNoTypeAtAllIsCountedAtTheUnknownFallback() {
        // Value.untyped(...) is public API — it lets the backend infer the type — and it carries a
        // null Type. Reading the type code without a guard throws, which would take down a job over
        // one record the serializer was entitled to produce.
        Mutation mutation =
                Mutation.newInsertBuilder("T")
                        .set("Anything")
                        .to(
                                Value.untyped(
                                        com.google.protobuf.Value.newBuilder()
                                                .setStringValue("x")
                                                .build()))
                        .build();

        assertThat(MutationSizeEstimator.sizeOf(mutation))
                .isEqualTo(MutationSizeEstimator.UNKNOWN_VALUE_BYTES);
    }

    @Test
    void anUntypedNullStillCostsNothing() {
        Mutation mutation =
                Mutation.newInsertBuilder("T")
                        .set("Anything")
                        .to(
                                Value.untyped(
                                        com.google.protobuf.Value.newBuilder()
                                                .setNullValue(
                                                        com.google.protobuf.NullValue.NULL_VALUE)
                                                .build()))
                        .build();

        assertThat(MutationSizeEstimator.sizeOf(mutation)).isZero();
    }

    @Test
    void aDeleteOfEverythingCostsNothingItCanSee() {
        // KeySet.all() names no key, so there is nothing to add up. The row cap is what bounds a
        // batch of these, which is why the estimate reading zero is not a hole.
        assertThat(MutationSizeEstimator.sizeOf(Mutation.delete("T", KeySet.all()))).isZero();
    }
}
