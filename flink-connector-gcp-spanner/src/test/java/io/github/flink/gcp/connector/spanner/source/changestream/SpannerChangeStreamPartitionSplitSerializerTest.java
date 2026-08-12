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

package io.github.flink.gcp.connector.spanner.source.changestream;

import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SpannerChangeStreamPartitionSplitSerializerTest {

    private static final Instant START = Instant.parse("2026-08-12T00:00:00.123456789Z");

    @Test
    void roundTripsEveryLifecycleStateAndOptionalField() throws Exception {
        SpannerChangeStreamPartitionSplitSerializer serializer =
                new SpannerChangeStreamPartitionSplitSerializer();

        for (PartitionLifecycleState state : PartitionLifecycleState.values()) {
            SpannerChangeStreamPartitionSplit split =
                    new SpannerChangeStreamPartitionSplit(
                            "token",
                            Arrays.asList("parent-a", "parent-b"),
                            START,
                            START.plusSeconds(30),
                            2_000,
                            START.plusNanos(7),
                            state,
                            START.plusNanos(5));

            assertThat(serializer.deserialize(serializer.getVersion(), serializer.serialize(split)))
                    .isEqualTo(split);
        }

        SpannerChangeStreamPartitionSplit initial =
                SpannerChangeStreamPartitionSplit.initial(START, null, 2_000);
        assertThat(serializer.deserialize(serializer.getVersion(), serializer.serialize(initial)))
                .isEqualTo(initial)
                .extracting(SpannerChangeStreamPartitionSplit::getPartitionToken)
                .isNull();
    }

    @Test
    void rejectsUnknownVersionsAndCorruptLifecycleTags() throws Exception {
        SpannerChangeStreamPartitionSplitSerializer serializer =
                new SpannerChangeStreamPartitionSplitSerializer();
        byte[] bytes =
                serializer.serialize(SpannerChangeStreamPartitionSplit.initial(START, null, 2_000));

        assertThatThrownBy(() -> serializer.deserialize(99, bytes))
                .isInstanceOf(java.io.IOException.class)
                .hasMessageContaining("version 99");

        // The lifecycle tag is immediately before the final twelve-byte Instant.
        bytes[bytes.length - 13] = 99;
        assertThatThrownBy(() -> serializer.deserialize(serializer.getVersion(), bytes))
                .isInstanceOf(java.io.IOException.class)
                .hasMessageContaining("lifecycle tag 99");
    }

    @Test
    void rejectsOutOfRangeInstantNanoseconds() throws Exception {
        DataOutputSerializer corrupt = new DataOutputSerializer(12);
        corrupt.writeLong(0);
        corrupt.writeInt(1_000_000_000);

        assertThatThrownBy(
                        () ->
                                SpannerChangeStreamPartitionSplitSerializer.readInstant(
                                        new DataInputDeserializer(corrupt.getCopyOfBuffer())))
                .isInstanceOf(java.io.IOException.class)
                .hasMessageContaining("invalid instant 0:1000000000");
    }

    @Test
    void rejectsNegativeParentCounts() throws Exception {
        DataOutputSerializer corrupt = new DataOutputSerializer(5);
        corrupt.writeBoolean(false);
        corrupt.writeInt(-1);

        assertThatThrownBy(
                        () ->
                                SpannerChangeStreamPartitionSplitSerializer.readSplit(
                                        new DataInputDeserializer(corrupt.getCopyOfBuffer())))
                .isInstanceOf(java.io.IOException.class)
                .hasMessageContaining("negative parent partition count -1");
    }
}
