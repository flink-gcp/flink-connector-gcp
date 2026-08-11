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

package io.github.flink.gcp.connector.bigtable.source.changestream;

import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import com.google.cloud.bigtable.data.v2.models.ChangeStreamContinuationToken;
import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ChangeStreamPartitionSplitSerializerTest {

    private final ChangeStreamPartitionSplitSerializer serializer =
            new ChangeStreamPartitionSplitSerializer();

    @Test
    void roundTripsMultipleParentTokensAndBinaryRanges() throws IOException {
        ByteStringRange left = ByteStringRange.unbounded().endOpen("m");
        ByteStringRange right = ByteStringRange.unbounded().startClosed("m");
        ChangeStreamPartitionSplit split =
                new ChangeStreamPartitionSplit(
                        "change-stream-7",
                        ByteStringRange.unbounded(),
                        Arrays.asList(
                                TestChangeStreamTokens.token(left, "left-token"),
                                TestChangeStreamTokens.token(right, "right-token")),
                        Instant.parse("2026-08-11T12:34:56.123456789Z"));

        ChangeStreamPartitionSplit restored =
                serializer.deserialize(serializer.getVersion(), serializer.serialize(split));

        assertThat(restored).isEqualTo(split);
        assertThat(restored.getContinuationTokens())
                .extracting(ChangeStreamContinuationToken::getToken)
                .containsExactly("left-token", "right-token");
    }

    @Test
    void rejectsAVersionItDidNotWrite() throws IOException {
        ChangeStreamPartitionSplit split =
                new ChangeStreamPartitionSplit(
                        "change-stream-0",
                        ByteStringRange.unbounded(),
                        java.util.Collections.emptyList(),
                        Instant.EPOCH);

        assertThatThrownBy(
                        () ->
                                serializer.deserialize(
                                        serializer.getVersion() + 1, serializer.serialize(split)))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Unsupported Bigtable change-stream split");
    }

    @Test
    void rejectsAByteLengthLargerThanTheSerializedState() throws IOException {
        DataOutputSerializer out = new DataOutputSerializer(4);
        out.writeInt(64);

        assertThatThrownBy(
                        () ->
                                ChangeStreamPartitionSplitSerializer.readBytes(
                                        new DataInputDeserializer(out.getCopyOfBuffer()),
                                        "test field"))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("exceeds the remaining 0 byte(s)");
    }
}
