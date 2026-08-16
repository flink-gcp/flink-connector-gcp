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

package io.github.flink.gcp.connector.bigtable.source.serializer;

import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.bigtable.source.changestream.ChangeStreamMutation;
import io.github.flink.gcp.connector.bigtable.source.changestream.ChangeStreamMutationSerializer;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Arrays;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ChangeStreamMutationSerializerSnapshotTest {

    private static final byte[] VERSION_ONE_GOLDEN =
            Base64.getDecoder()
                    .decode(
                            "AAAAAgD/AgAAAAFj//////////8AAAB7////+QAAAAF0AAAAAAAAAAIAAAHIAAAABgEAAAABcwAAAAIBAv/////////1AAAAAwMEBQIAAAABZAAAAAEGAQAAAAAAAAAMAgAAAAAAAAANAgAAAAF1AAAAAAAAAwAAAAFmBAAAAAFhAQAAAAEHAgAAAAAAAAAOAwAAAAAAAAAPBQAAAAFtAQAAAAEIAgAAAAAAAAAQAQAAAAIJCg==");

    @Test
    void roundTripsTheConnectorOwnedFormatSnapshot() throws Exception {
        ChangeStreamMutationSerializer.Snapshot written =
                new ChangeStreamMutationSerializer.Snapshot();
        DataOutputSerializer output = new DataOutputSerializer(32);
        written.writeSnapshot(output);
        ChangeStreamMutationSerializer.Snapshot restored =
                new ChangeStreamMutationSerializer.Snapshot();

        restored.readSnapshot(
                written.getCurrentVersion(),
                new DataInputDeserializer(output.getCopyOfBuffer()),
                getClass().getClassLoader());

        assertThat(
                        restored.resolveSchemaCompatibility(
                                new ChangeStreamMutationSerializer.Snapshot()))
                .matches(compatibility -> compatibility.isCompatibleAsIs());
        assertThat(restored.restoreSerializer()).isInstanceOf(ChangeStreamMutationSerializer.class);
    }

    @Test
    void rejectsAnUnknownSnapshotFormat() {
        ChangeStreamMutationSerializer.Snapshot snapshot =
                new ChangeStreamMutationSerializer.Snapshot();

        assertThatThrownBy(
                        () ->
                                snapshot.readSnapshot(
                                        2,
                                        new DataInputDeserializer(new byte[0]),
                                        getClass().getClassLoader()))
                .isInstanceOf(java.io.IOException.class)
                .hasMessageContaining("snapshot version 2");
    }

    @Test
    void versionOnePayloadRemainsReadableAndStable() throws Exception {
        ChangeStreamMutation expected =
                new ChangeStreamMutation(
                        ByteString.copyFrom(new byte[] {0, (byte) 0xff}),
                        ChangeStreamMutation.MutationType.GARBAGE_COLLECTION,
                        "c",
                        Instant.ofEpochSecond(-1, 123),
                        -7,
                        "t",
                        Instant.ofEpochSecond(2, 456),
                        Arrays.asList(
                                new ChangeStreamMutation.SetCellEntry(
                                        "s",
                                        ByteString.copyFrom(new byte[] {1, 2}),
                                        -11L,
                                        ByteString.copyFrom(new byte[] {3, 4, 5})),
                                new ChangeStreamMutation.DeleteCellsEntry(
                                        "d",
                                        ByteString.copyFrom(new byte[] {6}),
                                        new ChangeStreamMutation.TimestampRange(
                                                ChangeStreamMutation.TimestampBound.open(12L),
                                                ChangeStreamMutation.TimestampBound.closed(13L))),
                                new ChangeStreamMutation.DeleteCellsEntry(
                                        "u",
                                        ByteString.EMPTY,
                                        new ChangeStreamMutation.TimestampRange(
                                                ChangeStreamMutation.TimestampBound.unbounded(),
                                                ChangeStreamMutation.TimestampBound.unbounded())),
                                new ChangeStreamMutation.DeleteFamilyEntry("f"),
                                new ChangeStreamMutation.AddToCellEntry(
                                        "a",
                                        new ChangeStreamMutation.RawValue(
                                                ByteString.copyFrom(new byte[] {7})),
                                        new ChangeStreamMutation.RawTimestamp(14L),
                                        new ChangeStreamMutation.Int64Value(15L)),
                                new ChangeStreamMutation.MergeToCellEntry(
                                        "m",
                                        new ChangeStreamMutation.RawValue(
                                                ByteString.copyFrom(new byte[] {8})),
                                        new ChangeStreamMutation.RawTimestamp(16L),
                                        new ChangeStreamMutation.RawValue(
                                                ByteString.copyFrom(new byte[] {9, 10})))));
        ChangeStreamMutationSerializer serializer = new ChangeStreamMutationSerializer();

        ChangeStreamMutation restored =
                serializer.deserialize(new DataInputDeserializer(VERSION_ONE_GOLDEN));
        DataOutputSerializer serialized = new DataOutputSerializer(VERSION_ONE_GOLDEN.length);
        serializer.serialize(expected, serialized);

        assertThat(restored.getRowKey()).isEqualTo(expected.getRowKey());
        assertThat(restored.getType()).isEqualTo(expected.getType());
        assertThat(restored.getSourceClusterId()).isEqualTo(expected.getSourceClusterId());
        assertThat(restored.getCommitTime()).isEqualTo(expected.getCommitTime());
        assertThat(restored.getTieBreaker()).isEqualTo(expected.getTieBreaker());
        assertThat(restored.getToken()).isEqualTo(expected.getToken());
        assertThat(restored.getEstimatedLowWatermarkTime())
                .isEqualTo(expected.getEstimatedLowWatermarkTime());
        assertThat(restored.getEntries()).containsExactlyElementsOf(expected.getEntries());
        assertThat(serialized.getCopyOfBuffer()).isEqualTo(VERSION_ONE_GOLDEN);
    }
}
