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
import io.github.flink.gcp.connector.bigtable.source.changestream.BigtableChangeStreamMutation;
import io.github.flink.gcp.connector.bigtable.source.changestream.BigtableChangeStreamMutationSerializer;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Arrays;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BigtableChangeStreamMutationSerializerSnapshotTest {

    private static final byte[] VERSION_ONE_GOLDEN =
            Base64.getDecoder()
                    .decode(
                            "AAAAAgD/AgAAAAFj//////////8AAAB7////+QAAAAF0AAAAAAAAAAIAAAHIAAAABgEAAAABcwAAAAIBAv/////////1AAAAAwMEBQIAAAABZAAAAAEGAQAAAAAAAAAMAgAAAAAAAAANAgAAAAF1AAAAAAAAAwAAAAFmBAAAAAFhAQAAAAEHAgAAAAAAAAAOAwAAAAAAAAAPBQAAAAFtAQAAAAEIAgAAAAAAAAAQAQAAAAIJCg==");

    @Test
    void roundTripsTheConnectorOwnedFormatSnapshot() throws Exception {
        BigtableChangeStreamMutationSerializer.Snapshot written =
                new BigtableChangeStreamMutationSerializer.Snapshot();
        DataOutputSerializer output = new DataOutputSerializer(32);
        written.writeSnapshot(output);
        BigtableChangeStreamMutationSerializer.Snapshot restored =
                new BigtableChangeStreamMutationSerializer.Snapshot();

        restored.readSnapshot(
                written.getCurrentVersion(),
                new DataInputDeserializer(output.getCopyOfBuffer()),
                getClass().getClassLoader());

        assertThat(
                        restored.resolveSchemaCompatibility(
                                new BigtableChangeStreamMutationSerializer.Snapshot()))
                .matches(compatibility -> compatibility.isCompatibleAsIs());
        assertThat(restored.restoreSerializer())
                .isInstanceOf(BigtableChangeStreamMutationSerializer.class);
    }

    @Test
    void rejectsAnUnknownSnapshotFormat() {
        BigtableChangeStreamMutationSerializer.Snapshot snapshot =
                new BigtableChangeStreamMutationSerializer.Snapshot();

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
        BigtableChangeStreamMutation expected =
                new BigtableChangeStreamMutation(
                        ByteString.copyFrom(new byte[] {0, (byte) 0xff}),
                        BigtableChangeStreamMutation.MutationType.GARBAGE_COLLECTION,
                        "c",
                        Instant.ofEpochSecond(-1, 123),
                        -7,
                        "t",
                        Instant.ofEpochSecond(2, 456),
                        Arrays.asList(
                                new BigtableChangeStreamMutation.SetCellEntry(
                                        "s",
                                        ByteString.copyFrom(new byte[] {1, 2}),
                                        -11L,
                                        ByteString.copyFrom(new byte[] {3, 4, 5})),
                                new BigtableChangeStreamMutation.DeleteCellsEntry(
                                        "d",
                                        ByteString.copyFrom(new byte[] {6}),
                                        new BigtableChangeStreamMutation.TimestampRange(
                                                BigtableChangeStreamMutation.TimestampBound.open(
                                                        12L),
                                                BigtableChangeStreamMutation.TimestampBound.closed(
                                                        13L))),
                                new BigtableChangeStreamMutation.DeleteCellsEntry(
                                        "u",
                                        ByteString.EMPTY,
                                        new BigtableChangeStreamMutation.TimestampRange(
                                                BigtableChangeStreamMutation.TimestampBound
                                                        .unbounded(),
                                                BigtableChangeStreamMutation.TimestampBound
                                                        .unbounded())),
                                new BigtableChangeStreamMutation.DeleteFamilyEntry("f"),
                                new BigtableChangeStreamMutation.AddToCellEntry(
                                        "a",
                                        new BigtableChangeStreamMutation.RawValue(
                                                ByteString.copyFrom(new byte[] {7})),
                                        new BigtableChangeStreamMutation.RawTimestamp(14L),
                                        new BigtableChangeStreamMutation.Int64Value(15L)),
                                new BigtableChangeStreamMutation.MergeToCellEntry(
                                        "m",
                                        new BigtableChangeStreamMutation.RawValue(
                                                ByteString.copyFrom(new byte[] {8})),
                                        new BigtableChangeStreamMutation.RawTimestamp(16L),
                                        new BigtableChangeStreamMutation.RawValue(
                                                ByteString.copyFrom(new byte[] {9, 10})))));
        BigtableChangeStreamMutationSerializer serializer =
                new BigtableChangeStreamMutationSerializer();

        BigtableChangeStreamMutation restored =
                serializer.deserialize(new DataInputDeserializer(VERSION_ONE_GOLDEN));
        DataOutputSerializer serialized = new DataOutputSerializer(VERSION_ONE_GOLDEN.length);
        serializer.serialize(expected, serialized);

        assertThat(restored).isEqualTo(expected);
        assertThat(serialized.getCopyOfBuffer()).isEqualTo(VERSION_ONE_GOLDEN);
    }
}
