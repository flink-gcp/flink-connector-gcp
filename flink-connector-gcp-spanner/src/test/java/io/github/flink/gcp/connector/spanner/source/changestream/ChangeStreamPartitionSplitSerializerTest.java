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

package io.github.flink.gcp.connector.spanner.source.changestream;

import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ChangeStreamPartitionSplitSerializerTest {

    private static final Instant START = Instant.parse("2026-08-12T00:00:00.123456789Z");

    @Test
    void roundTripsEveryLifecycleStateAndOptionalField() throws Exception {
        ChangeStreamPartitionSplitSerializer serializer =
                new ChangeStreamPartitionSplitSerializer();

        for (PartitionLifecycleState state : PartitionLifecycleState.values()) {
            ChangeStreamPartitionSplit split =
                    new ChangeStreamPartitionSplit(
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

        ChangeStreamPartitionSplit initial = ChangeStreamPartitionSplit.initial(START, null, 2_000);
        assertThat(serializer.deserialize(serializer.getVersion(), serializer.serialize(initial)))
                .isEqualTo(initial)
                .extracting(ChangeStreamPartitionSplit::getPartitionToken)
                .isNull();
    }

    @Test
    void roundTripsAFinishedSplitWithItsChildReports() throws Exception {
        ChangeStreamPartitionSplitSerializer serializer =
                new ChangeStreamPartitionSplitSerializer();
        ChangeStreamPartitionSplit running =
                new ChangeStreamPartitionSplit(
                        "token",
                        Arrays.asList("parent-a"),
                        START,
                        null,
                        2_000,
                        START.plusNanos(7),
                        PartitionLifecycleState.RUNNING,
                        START.plusNanos(5));
        ChangeStreamPartitionSplit finished =
                running.withFinishReport(
                        Arrays.asList(
                                new ChildPartitionsEvent(
                                        running.splitId(),
                                        START.plusSeconds(1),
                                        Arrays.asList(
                                                new ChildPartitionsEvent.ChildPartition(
                                                        "child-1",
                                                        Arrays.asList(running.splitId())),
                                                new ChildPartitionsEvent.ChildPartition(
                                                        "merged",
                                                        Arrays.asList(
                                                                running.splitId(), "other")))),
                                new ChildPartitionsEvent(
                                        running.splitId(),
                                        START.plusSeconds(1),
                                        Arrays.asList(
                                                new ChildPartitionsEvent.ChildPartition(
                                                        "child-2",
                                                        Arrays.asList(running.splitId()))))));
        ChangeStreamPartitionSplit withoutChildren =
                running.withFinishReport(java.util.Collections.emptyList());

        assertThat(serializer.deserialize(serializer.getVersion(), serializer.serialize(finished)))
                .isEqualTo(finished);
        assertThat(
                        serializer.deserialize(
                                serializer.getVersion(), serializer.serialize(withoutChildren)))
                .isEqualTo(withoutChildren)
                .satisfies(split -> assertThat(split.awaitsFinishAcknowledgement()).isTrue());
    }

    @Test
    void readsAVersionOneSplitAsOneWithoutAFinishReport() throws Exception {
        ChangeStreamPartitionSplit running =
                new ChangeStreamPartitionSplit(
                        "token",
                        Arrays.asList("parent-a"),
                        START,
                        null,
                        2_000,
                        START.plusNanos(7),
                        PartitionLifecycleState.RUNNING,
                        START.plusNanos(5));
        // The version-1 layout written field by field, so a change to writeSplit cannot follow it.
        DataOutputSerializer versionOne = new DataOutputSerializer(64);
        versionOne.writeBoolean(true);
        versionOne.writeUTF("token");
        versionOne.writeInt(1);
        versionOne.writeUTF("parent-a");
        versionOne.writeLong(START.getEpochSecond());
        versionOne.writeInt(START.getNano());
        versionOne.writeBoolean(false);
        versionOne.writeLong(2_000);
        versionOne.writeLong(START.plusNanos(7).getEpochSecond());
        versionOne.writeInt(START.plusNanos(7).getNano());
        versionOne.writeByte(2);
        versionOne.writeLong(START.plusNanos(5).getEpochSecond());
        versionOne.writeInt(START.plusNanos(5).getNano());

        ChangeStreamPartitionSplit restored =
                new ChangeStreamPartitionSplitSerializer()
                        .deserialize(1, versionOne.getCopyOfBuffer());

        assertThat(restored).isEqualTo(running);
        assertThat(restored.awaitsFinishAcknowledgement()).isFalse();
    }

    @Test
    void rejectsAFinishReportOnASplitThatIsNotFinished() throws Exception {
        ChangeStreamPartitionSplitSerializer serializer =
                new ChangeStreamPartitionSplitSerializer();
        ChangeStreamPartitionSplit finished =
                new ChangeStreamPartitionSplit(
                                "token",
                                Arrays.asList("parent-a"),
                                START,
                                null,
                                2_000,
                                START,
                                PartitionLifecycleState.RUNNING,
                                START)
                        .withFinishReport(java.util.Collections.emptyList());
        byte[] bytes = serializer.serialize(finished);
        // The lifecycle tag precedes the twelve-byte watermark, the report flag and its count.
        bytes[bytes.length - 18] = 2;

        assertThatThrownBy(() -> serializer.deserialize(serializer.getVersion(), bytes))
                .isInstanceOf(java.io.IOException.class)
                .hasMessageContaining("a RUNNING split carries a finish report");
    }

    @Test
    void rejectsUnknownVersionsAndCorruptLifecycleTags() throws Exception {
        ChangeStreamPartitionSplitSerializer serializer =
                new ChangeStreamPartitionSplitSerializer();
        byte[] bytes = serializer.serialize(ChangeStreamPartitionSplit.initial(START, null, 2_000));

        assertThatThrownBy(() -> serializer.deserialize(99, bytes))
                .isInstanceOf(java.io.IOException.class)
                .hasMessageContaining("version 99");

        // The lifecycle tag precedes the twelve-byte watermark and the finish-report flag.
        bytes[bytes.length - 14] = 99;
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
                                ChangeStreamPartitionSplitSerializer.readInstant(
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
                                ChangeStreamPartitionSplitSerializer.readSplit(
                                        new DataInputDeserializer(corrupt.getCopyOfBuffer())))
                .isInstanceOf(java.io.IOException.class)
                .hasMessageContaining("negative parent partition count -1");
    }
}
