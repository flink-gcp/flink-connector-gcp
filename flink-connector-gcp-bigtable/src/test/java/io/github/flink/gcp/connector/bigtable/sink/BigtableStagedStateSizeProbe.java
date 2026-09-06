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

package io.github.flink.gcp.connector.bigtable.sink;

import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.util.FileUtils;

import com.google.bigtable.v2.CheckAndMutateRowRequest;
import com.google.bigtable.v2.Mutation;
import com.google.protobuf.ByteString;

import java.lang.management.ManagementFactory;
import java.lang.ref.Reference;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

/** Standalone sizing experiment; run each input in a fresh JVM, outside the unit-test suite. */
final class BigtableStagedStateSizeProbe {
    private BigtableStagedStateSizeProbe() {}

    public static void main(String[] args) throws Exception {
        int payloadBytes = Integer.parseInt(args[0]);
        int entries = Integer.parseInt(args[1]);
        if ((payloadBytes != 1024 && payloadBytes != 65_536)
                || (entries != 100 && entries != 1000)) {
            throw new IllegalArgumentException("Use 1024/65536 bytes and 100/1000 entries");
        }
        var checkpointDirectory = Files.createTempDirectory("bigtable-staged-sizing-");
        var test = new BigtableStagedCommitLifecycleTest();
        var serializer = new StagedMutationTestSink("sizing", 1, 1).getCommittableSerializer();
        List<CheckAndMutateRowRequest> staged = new ArrayList<>();
        List<CheckAndMutateRowRequest> restored = new ArrayList<>();
        try (var writer = test.writer(1, 0);
                var committer = test.committer(1, 0, true)) {
            committer.setCheckpointStorage(
                    new FileSystemCheckpointStorage(checkpointDirectory.toUri().toString()));
            writer.open();
            committer.open();
            long baseline = heap();
            long wireBytes = 0;
            long firstSnapshotBytes = 0;
            long secondSnapshotBytes = 0;
            List<Object> snapshots = new ArrayList<>();
            for (int checkpoint = 1; checkpoint <= 2; checkpoint++) {
                for (int i = 0; i < entries; i++) {
                    byte[] payload = new byte[payloadBytes];
                    payload[0] = (byte) i;
                    Mutation cell =
                            Mutation.newBuilder()
                                    .setSetCell(
                                            Mutation.SetCell.newBuilder()
                                                    .setFamilyName("data")
                                                    .setColumnQualifier(
                                                            ByteString.copyFromUtf8("value"))
                                                    .setTimestampMicros(1000)
                                                    .setValue(ByteString.copyFrom(payload)))
                                    .build();
                    writer.processElement(
                            new StagedMutationTestSink.Input(
                                    BigtableStagedCommitLifecycleTest.TABLE,
                                    ByteString.copyFromUtf8(
                                            String.format("%02d-%08d", checkpoint, i)),
                                    List.of(cell)),
                            0);
                }
                writer.getOperator().prepareSnapshotPreBarrier(checkpoint);
                for (var message : writer.extractOutputValues()) {
                    if (message
                            instanceof
                            org.apache.flink.streaming.api.connector.sink2.CommittableWithLineage) {
                        @SuppressWarnings("unchecked")
                        var lineage =
                                (org.apache.flink.streaming.api.connector.sink2
                                                        .CommittableWithLineage<
                                                CheckAndMutateRowRequest>)
                                        message;
                        staged.add(lineage.getCommittable());
                        wireBytes += lineage.getCommittable().getSerializedSize();
                    }
                }
                BigtableStagedCommitLifecycleTest.forward(writer, committer);
                var snapshot = committer.snapshot(checkpoint, 0);
                snapshots.add(snapshot);
                if (checkpoint == 1) {
                    firstSnapshotBytes = snapshot.getStateSize();
                } else {
                    secondSnapshotBytes = snapshot.getStateSize();
                }
            }
            long retained = heap();
            for (var request : staged) {
                restored.add(serializer.deserialize(1, serializer.serialize(request)));
            }
            long copied = heap();
            if (restored.size() != 2 * entries || secondSnapshotBytes <= firstSnapshotBytes) {
                throw new AssertionError("Sizing probe did not retain both intervals");
            }
            System.out.printf(
                    "SIZING,%d,%d,%d,%d,%d,%d,%d,%d%n",
                    payloadBytes,
                    entries,
                    wireBytes,
                    firstSnapshotBytes,
                    secondSnapshotBytes,
                    baseline,
                    retained,
                    copied);
            Reference.reachabilityFence(snapshots);
            Reference.reachabilityFence(staged);
            Reference.reachabilityFence(restored);
        } finally {
            test.cleanup();
            FileUtils.deleteDirectory(checkpointDirectory.toFile());
        }
    }

    private static long heap() {
        System.gc();
        return ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed();
    }
}
