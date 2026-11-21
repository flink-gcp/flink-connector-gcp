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

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Connector-owned checkpoint format for a Spanner Change Streams partition split.
 *
 * <p>Version 2 appends the finish report a reader keeps for a split whose query ended until the
 * coordinator acknowledges it. A version-1 split never had one, so it reads as a split without a
 * report. The coordinator's ledger never holds a report, so {@link #writeSplit} keeps the shared
 * per-split layout and the enumerator state format is unchanged.
 */
@Internal
public final class ChangeStreamPartitionSplitSerializer
        implements SimpleVersionedSerializer<ChangeStreamPartitionSplit> {

    private static final int VERSION = 2;
    private static final int VERSION_WITHOUT_FINISH_REPORT = 1;
    private static final int INITIAL_BUFFER_SIZE = 512;

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(ChangeStreamPartitionSplit split) throws IOException {
        DataOutputSerializer out = new DataOutputSerializer(INITIAL_BUFFER_SIZE);
        writeSplit(out, split);
        writeFinishReport(out, split.getFinishReport());
        return out.getCopyOfBuffer();
    }

    @Override
    public ChangeStreamPartitionSplit deserialize(int version, byte[] serialized)
            throws IOException {
        if (version != VERSION && version != VERSION_WITHOUT_FINISH_REPORT) {
            throw new IOException(
                    "Unsupported Spanner change-stream split serialization version "
                            + version
                            + "; this connector writes version "
                            + VERSION
                            + ".");
        }
        DataInputDeserializer in = new DataInputDeserializer(serialized);
        ChangeStreamPartitionSplit split = readSplit(in);
        if (version == VERSION_WITHOUT_FINISH_REPORT) {
            return split;
        }
        if (!in.readBoolean()) {
            return split;
        }
        if (split.getLifecycleState() != PartitionLifecycleState.FINISHED) {
            throw new IOException(
                    "Corrupt Spanner change-stream split state: a "
                            + split.getLifecycleState()
                            + " split carries a finish report.");
        }
        try {
            return split.withFinishReport(readFinishReport(in, split.splitId()));
        } catch (RuntimeException e) {
            throw new IOException("Corrupt Spanner change-stream split state.", e);
        }
    }

    private static void writeFinishReport(
            DataOutputSerializer out, List<ChildPartitionsEvent> report) throws IOException {
        out.writeBoolean(report != null);
        if (report == null) {
            return;
        }
        out.writeInt(report.size());
        for (ChildPartitionsEvent childReport : report) {
            writeInstant(out, childReport.getStartTimestamp());
            out.writeInt(childReport.getChildren().size());
            for (ChildPartitionsEvent.ChildPartition child : childReport.getChildren()) {
                out.writeUTF(child.getToken());
                out.writeInt(child.getParentPartitionIds().size());
                for (String parent : child.getParentPartitionIds()) {
                    out.writeUTF(parent);
                }
            }
        }
    }

    private static List<ChildPartitionsEvent> readFinishReport(
            DataInputDeserializer in, String splitId) throws IOException {
        int reportCount = readCount(in, "child report");
        List<ChildPartitionsEvent> report = new ArrayList<>(Math.min(reportCount, 1024));
        for (int i = 0; i < reportCount; i++) {
            Instant startTimestamp = readInstant(in);
            int childCount = readCount(in, "child partition");
            List<ChildPartitionsEvent.ChildPartition> children =
                    new ArrayList<>(Math.min(childCount, 1024));
            for (int j = 0; j < childCount; j++) {
                String token = in.readUTF();
                int parentCount = readCount(in, "child parent");
                List<String> parents = new ArrayList<>(Math.min(parentCount, 1024));
                for (int k = 0; k < parentCount; k++) {
                    parents.add(in.readUTF());
                }
                children.add(new ChildPartitionsEvent.ChildPartition(token, parents));
            }
            report.add(new ChildPartitionsEvent(splitId, startTimestamp, children));
        }
        return report;
    }

    static void writeSplit(DataOutputSerializer out, ChangeStreamPartitionSplit split)
            throws IOException {
        writeNullableString(out, split.getPartitionToken());
        out.writeInt(split.getParentPartitionIds().size());
        for (String parent : split.getParentPartitionIds()) {
            out.writeUTF(parent);
        }
        writeInstant(out, split.getStartTimestamp());
        writeNullableInstant(out, split.getEndTimestamp());
        out.writeLong(split.getHeartbeatMillis());
        writeInstant(out, split.getCurrentPosition());
        out.writeByte(stateTag(split.getLifecycleState()));
        writeInstant(out, split.getWatermark());
    }

    static ChangeStreamPartitionSplit readSplit(DataInputDeserializer in) throws IOException {
        String token = readNullableString(in);
        int parentCount = readCount(in, "parent partition");
        List<String> parents = new ArrayList<>(Math.min(parentCount, 1024));
        for (int i = 0; i < parentCount; i++) {
            parents.add(in.readUTF());
        }
        Instant startTimestamp = readInstant(in);
        Instant endTimestamp = readNullableInstant(in);
        long heartbeatMillis = in.readLong();
        Instant currentPosition = readInstant(in);
        PartitionLifecycleState state = readState(in.readByte());
        Instant watermark = readInstant(in);
        try {
            return new ChangeStreamPartitionSplit(
                    token,
                    parents,
                    startTimestamp,
                    endTimestamp,
                    heartbeatMillis,
                    currentPosition,
                    state,
                    watermark);
        } catch (RuntimeException e) {
            throw new IOException("Corrupt Spanner change-stream split state.", e);
        }
    }

    static void writeInstant(DataOutputSerializer out, Instant instant) throws IOException {
        out.writeLong(instant.getEpochSecond());
        out.writeInt(instant.getNano());
    }

    static Instant readInstant(DataInputDeserializer in) throws IOException {
        long seconds = in.readLong();
        int nanos = in.readInt();
        if (nanos < 0 || nanos > 999_999_999) {
            throw new IOException(
                    "Corrupt Spanner change-stream state: invalid instant "
                            + seconds
                            + ":"
                            + nanos);
        }
        try {
            return Instant.ofEpochSecond(seconds, nanos);
        } catch (RuntimeException e) {
            throw new IOException(
                    "Corrupt Spanner change-stream state: invalid instant " + seconds + ":" + nanos,
                    e);
        }
    }

    static void writeNullableInstant(DataOutputSerializer out, Instant instant) throws IOException {
        out.writeBoolean(instant != null);
        if (instant != null) {
            writeInstant(out, instant);
        }
    }

    static Instant readNullableInstant(DataInputDeserializer in) throws IOException {
        return in.readBoolean() ? readInstant(in) : null;
    }

    private static void writeNullableString(DataOutputSerializer out, String value)
            throws IOException {
        out.writeBoolean(value != null);
        if (value != null) {
            out.writeUTF(value);
        }
    }

    private static String readNullableString(DataInputDeserializer in) throws IOException {
        return in.readBoolean() ? in.readUTF() : null;
    }

    static int readCount(DataInputDeserializer in, String noun) throws IOException {
        int count = in.readInt();
        if (count < 0) {
            throw new IOException(
                    "Corrupt Spanner change-stream state: negative " + noun + " count " + count);
        }
        if (count > in.available()) {
            throw new IOException(
                    "Corrupt Spanner change-stream state: "
                            + noun
                            + " count "
                            + count
                            + " exceeds the remaining "
                            + in.available()
                            + " byte(s)");
        }
        return count;
    }

    private static int stateTag(PartitionLifecycleState state) {
        switch (state) {
            case CREATED:
                return 0;
            case SCHEDULED:
                return 1;
            case RUNNING:
                return 2;
            case FINISHED:
                return 3;
            default:
                throw new IllegalStateException("Unhandled partition lifecycle state " + state);
        }
    }

    private static PartitionLifecycleState readState(int tag) throws IOException {
        switch (tag) {
            case 0:
                return PartitionLifecycleState.CREATED;
            case 1:
                return PartitionLifecycleState.SCHEDULED;
            case 2:
                return PartitionLifecycleState.RUNNING;
            case 3:
                return PartitionLifecycleState.FINISHED;
            default:
                throw new IOException(
                        "Corrupt Spanner change-stream state: unknown lifecycle tag " + tag);
        }
    }
}
