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

package io.github.flink.gcp.connector.bigtable.source.changestream;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.Preconditions;

import com.google.protobuf.ByteString;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/** Serializes the connector-owned mutation model without reflective collection access. */
@Internal
public final class BigtableChangeStreamMutationSerializer
        extends TypeSerializer<BigtableChangeStreamMutation> {

    private static final long serialVersionUID = 1L;
    private static final ThreadLocal<byte[]> COPY_BUFFER =
            ThreadLocal.withInitial(() -> new byte[4 * 1024]);

    private static final int SET_CELL = 1;
    private static final int DELETE_CELLS = 2;
    private static final int DELETE_FAMILY = 3;
    private static final int ADD_TO_CELL = 4;
    private static final int MERGE_TO_CELL = 5;

    private static final int RAW_VALUE = 1;
    private static final int RAW_TIMESTAMP = 2;
    private static final int INT64 = 3;

    private static final EntryWriter ENTRY_WRITER = new EntryWriter();
    private static final ValueWriter VALUE_WRITER = new ValueWriter();

    @Override
    public boolean isImmutableType() {
        return true;
    }

    @Override
    public TypeSerializer<BigtableChangeStreamMutation> duplicate() {
        return this;
    }

    @Override
    public BigtableChangeStreamMutation createInstance() {
        return null;
    }

    @Override
    public BigtableChangeStreamMutation copy(BigtableChangeStreamMutation from) {
        return from;
    }

    @Override
    public BigtableChangeStreamMutation copy(
            BigtableChangeStreamMutation from, BigtableChangeStreamMutation reuse) {
        return from;
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    public void serialize(BigtableChangeStreamMutation record, DataOutputView target)
            throws IOException {
        Preconditions.checkNotNull(record, "record must not be null");
        writeBytes(record.getRowKey(), target);
        writeMutationType(record.getType(), target);
        writeString(record.getSourceClusterId(), target);
        writeInstant(record.getCommitTime(), target);
        target.writeInt(record.getTieBreaker());
        writeString(record.getToken(), target);
        writeInstant(record.getEstimatedLowWatermarkTime(), target);
        target.writeInt(record.getEntries().size());
        for (BigtableChangeStreamMutation.Entry entry : record.getEntries()) {
            writeEntry(entry, target);
        }
    }

    private static void writeEntry(BigtableChangeStreamMutation.Entry entry, DataOutputView target)
            throws IOException {
        // Through the dispatcher rather than calling the package-private accept directly, which
        // this package could: the dispatcher's call sites are then the complete list of places
        // that branch on an entry subtype, which is the list whoever adds one needs.
        ChangeStreamMutationDispatcher.dispatchEntry(entry, ENTRY_WRITER, target);
    }

    /**
     * Writes each entry subtype behind its wire tag.
     *
     * <p>The tags this writes are the ones {@link
     * BigtableChangeStreamMutationSerializer#readEntry(DataInputView)} and {@link
     * BigtableChangeStreamMutationSerializer#copyEntry(DataInputView, DataOutputView, byte[])}
     * decode, and those two stay {@code switch}es over a byte rather than becoming visitors: they
     * dispatch on what arrived on the wire, which is not a Java type yet.
     *
     * <p>Stateless and shared, because the serializer's {@code duplicate()} returns {@code this}
     * and task threads therefore share one instance — the same reason {@code COPY_BUFFER} is a
     * {@link ThreadLocal}.
     */
    private static final class EntryWriter
            implements ChangeStreamMutationDispatcher.EntryVisitor<Void, DataOutputView> {

        @Override
        public Void visit(BigtableChangeStreamMutation.SetCellEntry entry, DataOutputView target)
                throws IOException {
            target.writeByte(SET_CELL);
            writeString(entry.getFamilyName(), target);
            writeBytes(entry.getQualifier(), target);
            target.writeLong(entry.getTimestampMicros());
            writeBytes(entry.getValue(), target);
            return null;
        }

        @Override
        public Void visit(
                BigtableChangeStreamMutation.DeleteCellsEntry entry, DataOutputView target)
                throws IOException {
            target.writeByte(DELETE_CELLS);
            writeString(entry.getFamilyName(), target);
            writeBytes(entry.getQualifier(), target);
            writeRange(entry.getTimestampRange(), target);
            return null;
        }

        @Override
        public Void visit(
                BigtableChangeStreamMutation.DeleteFamilyEntry entry, DataOutputView target)
                throws IOException {
            target.writeByte(DELETE_FAMILY);
            writeString(entry.getFamilyName(), target);
            return null;
        }

        @Override
        public Void visit(BigtableChangeStreamMutation.AddToCellEntry entry, DataOutputView target)
                throws IOException {
            target.writeByte(ADD_TO_CELL);
            writeString(entry.getFamilyName(), target);
            writeValue(entry.getQualifier(), target);
            writeValue(entry.getTimestamp(), target);
            writeValue(entry.getInput(), target);
            return null;
        }

        @Override
        public Void visit(
                BigtableChangeStreamMutation.MergeToCellEntry entry, DataOutputView target)
                throws IOException {
            target.writeByte(MERGE_TO_CELL);
            writeString(entry.getFamilyName(), target);
            writeValue(entry.getQualifier(), target);
            writeValue(entry.getTimestamp(), target);
            writeValue(entry.getInput(), target);
            return null;
        }
    }

    private static void writeMutationType(
            BigtableChangeStreamMutation.MutationType type, DataOutputView target)
            throws IOException {
        switch (type) {
            case USER:
                target.writeByte(1);
                return;
            case GARBAGE_COLLECTION:
                target.writeByte(2);
                return;
            default:
                throw new IOException("Unsupported Bigtable Change Streams mutation type: " + type);
        }
    }

    private static void writeValue(BigtableChangeStreamMutation.Value value, DataOutputView target)
            throws IOException {
        ChangeStreamMutationDispatcher.dispatchValue(value, VALUE_WRITER, target);
    }

    /**
     * Writes each aggregate value subtype behind its wire tag; stateless, for {@link EntryWriter}'s
     * reason.
     */
    private static final class ValueWriter
            implements ChangeStreamMutationDispatcher.ValueVisitor<Void, DataOutputView> {

        @Override
        public Void visit(BigtableChangeStreamMutation.RawValue value, DataOutputView target)
                throws IOException {
            target.writeByte(RAW_VALUE);
            writeBytes(value.getValue(), target);
            return null;
        }

        @Override
        public Void visit(BigtableChangeStreamMutation.RawTimestamp value, DataOutputView target)
                throws IOException {
            target.writeByte(RAW_TIMESTAMP);
            target.writeLong(value.getValue());
            return null;
        }

        @Override
        public Void visit(BigtableChangeStreamMutation.Int64Value value, DataOutputView target)
                throws IOException {
            target.writeByte(INT64);
            target.writeLong(value.getValue());
            return null;
        }
    }

    private static void writeRange(
            BigtableChangeStreamMutation.TimestampRange range, DataOutputView target)
            throws IOException {
        writeBound(range.getStart(), target);
        writeBound(range.getEnd(), target);
    }

    private static void writeBound(
            BigtableChangeStreamMutation.TimestampBound bound, DataOutputView target)
            throws IOException {
        switch (bound.getType()) {
            case UNBOUNDED:
                target.writeByte(0);
                return;
            case OPEN:
                target.writeByte(1);
                target.writeLong(bound.getTimestampMicros().getAsLong());
                return;
            case CLOSED:
                target.writeByte(2);
                target.writeLong(bound.getTimestampMicros().getAsLong());
                return;
            default:
                throw new IOException("Unsupported timestamp bound " + bound.getType());
        }
    }

    @Override
    public BigtableChangeStreamMutation deserialize(DataInputView source) throws IOException {
        ByteString rowKey = readBytes(source);
        int mutationType = source.readUnsignedByte();
        BigtableChangeStreamMutation.MutationType type;
        if (mutationType == 1) {
            type = BigtableChangeStreamMutation.MutationType.USER;
        } else if (mutationType == 2) {
            type = BigtableChangeStreamMutation.MutationType.GARBAGE_COLLECTION;
        } else {
            throw new IOException("Unknown Bigtable Change Streams mutation type: " + mutationType);
        }
        String sourceClusterId = readString(source);
        Instant commitTime = readInstant(source);
        int tieBreaker = source.readInt();
        String token = readString(source);
        Instant estimatedLowWatermarkTime = readInstant(source);
        int count = readCount(source, "entry");
        List<BigtableChangeStreamMutation.Entry> entries = new ArrayList<>(count);
        for (int index = 0; index < count; index++) {
            entries.add(readEntry(source));
        }
        return new BigtableChangeStreamMutation(
                rowKey,
                type,
                sourceClusterId,
                commitTime,
                tieBreaker,
                token,
                estimatedLowWatermarkTime,
                entries);
    }

    private static BigtableChangeStreamMutation.Entry readEntry(DataInputView source)
            throws IOException {
        int tag = source.readUnsignedByte();
        String family = readString(source);
        switch (tag) {
            case SET_CELL:
                return new BigtableChangeStreamMutation.SetCellEntry(
                        family, readBytes(source), source.readLong(), readBytes(source));
            case DELETE_CELLS:
                return new BigtableChangeStreamMutation.DeleteCellsEntry(
                        family, readBytes(source), readRange(source));
            case DELETE_FAMILY:
                return new BigtableChangeStreamMutation.DeleteFamilyEntry(family);
            case ADD_TO_CELL:
                return new BigtableChangeStreamMutation.AddToCellEntry(
                        family, readValue(source), readValue(source), readValue(source));
            case MERGE_TO_CELL:
                return new BigtableChangeStreamMutation.MergeToCellEntry(
                        family, readValue(source), readValue(source), readValue(source));
            default:
                throw new IOException("Unknown Bigtable Change Streams entry tag: " + tag);
        }
    }

    private static BigtableChangeStreamMutation.Value readValue(DataInputView source)
            throws IOException {
        int tag = source.readUnsignedByte();
        switch (tag) {
            case RAW_VALUE:
                return new BigtableChangeStreamMutation.RawValue(readBytes(source));
            case RAW_TIMESTAMP:
                return new BigtableChangeStreamMutation.RawTimestamp(source.readLong());
            case INT64:
                return new BigtableChangeStreamMutation.Int64Value(source.readLong());
            default:
                throw new IOException("Unknown Bigtable Change Streams value tag: " + tag);
        }
    }

    private static BigtableChangeStreamMutation.TimestampRange readRange(DataInputView source)
            throws IOException {
        return new BigtableChangeStreamMutation.TimestampRange(
                readBound(source), readBound(source));
    }

    private static BigtableChangeStreamMutation.TimestampBound readBound(DataInputView source)
            throws IOException {
        int tag = source.readUnsignedByte();
        switch (tag) {
            case 0:
                return BigtableChangeStreamMutation.TimestampBound.unbounded();
            case 1:
                return BigtableChangeStreamMutation.TimestampBound.open(source.readLong());
            case 2:
                return BigtableChangeStreamMutation.TimestampBound.closed(source.readLong());
            default:
                throw new IOException("Unknown Bigtable timestamp-bound tag: " + tag);
        }
    }

    @Override
    public BigtableChangeStreamMutation deserialize(
            BigtableChangeStreamMutation reuse, DataInputView source) throws IOException {
        return deserialize(source);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        byte[] buffer = COPY_BUFFER.get();
        copyByteArray(source, target, buffer, "byte string");
        copyMutationType(source, target);
        copyByteArray(source, target, buffer, "string");
        copyInstant(source, target);
        target.writeInt(source.readInt());
        copyByteArray(source, target, buffer, "string");
        copyInstant(source, target);
        int entryCount = readCount(source, "entry");
        target.writeInt(entryCount);
        for (int index = 0; index < entryCount; index++) {
            copyEntry(source, target, buffer);
        }
    }

    private static void copyMutationType(DataInputView source, DataOutputView target)
            throws IOException {
        int tag = source.readUnsignedByte();
        if (tag != 1 && tag != 2) {
            throw new IOException("Unknown Bigtable Change Streams mutation type: " + tag);
        }
        target.writeByte(tag);
    }

    /**
     * Copies one entry by its wire tag.
     *
     * <p>The visitor over {@link BigtableChangeStreamMutation.Entry} does not reach here, and the
     * bound below is the one thing an added subtype has to be given by hand: a tag outside {@code
     * SET_CELL..MERGE_TO_CELL} is rejected, so a sixth tag is refused until this names it. That
     * refusal is loud rather than silent, but it arrives in a running job.
     */
    private static void copyEntry(DataInputView source, DataOutputView target, byte[] buffer)
            throws IOException {
        int tag = source.readUnsignedByte();
        if (tag < SET_CELL || tag > MERGE_TO_CELL) {
            throw new IOException("Unknown Bigtable Change Streams entry tag: " + tag);
        }
        target.writeByte(tag);
        copyByteArray(source, target, buffer, "string");
        switch (tag) {
            case SET_CELL:
                copyByteArray(source, target, buffer, "byte string");
                target.writeLong(source.readLong());
                copyByteArray(source, target, buffer, "byte string");
                return;
            case DELETE_CELLS:
                copyByteArray(source, target, buffer, "byte string");
                copyBound(source, target);
                copyBound(source, target);
                return;
            case DELETE_FAMILY:
                return;
            case ADD_TO_CELL:
            case MERGE_TO_CELL:
                copyValue(source, target, buffer);
                copyValue(source, target, buffer);
                copyValue(source, target, buffer);
                return;
            default:
                throw new AssertionError("validated entry tag " + tag);
        }
    }

    private static void copyValue(DataInputView source, DataOutputView target, byte[] buffer)
            throws IOException {
        int tag = source.readUnsignedByte();
        if (tag < RAW_VALUE || tag > INT64) {
            throw new IOException("Unknown Bigtable Change Streams value tag: " + tag);
        }
        target.writeByte(tag);
        if (tag == RAW_VALUE) {
            copyByteArray(source, target, buffer, "byte string");
        } else {
            target.writeLong(source.readLong());
        }
    }

    private static void copyBound(DataInputView source, DataOutputView target) throws IOException {
        int tag = source.readUnsignedByte();
        if (tag < 0 || tag > 2) {
            throw new IOException("Unknown Bigtable timestamp-bound tag: " + tag);
        }
        target.writeByte(tag);
        if (tag != 0) {
            target.writeLong(source.readLong());
        }
    }

    private static void copyInstant(DataInputView source, DataOutputView target)
            throws IOException {
        target.writeLong(source.readLong());
        target.writeInt(source.readInt());
    }

    @Override
    public TypeSerializerSnapshot<BigtableChangeStreamMutation> snapshotConfiguration() {
        return new Snapshot();
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof BigtableChangeStreamMutationSerializer;
    }

    @Override
    public int hashCode() {
        return BigtableChangeStreamMutationSerializer.class.hashCode();
    }

    private static void writeString(String value, DataOutputView target) throws IOException {
        writeByteArray(value.getBytes(StandardCharsets.UTF_8), target);
    }

    private static String readString(DataInputView source) throws IOException {
        return new String(readByteArray(source, "string"), StandardCharsets.UTF_8);
    }

    private static void writeBytes(ByteString value, DataOutputView target) throws IOException {
        int length = value.size();
        target.writeInt(length);
        byte[] buffer = COPY_BUFFER.get();
        for (ByteBuffer source : value.asReadOnlyByteBufferList()) {
            while (source.hasRemaining()) {
                int copied = Math.min(source.remaining(), buffer.length);
                source.get(buffer, 0, copied);
                target.write(buffer, 0, copied);
            }
        }
    }

    private static ByteString readBytes(DataInputView source) throws IOException {
        return ByteString.copyFrom(readByteArray(source, "byte string"));
    }

    private static void writeByteArray(byte[] bytes, DataOutputView target) throws IOException {
        target.writeInt(bytes.length);
        target.write(bytes);
    }

    private static byte[] readByteArray(DataInputView source, String description)
            throws IOException {
        int length = source.readInt();
        if (length < 0) {
            throw new IOException("Negative " + description + " length: " + length);
        }
        byte[] bytes = new byte[length];
        source.readFully(bytes);
        return bytes;
    }

    private static void copyByteArray(
            DataInputView source, DataOutputView target, byte[] buffer, String description)
            throws IOException {
        int length = source.readInt();
        if (length < 0) {
            throw new IOException("Negative " + description + " length: " + length);
        }
        target.writeInt(length);
        int remaining = length;
        while (remaining > 0) {
            int copied = Math.min(remaining, buffer.length);
            source.readFully(buffer, 0, copied);
            target.write(buffer, 0, copied);
            remaining -= copied;
        }
    }

    private static void writeInstant(Instant instant, DataOutputView target) throws IOException {
        target.writeLong(instant.getEpochSecond());
        target.writeInt(instant.getNano());
    }

    private static Instant readInstant(DataInputView source) throws IOException {
        return Instant.ofEpochSecond(source.readLong(), source.readInt());
    }

    private static int readCount(DataInputView source, String description) throws IOException {
        int count = source.readInt();
        if (count < 0) {
            throw new IOException("Negative " + description + " count: " + count);
        }
        return count;
    }

    /** Snapshot for the connector-owned field format. */
    public static final class Snapshot
            implements TypeSerializerSnapshot<BigtableChangeStreamMutation> {
        private static final int VERSION = 1;

        @Override
        public int getCurrentVersion() {
            return VERSION;
        }

        @Override
        public void writeSnapshot(DataOutputView out) {}

        @Override
        public void readSnapshot(int readVersion, DataInputView in, ClassLoader userCodeClassLoader)
                throws IOException {
            if (readVersion != VERSION) {
                throw new IOException(
                        "Unsupported BigtableChangeStreamMutation serializer snapshot version "
                                + readVersion
                                + "; this connector reads version "
                                + VERSION
                                + ".");
            }
        }

        @Override
        public TypeSerializer<BigtableChangeStreamMutation> restoreSerializer() {
            return new BigtableChangeStreamMutationSerializer();
        }

        @Override
        public TypeSerializerSchemaCompatibility<BigtableChangeStreamMutation>
                resolveSchemaCompatibility(
                        TypeSerializerSnapshot<BigtableChangeStreamMutation>
                                oldSerializerSnapshot) {
            return oldSerializerSnapshot instanceof Snapshot
                    ? TypeSerializerSchemaCompatibility.compatibleAsIs()
                    : TypeSerializerSchemaCompatibility.incompatible();
        }
    }
}
