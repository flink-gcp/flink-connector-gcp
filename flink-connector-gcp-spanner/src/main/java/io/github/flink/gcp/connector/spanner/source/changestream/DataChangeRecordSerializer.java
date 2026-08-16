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
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** Serializes {@link DataChangeRecord} without reflective access to JDK implementation fields. */
@Internal
public final class DataChangeRecordSerializer extends TypeSerializer<DataChangeRecord> {

    private static final long serialVersionUID = 1L;
    private static final ThreadLocal<byte[]> COPY_BUFFER =
            ThreadLocal.withInitial(() -> new byte[4 * 1024]);

    @Override
    public boolean isImmutableType() {
        return true;
    }

    @Override
    public TypeSerializer<DataChangeRecord> duplicate() {
        return this;
    }

    @Override
    public DataChangeRecord createInstance() {
        return null;
    }

    @Override
    public DataChangeRecord copy(DataChangeRecord from) {
        return from;
    }

    @Override
    public DataChangeRecord copy(DataChangeRecord from, DataChangeRecord reuse) {
        return from;
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    public void serialize(DataChangeRecord record, DataOutputView target) throws IOException {
        Objects.requireNonNull(record, "record");
        Instant commitTimestamp = record.getCommitTimestamp();
        target.writeLong(commitTimestamp.getEpochSecond());
        target.writeInt(commitTimestamp.getNano());
        writeString(record.getRecordSequence(), target);
        writeString(record.getServerTransactionId(), target);
        target.writeBoolean(record.isLastRecordInTransactionInPartition());
        writeString(record.getTableName(), target);

        List<DataChangeRecord.ColumnType> columnTypes = record.getColumnTypes();
        target.writeInt(columnTypes.size());
        for (DataChangeRecord.ColumnType columnType : columnTypes) {
            writeString(columnType.getName(), target);
            writeString(columnType.getTypeDescriptorJson(), target);
            target.writeBoolean(columnType.isPrimaryKey());
            target.writeLong(columnType.getOrdinalPosition());
        }

        List<Mod> mods = record.getMods();
        target.writeInt(mods.size());
        for (Mod mod : mods) {
            writeString(mod.getKeysJson(), target);
            writeNullableString(mod.getNewValuesJson().orElse(null), target);
            writeNullableString(mod.getOldValuesJson().orElse(null), target);
        }

        writeString(record.getModType().name(), target);
        writeString(record.getValueCaptureType().name(), target);
        target.writeLong(record.getNumberOfRecordsInTransaction());
        target.writeLong(record.getNumberOfPartitionsInTransaction());
        writeString(record.getTransactionTag(), target);
        target.writeBoolean(record.isSystemTransaction());
    }

    @Override
    public DataChangeRecord deserialize(DataInputView source) throws IOException {
        Instant commitTimestamp = Instant.ofEpochSecond(source.readLong(), source.readInt());
        String recordSequence = readString(source);
        String serverTransactionId = readString(source);
        boolean lastRecordInTransactionInPartition = source.readBoolean();
        String tableName = readString(source);

        int columnCount = readCount(source, "column type");
        List<DataChangeRecord.ColumnType> columnTypes = new ArrayList<>(columnCount);
        for (int i = 0; i < columnCount; i++) {
            columnTypes.add(
                    new DataChangeRecord.ColumnType(
                            readString(source),
                            readString(source),
                            source.readBoolean(),
                            source.readLong()));
        }

        int modCount = readCount(source, "mod");
        List<Mod> mods = new ArrayList<>(modCount);
        for (int i = 0; i < modCount; i++) {
            mods.add(
                    new Mod(
                            readString(source),
                            readNullableString(source),
                            readNullableString(source)));
        }

        ModType modType = readEnum(ModType.class, source);
        ValueCaptureType valueCaptureType = readEnum(ValueCaptureType.class, source);
        long numberOfRecordsInTransaction = source.readLong();
        long numberOfPartitionsInTransaction = source.readLong();
        String transactionTag = readString(source);
        boolean systemTransaction = source.readBoolean();

        return new DataChangeRecord(
                commitTimestamp,
                recordSequence,
                serverTransactionId,
                lastRecordInTransactionInPartition,
                tableName,
                columnTypes,
                mods,
                modType,
                valueCaptureType,
                numberOfRecordsInTransaction,
                numberOfPartitionsInTransaction,
                transactionTag,
                systemTransaction);
    }

    @Override
    public DataChangeRecord deserialize(DataChangeRecord reuse, DataInputView source)
            throws IOException {
        return deserialize(source);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        byte[] buffer = COPY_BUFFER.get();
        target.writeLong(source.readLong());
        target.writeInt(source.readInt());
        copyString(source, target, buffer);
        copyString(source, target, buffer);
        target.writeBoolean(source.readBoolean());
        copyString(source, target, buffer);

        int columnCount = readCount(source, "column type");
        target.writeInt(columnCount);
        for (int i = 0; i < columnCount; i++) {
            copyString(source, target, buffer);
            copyString(source, target, buffer);
            target.writeBoolean(source.readBoolean());
            target.writeLong(source.readLong());
        }

        int modCount = readCount(source, "mod");
        target.writeInt(modCount);
        for (int i = 0; i < modCount; i++) {
            copyString(source, target, buffer);
            copyNullableString(source, target, buffer);
            copyNullableString(source, target, buffer);
        }

        copyString(source, target, buffer);
        copyString(source, target, buffer);
        target.writeLong(source.readLong());
        target.writeLong(source.readLong());
        copyString(source, target, buffer);
        target.writeBoolean(source.readBoolean());
    }

    @Override
    public TypeSerializerSnapshot<DataChangeRecord> snapshotConfiguration() {
        return new Snapshot();
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof DataChangeRecordSerializer;
    }

    @Override
    public int hashCode() {
        return DataChangeRecordSerializer.class.hashCode();
    }

    private static void writeNullableString(String value, DataOutputView target)
            throws IOException {
        target.writeBoolean(value != null);
        if (value != null) {
            writeString(value, target);
        }
    }

    private static String readNullableString(DataInputView source) throws IOException {
        return source.readBoolean() ? readString(source) : null;
    }

    private static void copyNullableString(
            DataInputView source, DataOutputView target, byte[] buffer) throws IOException {
        boolean present = source.readBoolean();
        target.writeBoolean(present);
        if (present) {
            copyString(source, target, buffer);
        }
    }

    private static void writeString(String value, DataOutputView target) throws IOException {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        target.writeInt(bytes.length);
        target.write(bytes);
    }

    private static String readString(DataInputView source) throws IOException {
        int length = source.readInt();
        if (length < 0) {
            throw new IOException("Negative string length: " + length);
        }
        byte[] bytes = new byte[length];
        source.readFully(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private static void copyString(DataInputView source, DataOutputView target, byte[] buffer)
            throws IOException {
        int length = source.readInt();
        if (length < 0) {
            throw new IOException("Negative string length: " + length);
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

    private static int readCount(DataInputView source, String value) throws IOException {
        int count = source.readInt();
        if (count < 0) {
            throw new IOException("Negative " + value + " count: " + count);
        }
        return count;
    }

    private static <E extends Enum<E>> E readEnum(Class<E> enumType, DataInputView source)
            throws IOException {
        String name = readString(source);
        try {
            return Enum.valueOf(enumType, name);
        } catch (IllegalArgumentException e) {
            throw new IOException("Unknown " + enumType.getSimpleName() + ": " + name, e);
        }
    }

    /** Snapshot for the connector-owned field format. */
    public static final class Snapshot implements TypeSerializerSnapshot<DataChangeRecord> {

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
                        "Unsupported DataChangeRecord serializer snapshot version "
                                + readVersion
                                + "; this connector reads version "
                                + VERSION
                                + ".");
            }
        }

        @Override
        public TypeSerializer<DataChangeRecord> restoreSerializer() {
            return new DataChangeRecordSerializer();
        }

        @Override
        public TypeSerializerSchemaCompatibility<DataChangeRecord> resolveSchemaCompatibility(
                TypeSerializerSnapshot<DataChangeRecord> oldSerializerSnapshot) {
            return oldSerializerSnapshot instanceof Snapshot
                    ? TypeSerializerSchemaCompatibility.compatibleAsIs()
                    : TypeSerializerSchemaCompatibility.incompatible();
        }
    }
}
