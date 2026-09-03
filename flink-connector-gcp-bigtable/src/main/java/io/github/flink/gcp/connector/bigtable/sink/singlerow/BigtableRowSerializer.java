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

package io.github.flink.gcp.connector.bigtable.sink.singlerow;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.Preconditions;

import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.bigtable.LengthPrefixedFields;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Serializes {@link BigtableRow} without reflective collection access.
 *
 * <p>Wire format: the key as a length-prefixed byte string, a cell count, and per cell, in this
 * order, the family as a length-prefixed UTF-8 string, the qualifier as a length-prefixed byte
 * string, the timestamp as a long, the value as a length-prefixed byte string, and the labels as a
 * count followed by length-prefixed UTF-8 strings. The field encodings are {@link
 * LengthPrefixedFields}', shared with the change-stream serializer.
 */
@Internal
public final class BigtableRowSerializer extends TypeSerializer<BigtableRow> {

    private static final long serialVersionUID = 1L;

    @Override
    public boolean isImmutableType() {
        return true;
    }

    @Override
    public TypeSerializer<BigtableRow> duplicate() {
        return this;
    }

    @Override
    public BigtableRow createInstance() {
        return null;
    }

    @Override
    public BigtableRow copy(BigtableRow from) {
        return from;
    }

    @Override
    public BigtableRow copy(BigtableRow from, BigtableRow reuse) {
        return from;
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    public void serialize(BigtableRow record, DataOutputView target) throws IOException {
        Preconditions.checkNotNull(record, "record must not be null");
        LengthPrefixedFields.writeBytes(record.getKey(), target);
        target.writeInt(record.getCells().size());
        for (BigtableRow.Cell cell : record.getCells()) {
            LengthPrefixedFields.writeString(cell.getFamily(), target);
            LengthPrefixedFields.writeBytes(cell.getQualifier(), target);
            target.writeLong(cell.getTimestampMicros());
            LengthPrefixedFields.writeBytes(cell.getValue(), target);
            target.writeInt(cell.getLabels().size());
            for (String label : cell.getLabels()) {
                LengthPrefixedFields.writeString(label, target);
            }
        }
    }

    @Override
    public BigtableRow deserialize(DataInputView source) throws IOException {
        ByteString key = LengthPrefixedFields.readBytes(source);
        int cellCount = LengthPrefixedFields.readCount(source, "cell");
        List<BigtableRow.Cell> cells = new ArrayList<>(cellCount);
        for (int index = 0; index < cellCount; index++) {
            String family = LengthPrefixedFields.readString(source);
            ByteString qualifier = LengthPrefixedFields.readBytes(source);
            long timestampMicros = source.readLong();
            ByteString value = LengthPrefixedFields.readBytes(source);
            int labelCount = LengthPrefixedFields.readCount(source, "label");
            List<String> labels = new ArrayList<>(labelCount);
            for (int labelIndex = 0; labelIndex < labelCount; labelIndex++) {
                labels.add(LengthPrefixedFields.readString(source));
            }
            cells.add(new BigtableRow.Cell(family, qualifier, timestampMicros, value, labels));
        }
        return new BigtableRow(key, cells);
    }

    @Override
    public BigtableRow deserialize(BigtableRow reuse, DataInputView source) throws IOException {
        return deserialize(source);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        byte[] buffer = LengthPrefixedFields.copyBuffer();
        LengthPrefixedFields.copyByteArray(source, target, buffer, "byte string");
        int cellCount = LengthPrefixedFields.readCount(source, "cell");
        target.writeInt(cellCount);
        for (int index = 0; index < cellCount; index++) {
            LengthPrefixedFields.copyByteArray(source, target, buffer, "string");
            LengthPrefixedFields.copyByteArray(source, target, buffer, "byte string");
            target.writeLong(source.readLong());
            LengthPrefixedFields.copyByteArray(source, target, buffer, "byte string");
            int labelCount = LengthPrefixedFields.readCount(source, "label");
            target.writeInt(labelCount);
            for (int labelIndex = 0; labelIndex < labelCount; labelIndex++) {
                LengthPrefixedFields.copyByteArray(source, target, buffer, "string");
            }
        }
    }

    @Override
    public TypeSerializerSnapshot<BigtableRow> snapshotConfiguration() {
        return new Snapshot();
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof BigtableRowSerializer;
    }

    @Override
    public int hashCode() {
        return BigtableRowSerializer.class.hashCode();
    }

    /** Snapshot for the connector-owned field format. */
    public static final class Snapshot implements TypeSerializerSnapshot<BigtableRow> {
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
                        "Unsupported BigtableRow serializer snapshot version "
                                + readVersion
                                + "; this connector reads version "
                                + VERSION
                                + ".");
            }
        }

        @Override
        public TypeSerializer<BigtableRow> restoreSerializer() {
            return new BigtableRowSerializer();
        }

        @Override
        public TypeSerializerSchemaCompatibility<BigtableRow> resolveSchemaCompatibility(
                TypeSerializerSnapshot<BigtableRow> oldSerializerSnapshot) {
            return oldSerializerSnapshot instanceof Snapshot
                    ? TypeSerializerSchemaCompatibility.compatibleAsIs()
                    : TypeSerializerSchemaCompatibility.incompatible();
        }
    }
}
