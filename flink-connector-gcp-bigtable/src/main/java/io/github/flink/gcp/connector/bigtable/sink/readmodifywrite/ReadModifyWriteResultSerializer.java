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

package io.github.flink.gcp.connector.bigtable.sink.readmodifywrite;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.Preconditions;

import io.github.flink.gcp.connector.bigtable.LengthPrefixedFields;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.BigtableRow;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.BigtableRowSerializer;

import java.io.IOException;

/**
 * Serializes the destination's three UTF-8 components followed by the changed row. Snapshot version
 * 1 records the nested row serializer's snapshot.
 */
@Internal
public final class ReadModifyWriteResultSerializer extends TypeSerializer<ReadModifyWriteResult> {

    private static final long serialVersionUID = 1L;
    private final TypeSerializer<BigtableRow> rowSerializer;

    public ReadModifyWriteResultSerializer() {
        this(new BigtableRowSerializer());
    }

    private ReadModifyWriteResultSerializer(TypeSerializer<BigtableRow> rowSerializer) {
        this.rowSerializer = rowSerializer;
    }

    @Override
    public boolean isImmutableType() {
        return true;
    }

    @Override
    public TypeSerializer<ReadModifyWriteResult> duplicate() {
        return new ReadModifyWriteResultSerializer(rowSerializer.duplicate());
    }

    @Override
    public ReadModifyWriteResult createInstance() {
        return null;
    }

    @Override
    public ReadModifyWriteResult copy(ReadModifyWriteResult from) {
        return from;
    }

    @Override
    public ReadModifyWriteResult copy(ReadModifyWriteResult from, ReadModifyWriteResult reuse) {
        return from;
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    public void serialize(ReadModifyWriteResult record, DataOutputView target) throws IOException {
        Preconditions.checkNotNull(record, "record must not be null");
        LengthPrefixedFields.writeString(record.getDestination().getProject(), target);
        LengthPrefixedFields.writeString(record.getDestination().getInstance(), target);
        LengthPrefixedFields.writeString(record.getDestination().getTable(), target);
        rowSerializer.serialize(record.getRow(), target);
    }

    @Override
    public ReadModifyWriteResult deserialize(DataInputView source) throws IOException {
        TableDestination destination =
                TableDestination.of(
                        LengthPrefixedFields.readString(source),
                        LengthPrefixedFields.readString(source),
                        LengthPrefixedFields.readString(source));
        return new ReadModifyWriteResult(destination, rowSerializer.deserialize(source));
    }

    @Override
    public ReadModifyWriteResult deserialize(ReadModifyWriteResult reuse, DataInputView source)
            throws IOException {
        return deserialize(source);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        byte[] buffer = LengthPrefixedFields.copyBuffer();
        for (int i = 0; i < 3; i++) {
            LengthPrefixedFields.copyByteArray(source, target, buffer, "string");
        }
        rowSerializer.copy(source, target);
    }

    @Override
    public TypeSerializerSnapshot<ReadModifyWriteResult> snapshotConfiguration() {
        return new Snapshot(rowSerializer.snapshotConfiguration());
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof ReadModifyWriteResultSerializer
                && rowSerializer.equals(((ReadModifyWriteResultSerializer) other).rowSerializer);
    }

    @Override
    public int hashCode() {
        return 31 * ReadModifyWriteResultSerializer.class.hashCode() + rowSerializer.hashCode();
    }

    /** Snapshot for the connector-owned field format. */
    public static final class Snapshot implements TypeSerializerSnapshot<ReadModifyWriteResult> {
        private static final int VERSION = 1;
        private TypeSerializerSnapshot<BigtableRow> rowSnapshot;

        public Snapshot() {}

        private Snapshot(TypeSerializerSnapshot<BigtableRow> rowSnapshot) {
            this.rowSnapshot = rowSnapshot;
        }

        @Override
        public int getCurrentVersion() {
            return VERSION;
        }

        @Override
        public void writeSnapshot(DataOutputView out) throws IOException {
            TypeSerializerSnapshot.writeVersionedSnapshot(out, rowSnapshot);
        }

        @Override
        public void readSnapshot(int readVersion, DataInputView in, ClassLoader userCodeClassLoader)
                throws IOException {
            if (readVersion != VERSION) {
                throw new IOException(
                        "Unsupported ReadModifyWriteResult serializer snapshot version "
                                + readVersion
                                + "; this connector reads version "
                                + VERSION
                                + ".");
            }
            rowSnapshot = TypeSerializerSnapshot.readVersionedSnapshot(in, userCodeClassLoader);
        }

        @Override
        public TypeSerializer<ReadModifyWriteResult> restoreSerializer() {
            return new ReadModifyWriteResultSerializer(rowSnapshot.restoreSerializer());
        }

        @Override
        public TypeSerializerSchemaCompatibility<ReadModifyWriteResult> resolveSchemaCompatibility(
                TypeSerializerSnapshot<ReadModifyWriteResult> oldSerializerSnapshot) {
            if (!(oldSerializerSnapshot instanceof Snapshot)) {
                return TypeSerializerSchemaCompatibility.incompatible();
            }
            return rowSnapshot
                            .resolveSchemaCompatibility(
                                    ((Snapshot) oldSerializerSnapshot).rowSnapshot)
                            .isCompatibleAsIs()
                    ? TypeSerializerSchemaCompatibility.compatibleAsIs()
                    : TypeSerializerSchemaCompatibility.incompatible();
        }
    }
}
