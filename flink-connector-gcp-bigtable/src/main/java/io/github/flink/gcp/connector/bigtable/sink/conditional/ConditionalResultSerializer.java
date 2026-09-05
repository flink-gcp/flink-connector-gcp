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

package io.github.flink.gcp.connector.bigtable.sink.conditional;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.Preconditions;

import io.github.flink.gcp.connector.bigtable.LengthPrefixedFields;
import io.github.flink.gcp.connector.bigtable.TableDestination;

import java.io.IOException;

/**
 * Serializes the destination's three UTF-8 components, the row-key bytes and two outcome booleans.
 * Variable fields use LengthPrefixedFields; snapshot version 1 fixes their order.
 */
@Internal
public final class ConditionalResultSerializer extends TypeSerializer<ConditionalResult> {

    private static final long serialVersionUID = 1L;

    @Override
    public boolean isImmutableType() {
        return true;
    }

    @Override
    public TypeSerializer<ConditionalResult> duplicate() {
        return this;
    }

    @Override
    public ConditionalResult createInstance() {
        return null;
    }

    @Override
    public ConditionalResult copy(ConditionalResult from) {
        return from;
    }

    @Override
    public ConditionalResult copy(ConditionalResult from, ConditionalResult reuse) {
        return from;
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    public void serialize(ConditionalResult record, DataOutputView target) throws IOException {
        Preconditions.checkNotNull(record, "record must not be null");
        LengthPrefixedFields.writeString(record.getDestination().getProject(), target);
        LengthPrefixedFields.writeString(record.getDestination().getInstance(), target);
        LengthPrefixedFields.writeString(record.getDestination().getTable(), target);
        LengthPrefixedFields.writeBytes(record.getRowKey(), target);
        target.writeBoolean(record.isPredicateMatched());
        target.writeBoolean(record.isSelectedBranchHasMutations());
    }

    @Override
    public ConditionalResult deserialize(DataInputView source) throws IOException {
        TableDestination destination =
                TableDestination.of(
                        LengthPrefixedFields.readString(source),
                        LengthPrefixedFields.readString(source),
                        LengthPrefixedFields.readString(source));
        return new ConditionalResult(
                destination,
                LengthPrefixedFields.readBytes(source),
                source.readBoolean(),
                source.readBoolean());
    }

    @Override
    public ConditionalResult deserialize(ConditionalResult reuse, DataInputView source)
            throws IOException {
        return deserialize(source);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        byte[] buffer = LengthPrefixedFields.copyBuffer();
        for (int i = 0; i < 4; i++) {
            LengthPrefixedFields.copyByteArray(
                    source, target, buffer, i < 3 ? "string" : "byte string");
        }
        target.writeBoolean(source.readBoolean());
        target.writeBoolean(source.readBoolean());
    }

    @Override
    public TypeSerializerSnapshot<ConditionalResult> snapshotConfiguration() {
        return new Snapshot();
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof ConditionalResultSerializer;
    }

    @Override
    public int hashCode() {
        return ConditionalResultSerializer.class.hashCode();
    }

    /** Snapshot for the connector-owned field format. */
    public static final class Snapshot implements TypeSerializerSnapshot<ConditionalResult> {
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
                        "Unsupported ConditionalResult serializer snapshot version "
                                + readVersion
                                + "; this connector reads version "
                                + VERSION
                                + ".");
            }
        }

        @Override
        public TypeSerializer<ConditionalResult> restoreSerializer() {
            return new ConditionalResultSerializer();
        }

        @Override
        public TypeSerializerSchemaCompatibility<ConditionalResult> resolveSchemaCompatibility(
                TypeSerializerSnapshot<ConditionalResult> oldSerializerSnapshot) {
            return oldSerializerSnapshot instanceof Snapshot
                    ? TypeSerializerSchemaCompatibility.compatibleAsIs()
                    : TypeSerializerSchemaCompatibility.incompatible();
        }
    }
}
