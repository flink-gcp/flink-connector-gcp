/*
 * Copyright 2026 laughingman7743
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

package io.github.flink.gcp.connector.spanner.source.batch;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import com.google.cloud.spanner.BatchTransactionId;
import com.google.cloud.spanner.Partition;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamClass;
import java.io.Serializable;

/**
 * Serializer for {@link PartitionSplit}.
 *
 * <p>The two values a split carries are written with Java serialization, behind this serializer's
 * own version byte and an explicit length prefix each. That is a deliberate exception to this
 * repository's rule that a checkpointed split owns a byte format the connector controls, and it is
 * made because the client library leaves no alternative: a partition token is opaque, every
 * accessor on {@code Partition} but {@code getPartitionToken()} is package-private, {@code
 * BatchTransactionId} exposes none at all, and neither type has a public factory to rebuild one
 * from parts. Both are documented as serializable and as travelling between machines — that is what
 * they are for.
 *
 * <p>What normally makes delegating to a vendor format dangerous is that a client upgrade can move
 * it under a checkpoint written by an older version. Here the exposure is bounded by something else
 * first: the values name a batch transaction, and a snapshot older than the database's {@code
 * version_retention_period} cannot be read at all. A savepoint that outlives the format also
 * outlives the transaction, so the format is not what fails.
 *
 * <p>Classes are resolved through this connector's own classloader rather than through whichever
 * one happens to be latest on the calling stack, which is not the same thing inside a Flink
 * TaskManager.
 */
@Internal
public final class PartitionSplitSerializer implements SimpleVersionedSerializer<PartitionSplit> {

    private static final int VERSION = 1;

    /** A partition token runs to a few hundred bytes; the serializer grows the buffer if needed. */
    private static final int INITIAL_BUFFER_SIZE = 1024;

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(PartitionSplit split) throws IOException {
        DataOutputSerializer out = new DataOutputSerializer(INITIAL_BUFFER_SIZE);
        writeSplit(out, split);
        return out.getCopyOfBuffer();
    }

    @Override
    public PartitionSplit deserialize(int version, byte[] serialized) throws IOException {
        if (version != VERSION) {
            throw new IOException(
                    "Unsupported Spanner partition split serialization version "
                            + version
                            + "; this connector writes version "
                            + VERSION
                            + ".");
        }
        return readSplit(new DataInputDeserializer(serialized));
    }

    /**
     * Writes a split without a version tag.
     *
     * <p>Separate from {@link #serialize} for the enumerator state serializer, which embeds its
     * pending splits and carries a version of its own, so that the two formats can move
     * independently.
     *
     * @param out the output to write to
     * @param split the split to write
     * @throws IOException if writing fails
     */
    static void writeSplit(DataOutputSerializer out, PartitionSplit split) throws IOException {
        out.writeUTF(split.splitId());
        writeObject(out, split.getBatchTransactionId());
        writeObject(out, split.getPartition());
    }

    /**
     * Reads a split written by {@link #writeSplit}.
     *
     * @param in the input to read from
     * @return the split
     * @throws IOException if reading fails
     */
    static PartitionSplit readSplit(DataInputDeserializer in) throws IOException {
        String splitId = in.readUTF();
        BatchTransactionId batchTransactionId =
                readObject(in, BatchTransactionId.class, "batch transaction id");
        Partition partition = readObject(in, Partition.class, "partition");
        return new PartitionSplit(splitId, batchTransactionId, partition);
    }

    private static void writeObject(DataOutputSerializer out, Serializable value)
            throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream objects = new ObjectOutputStream(bytes)) {
            objects.writeObject(value);
        }
        byte[] serialized = bytes.toByteArray();
        out.writeInt(serialized.length);
        out.write(serialized);
    }

    private static <T> T readObject(DataInputDeserializer in, Class<T> type, String what)
            throws IOException {
        int length = in.readInt();
        if (length < 0) {
            throw new IOException(
                    "Corrupt Spanner partition split: negative " + what + " length " + length);
        }
        byte[] serialized = new byte[length];
        in.readFully(serialized);
        Object value;
        try (ObjectInputStream objects =
                new ConnectorObjectInputStream(new ByteArrayInputStream(serialized))) {
            value = objects.readObject();
        } catch (ClassNotFoundException e) {
            throw new IOException(
                    "Could not read the "
                            + what
                            + " of a Spanner partition split; a class it names is not on the"
                            + " classpath.",
                    e);
        }
        if (!type.isInstance(value)) {
            throw new IOException(
                    "Corrupt Spanner partition split: expected the "
                            + what
                            + " to be a "
                            + type.getName()
                            + " but it was "
                            + (value == null ? "null" : value.getClass().getName())
                            + ".");
        }
        return type.cast(value);
    }

    /**
     * Resolves classes through the connector's own classloader.
     *
     * <p>{@link ObjectInputStream} otherwise resolves through the latest user-defined classloader
     * on the calling stack, which inside a TaskManager is whichever job happened to call in. The
     * fallback covers what {@code Class.forName} cannot name — the primitive types a serialized
     * field descriptor may hold.
     */
    private static final class ConnectorObjectInputStream extends ObjectInputStream {

        ConnectorObjectInputStream(InputStream in) throws IOException {
            super(in);
        }

        @Override
        protected Class<?> resolveClass(ObjectStreamClass desc)
                throws IOException, ClassNotFoundException {
            try {
                return Class.forName(
                        desc.getName(), false, PartitionSplitSerializer.class.getClassLoader());
            } catch (ClassNotFoundException e) {
                return super.resolveClass(desc);
            }
        }
    }
}
