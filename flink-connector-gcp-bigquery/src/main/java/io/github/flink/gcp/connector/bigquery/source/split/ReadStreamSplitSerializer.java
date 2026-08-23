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

package io.github.flink.gcp.connector.bigquery.source.split;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;

/**
 * Serializer for {@link ReadStreamSplit}.
 *
 * <p>Hand-written rather than generated, as the Pub/Sub source's split serializer is: two strings,
 * a long and a flag do not repay a protobuf schema and its code-generation plugin.
 *
 * <p>The Avro schema is written with an explicit length prefix rather than through {@code
 * writeUTF}, whose modified-UTF-8 encoding cannot carry more than 65535 bytes — a limit a wide
 * table's schema reaches, and one that would only be discovered by the job that has such a table.
 *
 * <p>Version 2 appended the session's expiry. Version 1 is still read, as the same layout without
 * it: a job restoring a checkpoint written before the field existed carries {@code null} there and
 * loses nothing but the annotation on a failure it may never meet.
 */
@Internal
public final class ReadStreamSplitSerializer implements SimpleVersionedSerializer<ReadStreamSplit> {

    /** The split layout that carried no session expiry. */
    public static final int VERSION_WITHOUT_EXPIRY = 1;

    /** The split layout this serializer writes, and the version {@link #getVersion()} reports. */
    public static final int VERSION_WITH_EXPIRY = 2;

    /** Enough for a stream name and a small schema; the serializer grows the buffer if needed. */
    private static final int INITIAL_BUFFER_SIZE = 4096;

    @Override
    public int getVersion() {
        return VERSION_WITH_EXPIRY;
    }

    @Override
    public byte[] serialize(ReadStreamSplit split) throws IOException {
        DataOutputSerializer out = new DataOutputSerializer(INITIAL_BUFFER_SIZE);
        writeSplit(out, split);
        return out.getCopyOfBuffer();
    }

    @Override
    public ReadStreamSplit deserialize(int version, byte[] serialized) throws IOException {
        if (version != VERSION_WITH_EXPIRY && version != VERSION_WITHOUT_EXPIRY) {
            throw new IOException(
                    "Unsupported BigQuery read stream split serialization version "
                            + version
                            + "; this connector writes version "
                            + VERSION_WITH_EXPIRY
                            + ".");
        }
        return readSplit(new DataInputDeserializer(serialized), version);
    }

    /**
     * Writes a split without a version tag.
     *
     * <p>Public for the enumerator state serializer, which embeds its pending splits and carries a
     * version of its own, so that the two formats can move independently.
     *
     * @param out the output to write to
     * @param split the split to write
     * @throws IOException if writing fails
     */
    public static void writeSplit(DataOutputSerializer out, ReadStreamSplit split)
            throws IOException {
        out.writeUTF(split.getStreamName());
        out.writeLong(split.getOffset());
        byte[] schema = split.getAvroSchemaJson().getBytes(StandardCharsets.UTF_8);
        out.writeInt(schema.length);
        out.write(schema);
        Instant expireTime = split.getSessionExpireTime();
        out.writeBoolean(expireTime != null);
        if (expireTime != null) {
            out.writeLong(expireTime.getEpochSecond());
            out.writeInt(expireTime.getNano());
        }
    }

    /**
     * Reads a split written by {@link #writeSplit}.
     *
     * @param in the input to read from
     * @param version the layout the split was written in: {@link #VERSION_WITH_EXPIRY}, or {@link
     *     #VERSION_WITHOUT_EXPIRY} for one written before the session expiry was carried
     * @return the split
     * @throws IOException if reading fails
     */
    public static ReadStreamSplit readSplit(DataInputDeserializer in, int version)
            throws IOException {
        String streamName = in.readUTF();
        long offset = in.readLong();
        int schemaLength = in.readInt();
        if (schemaLength < 0) {
            throw new IOException(
                    "Corrupt BigQuery read stream split: negative schema length " + schemaLength);
        }
        byte[] schema = new byte[schemaLength];
        in.readFully(schema);
        Instant expireTime = null;
        if (version >= VERSION_WITH_EXPIRY && in.readBoolean()) {
            expireTime = Instant.ofEpochSecond(in.readLong(), in.readInt());
        }
        return new ReadStreamSplit(
                streamName, offset, new String(schema, StandardCharsets.UTF_8), expireTime);
    }
}
