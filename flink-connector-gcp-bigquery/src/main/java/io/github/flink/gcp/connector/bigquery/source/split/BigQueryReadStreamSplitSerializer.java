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

package io.github.flink.gcp.connector.bigquery.source.split;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Serializer for {@link BigQueryReadStreamSplit}.
 *
 * <p>Hand-written rather than generated, as the Pub/Sub source's split serializer is: two strings,
 * a long and a flag do not repay a protobuf schema and its code-generation plugin.
 *
 * <p>The Avro schema is written with an explicit length prefix rather than through {@code
 * writeUTF}, whose modified-UTF-8 encoding cannot carry more than 65535 bytes — a limit a wide
 * table's schema reaches, and one that would only be discovered by the job that has such a table.
 */
@Internal
public final class BigQueryReadStreamSplitSerializer
        implements SimpleVersionedSerializer<BigQueryReadStreamSplit> {

    private static final int VERSION = 1;

    /** Enough for a stream name and a small schema; the serializer grows the buffer if needed. */
    private static final int INITIAL_BUFFER_SIZE = 4096;

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(BigQueryReadStreamSplit split) throws IOException {
        DataOutputSerializer out = new DataOutputSerializer(INITIAL_BUFFER_SIZE);
        writeSplit(out, split);
        return out.getCopyOfBuffer();
    }

    @Override
    public BigQueryReadStreamSplit deserialize(int version, byte[] serialized) throws IOException {
        if (version != VERSION) {
            throw new IOException(
                    "Unsupported BigQuery read stream split serialization version "
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
     * <p>Public for the enumerator state serializer, which embeds its pending splits and carries a
     * version of its own, so that the two formats can move independently.
     *
     * @param out the output to write to
     * @param split the split to write
     * @throws IOException if writing fails
     */
    public static void writeSplit(DataOutputSerializer out, BigQueryReadStreamSplit split)
            throws IOException {
        out.writeUTF(split.getStreamName());
        out.writeLong(split.getOffset());
        byte[] schema = split.getAvroSchemaJson().getBytes(StandardCharsets.UTF_8);
        out.writeInt(schema.length);
        out.write(schema);
    }

    /**
     * Reads a split written by {@link #writeSplit}.
     *
     * @param in the input to read from
     * @return the split
     * @throws IOException if reading fails
     */
    public static BigQueryReadStreamSplit readSplit(DataInputDeserializer in) throws IOException {
        String streamName = in.readUTF();
        long offset = in.readLong();
        int schemaLength = in.readInt();
        if (schemaLength < 0) {
            throw new IOException(
                    "Corrupt BigQuery read stream split: negative schema length " + schemaLength);
        }
        byte[] schema = new byte[schemaLength];
        in.readFully(schema);
        return new BigQueryReadStreamSplit(
                streamName, offset, new String(schema, StandardCharsets.UTF_8));
    }
}
