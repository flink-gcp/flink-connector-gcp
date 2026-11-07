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

package io.github.flink.gcp.connector.pubsub.source.streamingpull;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * Encodes the message ids of a restore-deduplication ledger into bounded chunks.
 *
 * <p>Pub/Sub documents a message id only as a string unique within its topic, so the codec assumes
 * nothing about its alphabet or length. The ids are sorted by their UTF-8 bytes and front-coded:
 * each entry stores how many leading bytes it shares with the previous entry and the remaining
 * suffix. Ids the service assigns from a common counter share long prefixes and shrink well; any
 * other string still round-trips exactly, except one holding an unpaired surrogate. Such a string
 * has no UTF-8 form, and encoding it would turn it into another id, so it is left out: a missing
 * entry costs a duplicate, while a wrong one would suppress a message that no checkpoint holds.
 * Protobuf decodes a delivered message id from UTF-8, so a delivered id never holds one.
 *
 * <p>Every chunk is self-contained — its first entry shares nothing — so chunks can be decoded in
 * any order and on any subtask. A chunk closes once its entries reach the size limit, so a chunk
 * exceeds the limit by at most one entry and its header.
 */
@Internal
public final class MessageIdLedgerCodec {

    private static final int FORMAT_VERSION = 1;

    private MessageIdLedgerCodec() {}

    /**
     * Encodes the given ids, dropping duplicates, into chunks of about {@code maxChunkBytes} each.
     *
     * @param messageIds the ids to encode, in any order
     * @param maxChunkBytes the size at which a chunk is closed; must be positive
     * @return the chunks, empty when there are no ids
     */
    public static List<byte[]> encode(Collection<String> messageIds, int maxChunkBytes) {
        Preconditions.checkArgument(maxChunkBytes > 0, "maxChunkBytes must be positive");
        ChunkWriter writer = new ChunkWriter(maxChunkBytes);
        long[] packed = packAll(messageIds);
        if (packed != null) {
            // Packed values sort in the ids' byte order, and sorting primitives is several times
            // faster than sorting byte arrays; this runs on the task thread during the
            // synchronous part of every checkpoint.
            Arrays.sort(packed);
            byte[] id = new byte[DecimalMessageIds.MAX_DIGITS];
            for (int i = 0; i < packed.length; i++) {
                if (i > 0 && packed[i] == packed[i - 1]) {
                    continue;
                }
                writer.add(id, DecimalMessageIds.unpack(packed[i], id));
            }
            return writer.finish();
        }
        byte[][] sorted = new byte[messageIds.size()][];
        int count = 0;
        for (String id : messageIds) {
            if (isWellFormed(id)) {
                sorted[count++] = id.getBytes(StandardCharsets.UTF_8);
            }
        }
        Arrays.sort(sorted, 0, count, Arrays::compareUnsigned);
        for (int i = 0; i < count; i++) {
            if (i > 0 && Arrays.equals(sorted[i - 1], sorted[i])) {
                continue;
            }
            writer.add(sorted[i], sorted[i].length);
        }
        return writer.finish();
    }

    /** Whether every surrogate in the string belongs to a pair, so that it has a UTF-8 form. */
    private static boolean isWellFormed(String id) {
        for (int i = 0; i < id.length(); i++) {
            char c = id.charAt(i);
            if (Character.isHighSurrogate(c)
                    && i + 1 < id.length()
                    && Character.isLowSurrogate(id.charAt(i + 1))) {
                i++;
            } else if (Character.isSurrogate(c)) {
                return false;
            }
        }
        return true;
    }

    /** Packs every id, or returns {@code null} as soon as one does not pack. */
    @Nullable
    private static long[] packAll(Collection<String> messageIds) {
        long[] packed = new long[messageIds.size()];
        int count = 0;
        for (String id : messageIds) {
            long value = DecimalMessageIds.pack(id);
            if (value == DecimalMessageIds.NOT_PACKED) {
                return null;
            }
            packed[count++] = value;
        }
        return packed;
    }

    /**
     * Decodes one chunk.
     *
     * @param chunk a chunk produced by {@link #encode(Collection, int)}
     * @return the ids in the chunk, in encoding order
     * @throws IOException if the chunk is malformed or of an unknown format
     */
    public static List<String> decode(byte[] chunk) throws IOException {
        DataInputDeserializer in = new DataInputDeserializer(chunk);
        int version = in.readUnsignedByte();
        if (version != FORMAT_VERSION) {
            throw new IOException(
                    "Unsupported message-id ledger format "
                            + version
                            + "; this connector writes format "
                            + FORMAT_VERSION
                            + ".");
        }
        int entryCount = readVarInt(in);
        // Every entry takes at least two bytes, so a count the remaining input cannot hold is
        // rejected before anything is allocated for it.
        if (entryCount > in.available() / 2) {
            throw new IOException(
                    "Malformed message-id ledger: "
                            + entryCount
                            + " entries in "
                            + in.available()
                            + " bytes.");
        }
        List<String> ids = new ArrayList<>(entryCount);
        byte[] previous = new byte[0];
        for (int i = 0; i < entryCount; i++) {
            int shared = readVarInt(in);
            int suffixLength = readVarInt(in);
            if (suffixLength > in.available()) {
                throw new IOException(
                        "Malformed message-id ledger: entry "
                                + i
                                + " claims "
                                + suffixLength
                                + " bytes, but "
                                + in.available()
                                + " remain.");
            }
            if (shared > previous.length) {
                throw new IOException(
                        "Malformed message-id ledger: entry "
                                + i
                                + " shares "
                                + shared
                                + " bytes with a "
                                + previous.length
                                + "-byte predecessor.");
            }
            byte[] id = Arrays.copyOf(previous, shared + suffixLength);
            in.readFully(id, shared, suffixLength);
            ids.add(new String(id, StandardCharsets.UTF_8));
            previous = id;
        }
        if (in.available() != 0) {
            throw new IOException(
                    "Malformed message-id ledger: " + in.available() + " trailing bytes.");
        }
        return ids;
    }

    /** Front-codes ids handed to it in sorted order into self-contained chunks. */
    private static final class ChunkWriter {
        private final int maxChunkBytes;
        private final List<byte[]> chunks = new ArrayList<>();
        private final DataOutputSerializer entries;
        private int entryCount;
        // A copy, because the packed path reuses its id buffer.
        private byte[] previous = new byte[16];
        private int previousLength = -1;

        ChunkWriter(int maxChunkBytes) {
            this.maxChunkBytes = maxChunkBytes;
            this.entries = new DataOutputSerializer(Math.min(maxChunkBytes, 1 << 16));
        }

        void add(byte[] id, int length) {
            try {
                int shared =
                        previousLength < 0 ? 0 : sharedPrefix(previous, previousLength, id, length);
                writeVarInt(entries, shared);
                writeVarInt(entries, length - shared);
                entries.write(id, shared, length - shared);
                entryCount++;
                if (previous.length < length) {
                    previous = new byte[Math.max(length, previous.length * 2)];
                }
                System.arraycopy(id, 0, previous, 0, length);
                previousLength = length;
                if (entries.length() >= maxChunkBytes) {
                    chunks.add(seal(entries, entryCount));
                    entries.clear();
                    entryCount = 0;
                    previousLength = -1;
                }
            } catch (IOException e) {
                // DataOutputSerializer writes to memory and grows on demand; it does not fail.
                throw new IllegalStateException("Failed to encode the message-id ledger", e);
            }
        }

        List<byte[]> finish() {
            if (entryCount > 0) {
                try {
                    chunks.add(seal(entries, entryCount));
                } catch (IOException e) {
                    throw new IllegalStateException("Failed to encode the message-id ledger", e);
                }
            }
            return chunks;
        }
    }

    private static byte[] seal(DataOutputSerializer entries, int entryCount) throws IOException {
        DataOutputSerializer header = new DataOutputSerializer(6);
        header.writeByte(FORMAT_VERSION);
        writeVarInt(header, entryCount);
        byte[] chunk = new byte[header.length() + entries.length()];
        System.arraycopy(header.getSharedBuffer(), 0, chunk, 0, header.length());
        System.arraycopy(entries.getSharedBuffer(), 0, chunk, header.length(), entries.length());
        return chunk;
    }

    private static int sharedPrefix(byte[] a, int aLength, byte[] b, int bLength) {
        int limit = Math.min(aLength, bLength);
        int i = 0;
        while (i < limit && a[i] == b[i]) {
            i++;
        }
        return i;
    }

    private static void writeVarInt(DataOutputSerializer out, int value) throws IOException {
        int remaining = value;
        while ((remaining & ~0x7F) != 0) {
            out.writeByte((remaining & 0x7F) | 0x80);
            remaining >>>= 7;
        }
        out.writeByte(remaining);
    }

    private static int readVarInt(DataInputDeserializer in) throws IOException {
        int value = 0;
        for (int shift = 0; shift < 32; shift += 7) {
            int b = in.readUnsignedByte();
            if (shift == 28 && (b & 0xF8) != 0) {
                // The fifth byte has room for the top three bits of a non-negative int only;
                // anything more would be silently cut off below.
                throw new IOException("Malformed message-id ledger: varint exceeds an int.");
            }
            value |= (b & 0x7F) << shift;
            if ((b & 0x80) == 0) {
                return value;
            }
        }
        throw new IOException("Malformed message-id ledger: varint longer than five bytes.");
    }
}
