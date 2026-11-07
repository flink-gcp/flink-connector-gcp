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

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link MessageIdLedgerCodec}. */
class MessageIdLedgerCodecTest {

    @Test
    void roundTripsIdsOfAnyShape() throws IOException {
        // Pub/Sub documents a message id only as a string unique within its topic, so nothing about
        // its alphabet may be assumed: digits, letters, separators, multi-byte characters, and ids
        // that are prefixes of one another.
        List<String> ids =
                Arrays.asList(
                        "17045123456789012",
                        "17045123456789013",
                        "1704512345678901",
                        "abc-DEF_123",
                        "ab",
                        "メッセージ",
                        "メッ",
                        "😀emoji",
                        "");

        assertThat(decodeAll(MessageIdLedgerCodec.encode(ids, 1 << 20)))
                .containsExactlyInAnyOrderElementsOf(ids);
    }

    @Test
    void decimalIdsDecodeInTheirByteOrderWhateverTheirLengths() throws IOException {
        // Ids of digits alone take the packed path; its order must still be the byte order that
        // front coding relies on, including prefixes, trailing zeros and leading zeros.
        List<String> ids =
                new ArrayList<>(
                        Arrays.asList(
                                "17045123456789012",
                                "1704512345678901",
                                "17045123456789020",
                                "1",
                                "10",
                                "100",
                                "0",
                                "007",
                                "17045123456789012"));
        List<String> expected = new ArrayList<>(new TreeSet<>(ids));

        List<byte[]> chunks = MessageIdLedgerCodec.encode(ids, 1 << 20);

        assertThat(chunks).hasSize(1);
        assertThat(MessageIdLedgerCodec.decode(chunks.get(0))).containsExactlyElementsOf(expected);
    }

    @Test
    void aSingleNonDecimalIdSendsEveryIdThroughTheByteOrderPath() throws IOException {
        // One id that does not pack moves the whole set to the byte-array path, which must still
        // drop duplicates and decode in unsigned byte order.
        List<String> ids =
                Arrays.asList(
                        "17045123456789012",
                        "007",
                        "abc",
                        "170451234567890123",
                        "abc",
                        "メッ",
                        "007");

        List<byte[]> chunks = MessageIdLedgerCodec.encode(ids, 1 << 20);

        assertThat(chunks).hasSize(1);
        assertThat(MessageIdLedgerCodec.decode(chunks.get(0)))
                .containsExactly("007", "17045123456789012", "170451234567890123", "abc", "メッ");
    }

    @Test
    void longIdsCloseChunksOnTheByteOrderPath() throws IOException {
        // Ids far longer than the writer's initial copy buffer, sharing long prefixes.
        List<String> ids = new ArrayList<>();
        for (int i = 0; i < 2_000; i++) {
            ids.add("projects/p/topics/t/messages/" + "x".repeat(i % 50) + "-" + i);
        }

        List<byte[]> chunks = MessageIdLedgerCodec.encode(ids, 4096);

        assertThat(chunks).hasSizeGreaterThan(1);
        assertThat(decodeAll(chunks)).containsExactlyInAnyOrderElementsOf(ids);
    }

    @Test
    void anIdMoreThanTwiceAsLongAsItsPredecessorRoundTrips() throws IOException {
        List<String> ids = Arrays.asList("a", "a" + "b".repeat(100), "a" + "b".repeat(1000));

        assertThat(decodeAll(MessageIdLedgerCodec.encode(ids, 1 << 20)))
                .containsExactlyElementsOf(ids);
    }

    @Test
    void leavesOutIdsWithAnUnpairedSurrogateRatherThanAlteringThem() throws IOException {
        // UTF-8 encoding would turn each lone surrogate into "?", merging these four ids into two
        // and putting "?" into the ledger for messages whose id is not "?".
        List<String> ids = Arrays.asList("\uD800", "?", "a\uDC00b", "a?b", "\uD83D\uDE00ok");

        assertThat(decodeAll(MessageIdLedgerCodec.encode(ids, 1 << 20)))
                .containsExactlyInAnyOrder("?", "a?b", "\uD83D\uDE00ok");
    }

    @Test
    void encodesNothingForIdsThatAllHoldAnUnpairedSurrogate() {
        assertThat(MessageIdLedgerCodec.encode(Arrays.asList("\uD800", "a\uDC00b"), 1 << 20))
                .isEmpty();
    }

    @Test
    void dropsDuplicates() throws IOException {
        List<String> chunks =
                decodeAll(MessageIdLedgerCodec.encode(Arrays.asList("1", "2", "1", "2"), 1 << 20));

        assertThat(chunks).containsExactly("1", "2");
    }

    @Test
    void encodesNothingAsNoChunks() {
        assertThat(MessageIdLedgerCodec.encode(Collections.emptyList(), 1024)).isEmpty();
    }

    @Test
    void closesChunksAtTheLimitAndEachDecodesOnItsOwn() throws IOException {
        List<String> ids = new ArrayList<>();
        for (int i = 0; i < 10_000; i++) {
            ids.add(Long.toString(17_045_000_000_000_000L + i * 7L));
        }
        int limit = 4096;

        List<byte[]> chunks = MessageIdLedgerCodec.encode(ids, limit);

        assertThat(chunks).hasSizeGreaterThan(1);
        Set<String> decoded = new HashSet<>();
        for (byte[] chunk : chunks) {
            // A chunk closes once it reaches the limit, so it exceeds it by at most one entry plus
            // the header.
            assertThat(chunk.length).isLessThan(limit + 64);
            decoded.addAll(MessageIdLedgerCodec.decode(chunk));
        }
        assertThat(decoded).containsExactlyInAnyOrderElementsOf(ids);
    }

    @Test
    void frontCodingShrinksIdsThatShareAPrefix() {
        List<String> ids = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            ids.add(Long.toString(17_045_000_000_000_000L + i));
        }

        int encoded = MessageIdLedgerCodec.encode(ids, 1 << 20).get(0).length;

        // Seventeen characters each as plain strings; the shared prefix leaves a few bytes.
        assertThat(encoded).isLessThan(ids.size() * 6);
    }

    @Test
    void rejectsAnUnknownFormat() {
        assertThatThrownBy(() -> MessageIdLedgerCodec.decode(new byte[] {9, 0}))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Unsupported message-id ledger format 9");
    }

    @Test
    void rejectsATruncatedChunk() {
        byte[] chunk = MessageIdLedgerCodec.encode(Arrays.asList("12345", "12346"), 1024).get(0);

        assertThatThrownBy(
                        () -> MessageIdLedgerCodec.decode(Arrays.copyOf(chunk, chunk.length - 1)))
                .isInstanceOf(IOException.class);
    }

    @Test
    void rejectsAnEntryCountTheChunkCannotHoldBeforeAllocatingForIt() {
        // Format 1, then a varint count of 2^31 - 1 and nothing else.
        byte[] chunk = {1, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, 0x07};

        assertThatThrownBy(() -> MessageIdLedgerCodec.decode(chunk))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("2147483647 entries");
    }

    @Test
    void rejectsASuffixLongerThanTheChunkBeforeAllocatingForIt() {
        // Format 1, one entry, no shared prefix, a suffix of 2^31 - 1 bytes that are not there.
        byte[] chunk = {1, 1, 0, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, 0x07, 0, 0};

        assertThatThrownBy(() -> MessageIdLedgerCodec.decode(chunk))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("claims 2147483647 bytes");
    }

    @Test
    void rejectsAVarintBeyondAnIntRatherThanTruncatingIt() {
        // Format 1, then a count of 2^32: kept to 32 bits it would read as zero entries.
        byte[] chunk = {1, (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80, 0x10};

        assertThatThrownBy(() -> MessageIdLedgerCodec.decode(chunk))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("varint exceeds an int");
    }

    @Test
    void rejectsAVarintThatWouldBeNegative() {
        // Format 1, then a count of 2^31, which an int holds only as a negative number.
        byte[] chunk = {1, (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80, 0x08};

        assertThatThrownBy(() -> MessageIdLedgerCodec.decode(chunk))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("varint exceeds an int");
    }

    @Test
    void rejectsAPrefixLongerThanItsPredecessor() {
        // Format 1, one entry, sharing 3 bytes with a predecessor that does not exist.
        assertThatThrownBy(() -> MessageIdLedgerCodec.decode(new byte[] {1, 1, 3, 0}))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("shares 3 bytes");
    }

    private static List<String> decodeAll(List<byte[]> chunks) throws IOException {
        List<String> ids = new ArrayList<>();
        for (byte[] chunk : chunks) {
            ids.addAll(MessageIdLedgerCodec.decode(chunk));
        }
        return ids;
    }
}
