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

package io.github.flink.gcp.connector.firestore.source.batch;

import org.apache.flink.core.memory.DataOutputSerializer;

import com.google.cloud.Timestamp;
import com.google.cloud.firestore.FieldPath;
import com.google.cloud.firestore.Firestore;
import com.google.firestore.v1.RunQueryRequest;
import com.google.firestore.v1.StructuredQuery;
import io.github.flink.gcp.connector.firestore.TestDocuments;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** The split's and the enumerator state's byte formats. */
class QuerySplitSerializerTest {

    private static final Timestamp READ_TIME =
            Timestamp.ofTimeSecondsAndNanos(1_700_000_000L, 123_456_000);

    private static Firestore client;

    @BeforeAll
    static void openClient() {
        client = TestDocuments.offlineClient("p");
    }

    @AfterAll
    static void closeClient() throws Exception {
        client.close();
    }

    private static QuerySplit split(String id) {
        return new QuerySplit(
                id,
                client.collectionGroup("orders")
                        .select("a", "b.c")
                        .orderBy(FieldPath.documentId())
                        .startAt(client.document("orders/x"))
                        .endBefore(client.document("parents/p/orders/y"))
                        .limit(9)
                        .toProto(),
                READ_TIME);
    }

    @Test
    void roundTripsASplit() throws IOException {
        QuerySplitSerializer serializer = new QuerySplitSerializer();
        QuerySplit split = split("12");

        QuerySplit read =
                serializer.deserialize(serializer.getVersion(), serializer.serialize(split));

        assertThat(read).isEqualTo(split);
        assertThat(read.getReadTime().getNanos()).isEqualTo(123_456_000);
    }

    @Test
    void roundTripsTheEnumeratorState() throws IOException {
        FirestoreBatchEnumeratorStateSerializer serializer =
                new FirestoreBatchEnumeratorStateSerializer();
        FirestoreBatchEnumeratorState state =
                new FirestoreBatchEnumeratorState(true, List.of(split("0"), split("1")));

        assertThat(serializer.deserialize(serializer.getVersion(), serializer.serialize(state)))
                .isEqualTo(state);
    }

    @Test
    void readsAndWritesTheFrozenVersionOneLayout() throws IOException {
        // Checkpoints written by an earlier build are restored by this one: the layout of version
        // one does not move without a version bump.
        QuerySplit split =
                new QuerySplit(
                        "0",
                        RunQueryRequest.newBuilder()
                                .setParent("p")
                                .setStructuredQuery(
                                        StructuredQuery.newBuilder()
                                                .addFrom(
                                                        StructuredQuery.CollectionSelector
                                                                .newBuilder()
                                                                .setCollectionId("c")))
                                .build(),
                        Timestamp.ofTimeSecondsAndNanos(1, 2));
        byte[] frozen = {
            0,
            1,
            '0', // split id, modified UTF-8
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            1, // read time seconds
            0,
            0,
            0,
            2, // read time nanos
            0,
            0,
            0,
            10, // query length
            0x0A,
            1,
            'p',
            0x12,
            5,
            0x12,
            3,
            0x12,
            1,
            'c' // RunQueryRequest protobuf
        };
        QuerySplitSerializer serializer = new QuerySplitSerializer();

        assertThat(serializer.getVersion()).isOne();
        assertThat(serializer.serialize(split)).isEqualTo(frozen);
        assertThat(serializer.deserialize(1, frozen)).isEqualTo(split);
    }

    @Test
    void refusesAnUnknownVersion() {
        assertThatThrownBy(() -> new QuerySplitSerializer().deserialize(2, new byte[0]))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("version 2");
        assertThatThrownBy(
                        () ->
                                new FirestoreBatchEnumeratorStateSerializer()
                                        .deserialize(2, new byte[0]))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("version 2");
    }

    @Test
    void refusesACorruptQuery() throws IOException {
        DataOutputSerializer out = new DataOutputSerializer(64);
        out.writeUTF("0");
        out.writeLong(READ_TIME.getSeconds());
        out.writeInt(READ_TIME.getNanos());
        // Field 1, length-delimited, announcing 127 bytes that never come.
        byte[] garbage = {0x0A, 0x7F};
        out.writeInt(garbage.length);
        out.write(garbage);

        assertThatThrownBy(() -> new QuerySplitSerializer().deserialize(1, out.getCopyOfBuffer()))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Corrupt Firestore query split 0");
    }

    @Test
    void refusesANegativeQueryLength() throws IOException {
        DataOutputSerializer out = new DataOutputSerializer(64);
        out.writeUTF("0");
        out.writeLong(0L);
        out.writeInt(0);
        out.writeInt(-1);

        assertThatThrownBy(() -> new QuerySplitSerializer().deserialize(1, out.getCopyOfBuffer()))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("negative query length");
    }

    @Test
    void aSplitRefusesAQueryThatCarriesItsOwnReadTime() {
        RunQueryRequest withReadTime =
                client.collection("c").toProto().toBuilder()
                        .setReadTime(READ_TIME.toProto())
                        .build();

        assertThatThrownBy(() -> new QuerySplit("0", withReadTime, READ_TIME))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("read time");
    }
}
