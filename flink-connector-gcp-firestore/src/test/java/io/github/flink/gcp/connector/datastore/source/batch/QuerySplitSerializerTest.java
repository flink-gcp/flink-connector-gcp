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

package io.github.flink.gcp.connector.datastore.source.batch;

import org.apache.flink.core.memory.DataOutputSerializer;

import com.google.cloud.Timestamp;
import com.google.datastore.v1.PartitionId;
import com.google.datastore.v1.PropertyFilter;
import com.google.datastore.v1.ReadOptions;
import com.google.datastore.v1.RunQueryRequest;
import com.google.datastore.v1.client.DatastoreHelper;
import com.google.protobuf.ByteString;
import com.google.protobuf.Int32Value;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** The split's and the enumerator state's byte formats. */
class QuerySplitSerializerTest {

    private static final Timestamp READ_TIME =
            Timestamp.ofTimeSecondsAndNanos(1_700_000_000L, 123_456_000);

    private static QuerySplit split(String id) {
        return new QuerySplit(
                id,
                RunQueryRequest.newBuilder()
                        .setProjectId("p")
                        .setDatabaseId("db")
                        .setPartitionId(
                                PartitionId.newBuilder()
                                        .setProjectId("p")
                                        .setDatabaseId("db")
                                        .setNamespaceId("tenant"))
                        .setQuery(
                                SplittableQueries.ofKind("Task").toBuilder()
                                        .setFilter(
                                                DatastoreHelper.makeAndFilter(
                                                        DatastoreHelper.makeFilter(
                                                                        "done",
                                                                        PropertyFilter.Operator
                                                                                .EQUAL,
                                                                        DatastoreHelper.makeValue(
                                                                                false))
                                                                .build(),
                                                        DatastoreHelper.makeFilter(
                                                                        "__key__",
                                                                        PropertyFilter.Operator
                                                                                .LESS_THAN,
                                                                        DatastoreHelper.makeValue(
                                                                                DatastoreHelper
                                                                                        .makeKey(
                                                                                                "Task",
                                                                                                "m")))
                                                                .build()))
                                        .setStartCursor(ByteString.copyFromUtf8("cursor"))
                                        .setLimit(Int32Value.of(9)))
                        .build(),
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
        DatastoreBatchEnumeratorStateSerializer serializer =
                new DatastoreBatchEnumeratorStateSerializer();
        DatastoreBatchEnumeratorState state =
                new DatastoreBatchEnumeratorState(true, List.of(split("0"), split("1")));

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
                                .setProjectId("p")
                                .setQuery(SplittableQueries.ofKind("c"))
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
            10, // request length
            0x1A,
            5,
            0x1A,
            3,
            0x0A,
            1,
            'c', // query (field 3): kind (field 3): name (field 1)
            0x42,
            1,
            'p' // project_id (field 8)
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
                                new DatastoreBatchEnumeratorStateSerializer()
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
        // Field 3, length-delimited, announcing 127 bytes that never come.
        byte[] garbage = {0x1A, 0x7F};
        out.writeInt(garbage.length);
        out.write(garbage);

        assertThatThrownBy(() -> new QuerySplitSerializer().deserialize(1, out.getCopyOfBuffer()))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Corrupt Datastore query split 0");
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
    void aSplitRefusesARequestThatCarriesItsOwnReadOptions() {
        RunQueryRequest withReadTime =
                split("0").getRequest().toBuilder()
                        .setReadOptions(ReadOptions.newBuilder().setReadTime(READ_TIME.toProto()))
                        .build();

        assertThatThrownBy(() -> new QuerySplit("0", withReadTime, READ_TIME))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("read options");
    }

    @Test
    void aSplitRefusesARequestWithoutAStructuredQuery() {
        RunQueryRequest gql =
                RunQueryRequest.newBuilder()
                        .setGqlQuery(
                                com.google.datastore.v1.GqlQuery.newBuilder()
                                        .setQueryString("SELECT * FROM Task"))
                        .build();

        assertThatThrownBy(() -> new QuerySplit("0", gql, READ_TIME))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("structured query");
    }
}
