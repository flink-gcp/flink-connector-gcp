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

package io.github.flink.gcp.connector.datastore.table.source;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

import com.google.auth.Credentials;
import com.google.cloud.Timestamp;
import com.google.datastore.v1.Entity;
import com.google.datastore.v1.EntityResult;
import com.google.datastore.v1.Key;
import com.google.datastore.v1.PartitionId;
import com.google.datastore.v1.QueryResultBatch;
import com.google.datastore.v1.RunQueryRequest;
import com.google.datastore.v1.Value;
import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.datastore.DatabaseDestination;
import io.github.flink.gcp.connector.datastore.source.DatastoreSource;
import io.github.flink.gcp.connector.datastore.source.DatastoreSourceBuilder;
import io.github.flink.gcp.connector.datastore.source.TestSources;
import io.github.flink.gcp.connector.datastore.source.batch.DatastoreBatchSource;
import io.github.flink.gcp.connector.datastore.source.batch.SplittableQueries;
import io.github.flink.gcp.connector.datastore.source.batch.reader.QueryPageReader;
import io.github.flink.gcp.connector.datastore.table.DatastoreTableSchema;
import io.github.flink.gcp.connector.datastore.table.TypeMismatchPolicy;
import io.github.flink.gcp.connector.datastore.table.source.DatastoreFullCacheInputFormat.LoadSplit;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** The full lookup cache's loader over scripted pages, for answers the emulator never gives. */
class DatastoreFullCacheInputFormatTest {

    private static final RowType ROW =
            (RowType)
                    DataTypes.ROW(
                                    DataTypes.FIELD("id", DataTypes.STRING().notNull()),
                                    DataTypes.FIELD("n", DataTypes.BIGINT()))
                            .getLogicalType();

    private static EntityResult entity(String name, long n) {
        return EntityResult.newBuilder()
                .setEntity(
                        Entity.newBuilder()
                                .setKey(
                                        Key.newBuilder()
                                                .setPartitionId(
                                                        PartitionId.newBuilder().setProjectId("p"))
                                                .addPath(
                                                        Key.PathElement.newBuilder()
                                                                .setKind("K")
                                                                .setName(name)))
                                .putProperties("n", Value.newBuilder().setIntegerValue(n).build()))
                .setCursor(ByteString.copyFromUtf8("after-" + name))
                .build();
    }

    @Test
    void aPageWithNoEntityWhoseCursorMovedContinuesTheLoad() throws Exception {
        // The service may scan entries a pushed filter rejects and answer with none, more to come.
        ScriptedPages pages =
                new ScriptedPages(
                        QueryResultBatch.newBuilder()
                                .setMoreResults(QueryResultBatch.MoreResultsType.NOT_FINISHED)
                                .setEndCursor(ByteString.copyFromUtf8("scanned"))
                                .build(),
                        QueryResultBatch.newBuilder()
                                .addEntityResults(entity("a", 1L))
                                .setMoreResults(QueryResultBatch.MoreResultsType.NO_MORE_RESULTS)
                                .setEndCursor(ByteString.copyFromUtf8("after-a"))
                                .build());
        RowDataDeserializationSchema deserializer =
                new RowDataDeserializationSchema(
                        DatastoreTableSchema.of(
                                ROW, new int[] {0}, List.of(), List.of(), List.of()),
                        TestSources.DATABASE,
                        "",
                        new int[] {0, 1},
                        List.of(),
                        TypeMismatchPolicy.FAIL,
                        InternalTypeInfo.of(ROW));
        DatastoreSourceBuilder<RowData> builder =
                DatastoreSource.<RowData>builder()
                        .database(TestSources.DATABASE)
                        .deserializer(deserializer)
                        // Never connected to: the split is built here, so nothing is planned.
                        .emulatorEndpoint("localhost:1")
                        .kind("K")
                        .splitCount(1);
        DatastoreFullCacheInputFormat format =
                new DatastoreFullCacheInputFormat(
                        ((DatastoreBatchSource<RowData>)
                                        TestSources.withPageReader(builder, pages).build())
                                .getConfig());
        LoadSplit split =
                new LoadSplit(
                        RunQueryRequest.newBuilder()
                                .setProjectId("p")
                                .setQuery(SplittableQueries.ofKind("K"))
                                .build(),
                        Timestamp.ofTimeSecondsAndNanos(1_786_000_000L, 0));
        format.configure(new Configuration());
        format.open(split);
        List<String> rows = new ArrayList<>();
        try {
            while (!format.reachedEnd()) {
                RowData row = format.nextRecord(null);
                rows.add(row.getString(0) + "=" + row.getLong(1));
            }
        } finally {
            format.close();
        }

        assertThat(rows).containsExactly("a=1");
        assertThat(pages.requests).hasSize(2);
        assertThat(pages.requests.get(1).getQuery().getStartCursor().toStringUtf8())
                .isEqualTo("scanned");
    }

    /** Answers each page request with the next scripted batch and records the requests. */
    private static final class ScriptedPages implements QueryPageReader {

        private static final long serialVersionUID = 1L;

        private final List<QueryResultBatch> batches;
        private final List<RunQueryRequest> requests = new ArrayList<>();

        ScriptedPages(QueryResultBatch... batches) {
            this.batches = List.of(batches);
        }

        @Override
        public QueryResultBatch read(DatabaseDestination database, RunQueryRequest page) {
            requests.add(page);
            return batches.get(requests.size() - 1);
        }

        @Override
        public void useCredentials(@Nullable Credentials credentials) {}

        @Override
        public void close() {}
    }
}
