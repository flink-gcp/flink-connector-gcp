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
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.InstantiationUtil;

import com.google.auth.Credentials;
import com.google.cloud.datastore.Entity;
import com.google.datastore.v1.QueryResultBatch;
import com.google.datastore.v1.RunQueryRequest;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.datastore.AbstractDatastoreEmulatorITCase;
import io.github.flink.gcp.connector.datastore.DatabaseDestination;
import io.github.flink.gcp.connector.datastore.source.DatastoreSource;
import io.github.flink.gcp.connector.datastore.source.DatastoreSourceBuilder;
import io.github.flink.gcp.connector.datastore.source.FixedSplitsPlannerFactory;
import io.github.flink.gcp.connector.datastore.source.TestSources;
import io.github.flink.gcp.connector.datastore.source.batch.DatastoreBatchSource;
import io.github.flink.gcp.connector.datastore.source.batch.reader.ClientQueryPageReader;
import io.github.flink.gcp.connector.datastore.source.batch.reader.QueryPageReader;
import io.github.flink.gcp.connector.datastore.table.DatastoreTableSchema;
import io.github.flink.gcp.connector.datastore.table.TypeMismatchPolicy;
import io.github.flink.gcp.connector.datastore.table.source.DatastoreFullCacheInputFormat.LoadSplit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.UnaryOperator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The full lookup cache's loader against the emulator, driven as Flink's {@code
 * InputFormatCacheLoader} drives it: splits from the initial format, each read by its own copy.
 */
@Testcontainers
class DatastoreFullCacheInputFormatITCase extends AbstractDatastoreEmulatorITCase {

    private static final RowType PHYSICAL =
            (RowType)
                    DataTypes.ROW(
                                    DataTypes.FIELD("id", DataTypes.STRING().notNull()),
                                    DataTypes.FIELD("n", DataTypes.BIGINT()))
                            .getLogicalType();

    private static final RowType PRODUCED =
            (RowType)
                    DataTypes.ROW(
                                    DataTypes.FIELD("id", DataTypes.STRING().notNull()),
                                    DataTypes.FIELD("n", DataTypes.BIGINT()),
                                    DataTypes.FIELD(
                                            "read_time", DataTypes.TIMESTAMP_LTZ(6).notNull()))
                            .getLogicalType();

    @AfterEach
    void forgetRecordings() {
        FailingPageReader.reset();
    }

    private static RowDataDeserializationSchema deserializer() {
        return new RowDataDeserializationSchema(
                DatastoreTableSchema.of(PHYSICAL, new int[] {0}, List.of(), List.of(), List.of()),
                database(),
                "",
                new int[] {0, 1},
                List.of("read-time"),
                TypeMismatchPolicy.FAIL,
                InternalTypeInfo.of(PRODUCED));
    }

    private static DatastoreFullCacheInputFormat format(
            UnaryOperator<DatastoreSourceBuilder<RowData>> customizer) {
        DatastoreSourceBuilder<RowData> builder =
                DatastoreSource.<RowData>builder()
                        .database(database())
                        .deserializer(deserializer())
                        .emulatorEndpoint(emulatorEndpoint());
        return new DatastoreFullCacheInputFormat(
                ((DatastoreBatchSource<RowData>) customizer.apply(builder).build()).getConfig());
    }

    private static DatastoreFullCacheInputFormat kindFormat(String kind, int pageSize) {
        return format(builder -> builder.kind(kind).splitCount(1).pageSize(pageSize));
    }

    /** One load: every split of the initial format, each read to its end by its own copy. */
    private static List<String> load(DatastoreFullCacheInputFormat initial, List<LoadSplit> splits)
            throws Exception {
        LoadSplit[] planned = initial.createInputSplits(1);
        List<String> rows = new ArrayList<>();
        for (LoadSplit split : planned) {
            splits.add(split);
            rows.addAll(read(initial, split));
        }
        return rows;
    }

    /**
     * Reads one split to its end in a copy of the initial format, as Flink's loader does, and
     * checks that every row carries the split's read time as its {@code read-time} metadata.
     */
    private static List<String> read(DatastoreFullCacheInputFormat initial, LoadSplit split)
            throws Exception {
        List<String> rows = new ArrayList<>();
        DatastoreFullCacheInputFormat copy = InstantiationUtil.clone(initial);
        copy.configure(new Configuration());
        copy.open(split);
        TimestampData readTime =
                TimestampData.fromInstant(
                        Instant.ofEpochSecond(
                                split.getReadTime().getSeconds(), split.getReadTime().getNanos()));
        try {
            while (!copy.reachedEnd()) {
                RowData row = copy.nextRecord(null);
                assertThat(row.getTimestamp(2, 6)).isEqualTo(readTime);
                rows.add(
                        row.getString(0)
                                + "="
                                + (row.isNullAt(1) ? "null" : String.valueOf(row.getLong(1))));
            }
        } finally {
            copy.close();
        }
        return rows;
    }

    private static void put(String kind, String name, long n) {
        client().put(Entity.newBuilder(key(kind, name)).set("n", n).build());
    }

    @Test
    void oneSplitReadsTheKindAsOneQueryWhereTheScanWouldCutKeyRanges() throws Exception {
        String kind = uniqueKind();
        put(kind, "a", 1L);
        put(kind, "b", 2L);
        put(kind, "c", 3L);
        // The planner factory cuts ranges at 'b' whenever it is asked to split, which the
        // configured single split never does.
        DatastoreFullCacheInputFormat initial =
                format(
                        builder ->
                                TestSources.withPlannerFactory(
                                        builder.kind(kind).splitCount(1).pageSize(1),
                                        new FixedSplitsPlannerFactory(
                                                emulatorEndpoint(), List.of("b"))));
        List<LoadSplit> splits = new ArrayList<>();

        // A page of one entity makes the query take several requests.
        assertThat(load(initial, splits)).containsExactly("a=1", "b=2", "c=3");
        assertThat(splits).hasSize(1);
        assertThat(splits.get(0).getQuery().getQuery().hasFilter()).isFalse();
    }

    @Test
    void aLoadReadsTheSnapshotOfItsPlanningNotOfItsRead() throws Exception {
        String kind = uniqueKind();
        put(kind, "a", 1L);
        DatastoreFullCacheInputFormat initial = kindFormat(kind, 10);
        LoadSplit split = initial.createInputSplits(1)[0];
        put(kind, "a", 10L);
        put(kind, "b", 2L);

        assertThat(read(initial, split)).containsExactly("a=1");
    }

    @Test
    void everyLoadPlansAgainAndReadsANewerSnapshot() throws Exception {
        String kind = uniqueKind();
        put(kind, "a", 1L);
        put(kind, "b", 2L);
        DatastoreFullCacheInputFormat initial = kindFormat(kind, 1);
        List<LoadSplit> first = new ArrayList<>();
        List<LoadSplit> second = new ArrayList<>();

        assertThat(load(initial, first)).containsExactly("a=1", "b=2");
        put(kind, "a", 10L);
        put(kind, "c", 3L);

        assertThat(load(initial, second)).containsExactly("a=10", "b=2", "c=3");
        assertThat(first).hasSize(1);
        assertThat(second).hasSize(1);
        assertThat(second.get(0).getReadTime()).isGreaterThan(first.get(0).getReadTime());
    }

    @Test
    void anEmptyKindLoadsNoRowsFromOneSplit() throws Exception {
        List<LoadSplit> splits = new ArrayList<>();

        assertThat(load(kindFormat(uniqueKind(), 10), splits)).isEmpty();
        assertThat(splits).hasSize(1);
    }

    @Test
    void aFailedOpenReleasesThePageReaderItTook() throws Exception {
        DatastoreFullCacheInputFormat initial =
                format(
                        builder ->
                                TestSources.withPageReader(
                                        builder.kind(uniqueKind()).splitCount(1),
                                        new FailingPageReader(emulatorEndpoint(), 0, true)));
        LoadSplit split = initial.createInputSplits(1)[0];
        DatastoreFullCacheInputFormat copy = InstantiationUtil.clone(initial);

        assertThatThrownBy(() -> copy.open(split))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("scripted credential failure");
        // The failed open released the page reader itself.
        assertThat(FailingPageReader.CLOSES.get()).isEqualTo(1);
        // Flink's loader closes the format after a failed open too; nothing is released twice.
        copy.close();

        assertThat(FailingPageReader.CLOSES.get()).isEqualTo(1);
    }

    @Test
    void aFailedPageFailsTheLoadAndCloseStillReleasesThePageReader() throws Exception {
        String kind = uniqueKind();
        put(kind, "a", 1L);
        put(kind, "b", 2L);
        DatastoreFullCacheInputFormat initial =
                format(
                        builder ->
                                TestSources.withPageReader(
                                        builder.kind(kind).splitCount(1).pageSize(1),
                                        new FailingPageReader(emulatorEndpoint(), 1, false)));
        LoadSplit split = initial.createInputSplits(1)[0];
        DatastoreFullCacheInputFormat copy = InstantiationUtil.clone(initial);
        copy.open(split);

        assertThat(copy.reachedEnd()).isFalse();
        assertThat(copy.nextRecord(null).getString(0).toString()).isEqualTo("a");
        assertThatThrownBy(copy::reachedEnd)
                .isInstanceOf(IOException.class)
                .hasStackTraceContaining("scripted page failure");
        // Flink's loader closes the format in a finally block after any failure.
        copy.close();
        copy.close();

        assertThat(FailingPageReader.CLOSES.get()).isEqualTo(1);
    }

    @Test
    void aConfigurationWithAReadTimeOrSeveralKeyRangesIsRefused() {
        assertThatThrownBy(
                        () ->
                                format(
                                        builder ->
                                                builder.kind("K")
                                                        .splitCount(1)
                                                        .readTime(Instant.now())))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("carries no read time");
        for (UnaryOperator<DatastoreSourceBuilder<RowData>> severalRanges :
                List.<UnaryOperator<DatastoreSourceBuilder<RowData>>>of(
                        builder -> builder.kind("K"), builder -> builder.kind("K").splitCount(2))) {
            assertThatThrownBy(() -> format(severalRanges))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("reads one key range");
        }
    }

    /**
     * The production page reader against the emulator, which fails every page after the first few,
     * or refuses its credentials. The counts are static because each split reads through its own
     * deserialized copy; the tests of this class run one at a time.
     */
    private static final class FailingPageReader implements QueryPageReader {

        private static final long serialVersionUID = 1L;

        static final AtomicInteger PAGES = new AtomicInteger();
        static final AtomicInteger CLOSES = new AtomicInteger();

        private final ClientQueryPageReader delegate;
        private final int freePages;
        private final boolean refuseCredentials;

        FailingPageReader(String emulatorEndpoint, int freePages, boolean refuseCredentials) {
            this.delegate =
                    new ClientQueryPageReader(EmulatorEndpoint.parse(emulatorEndpoint, "endpoint"));
            this.freePages = freePages;
            this.refuseCredentials = refuseCredentials;
        }

        static void reset() {
            PAGES.set(0);
            CLOSES.set(0);
        }

        @Override
        public QueryResultBatch read(DatabaseDestination database, RunQueryRequest page)
                throws IOException {
            if (PAGES.incrementAndGet() > freePages) {
                throw new IOException("scripted page failure");
            }
            return delegate.read(database, page);
        }

        @Override
        public void useCredentials(@Nullable Credentials credentials) {
            if (refuseCredentials) {
                throw new IllegalStateException("scripted credential failure");
            }
            delegate.useCredentials(credentials);
        }

        @Override
        public void close() throws IOException {
            CLOSES.incrementAndGet();
            delegate.close();
        }
    }
}
