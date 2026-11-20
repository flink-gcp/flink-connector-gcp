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

package io.github.flink.gcp.connector.firestore.table.source;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.InstantiationUtil;

import com.google.auth.Credentials;
import com.google.cloud.Timestamp;
import com.google.cloud.firestore.DocumentSnapshot;
import com.google.cloud.firestore.FieldPath;
import com.google.cloud.firestore.Query;
import com.google.firestore.v1.RunQueryRequest;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.firestore.AbstractFirestoreEmulatorITCase;
import io.github.flink.gcp.connector.firestore.DatabaseDestination;
import io.github.flink.gcp.connector.firestore.source.FirestoreSource;
import io.github.flink.gcp.connector.firestore.source.FirestoreSourceBuilder;
import io.github.flink.gcp.connector.firestore.source.FixedPartitionsPlannerFactory;
import io.github.flink.gcp.connector.firestore.source.TestSources;
import io.github.flink.gcp.connector.firestore.source.batch.FirestoreBatchSource;
import io.github.flink.gcp.connector.firestore.source.batch.reader.ClientQueryPageReader;
import io.github.flink.gcp.connector.firestore.source.batch.reader.QueryPageReader;
import io.github.flink.gcp.connector.firestore.table.FirestoreTableSchema;
import io.github.flink.gcp.connector.firestore.table.TypeMismatchPolicy;
import io.github.flink.gcp.connector.firestore.table.source.FirestoreFullCacheInputFormat.LoadSplit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.UnaryOperator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The full lookup cache's loader against the emulator, driven as Flink's {@code
 * InputFormatCacheLoader} drives it: splits from the initial format, each read by its own copy.
 */
@Testcontainers
class FirestoreFullCacheInputFormatITCase extends AbstractFirestoreEmulatorITCase {

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
                                    DataTypes.FIELD("path", DataTypes.STRING().notNull()))
                            .getLogicalType();

    @AfterEach
    void forgetRecordings() {
        FailingPageReader.reset();
    }

    private static RowDataDeserializationSchema deserializer() {
        return new RowDataDeserializationSchema(
                FirestoreTableSchema.of(PHYSICAL, new int[] {0}, List.of(), List.of()),
                RowDataDeserializationSchema.topLevel(0, 1),
                List.of("document-path"),
                TypeMismatchPolicy.FAIL,
                InternalTypeInfo.of(PRODUCED));
    }

    private static FirestoreFullCacheInputFormat format(
            UnaryOperator<FirestoreSourceBuilder<RowData>> customizer) {
        FirestoreSourceBuilder<RowData> builder =
                FirestoreSource.<RowData>builder()
                        .database(database())
                        .deserializer(deserializer())
                        .emulatorEndpoint(emulatorEndpoint());
        return new FirestoreFullCacheInputFormat(
                ((FirestoreBatchSource<RowData>) customizer.apply(builder).build()).getConfig());
    }

    private static FirestoreFullCacheInputFormat collectionFormat(String collection, int pageSize) {
        return format(
                builder ->
                        builder.query(
                                        new FirestoreCollectionQueryFactory(
                                                collection, new String[] {"n"}, List.of(), null))
                                .pageSize(pageSize));
    }

    /** One load: every split of the initial format, each read to its end by its own copy. */
    private static List<String> load(FirestoreFullCacheInputFormat initial, List<LoadSplit> splits)
            throws Exception {
        LoadSplit[] planned = initial.createInputSplits(1);
        List<String> rows = new ArrayList<>();
        for (LoadSplit split : planned) {
            splits.add(split);
            rows.addAll(read(initial, split));
        }
        return rows;
    }

    /** Reads one split to its end in a copy of the initial format, as Flink's loader does. */
    private static List<String> read(FirestoreFullCacheInputFormat initial, LoadSplit split)
            throws Exception {
        List<String> rows = new ArrayList<>();
        FirestoreFullCacheInputFormat copy = InstantiationUtil.clone(initial);
        copy.configure(new Configuration());
        copy.open(split);
        try {
            while (!copy.reachedEnd()) {
                RowData row = copy.nextRecord(null);
                rows.add(
                        row.getString(2)
                                + "="
                                + (row.isNullAt(1) ? "null" : String.valueOf(row.getLong(1))));
            }
        } finally {
            copy.close();
        }
        return rows;
    }

    @Test
    void oneSplitReadsEveryPlannedPartitionOfACollectionGroup() throws Exception {
        String group = uniqueCollection();
        client().document("a/1/" + group + "/x").set(Map.of("n", 1L)).get();
        client().document("b/1/" + group + "/y").set(Map.of("n", 2L)).get();
        client().document("c/1/" + group + "/z").set(Map.of("n", 3L)).get();
        FirestoreFullCacheInputFormat initial =
                format(
                        builder ->
                                TestSources.withPlannerFactory(
                                        builder.collectionGroup(group)
                                                .select(FieldPath.of("n"))
                                                .pageSize(1),
                                        new FixedPartitionsPlannerFactory(
                                                emulatorEndpoint(),
                                                List.of("b/1/" + group + "/y"))));
        List<LoadSplit> splits = new ArrayList<>();

        // A page of one document makes every partition take several requests.
        assertThat(load(initial, splits))
                .containsExactlyInAnyOrder(
                        "a/1/" + group + "/x=1", "b/1/" + group + "/y=2", "c/1/" + group + "/z=3");
        // Flink copies every split's rows through one shared serializer on parallel threads,
        // so the partitions are read in one split.
        assertThat(splits).hasSize(1);
        assertThat(splits.get(0).queryCount()).isEqualTo(2);
    }

    @Test
    void aLoadReadsTheSnapshotOfItsPlanningNotOfItsRead() throws Exception {
        String collection = uniqueCollection();
        client().document(collection + "/a").set(Map.of("n", 1L)).get();
        FirestoreFullCacheInputFormat initial = collectionFormat(collection, 10);
        LoadSplit split = initial.createInputSplits(1)[0];
        client().document(collection + "/a").set(Map.of("n", 10L)).get();
        client().document(collection + "/b").set(Map.of("n", 2L)).get();

        assertThat(read(initial, split)).containsExactly(collection + "/a=1");
    }

    @Test
    void aFailedOpenReleasesThePageReaderItTook() throws Exception {
        FirestoreFullCacheInputFormat initial =
                format(
                        builder ->
                                TestSources.withPageReader(
                                        builder.query(
                                                new FirestoreCollectionQueryFactory(
                                                        uniqueCollection(),
                                                        new String[] {"n"},
                                                        List.of(),
                                                        null)),
                                        new FailingPageReader(emulatorEndpoint(), 0, true)));
        LoadSplit split = initial.createInputSplits(1)[0];
        FirestoreFullCacheInputFormat copy = InstantiationUtil.clone(initial);

        assertThatThrownBy(() -> copy.open(split))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("scripted credential failure");
        // Flink's loader closes the format after a failed open too; nothing is released twice.
        copy.close();

        assertThat(FailingPageReader.CLOSES.get()).isEqualTo(1);
    }

    @Test
    void everyLoadPlansAgainAndReadsANewerSnapshot() throws Exception {
        String collection = uniqueCollection();
        client().document(collection + "/a").set(Map.of("n", 1L)).get();
        client().document(collection + "/b").set(Map.of("n", 2L)).get();
        FirestoreFullCacheInputFormat initial = collectionFormat(collection, 1);
        List<LoadSplit> first = new ArrayList<>();
        List<LoadSplit> second = new ArrayList<>();

        assertThat(load(initial, first))
                .containsExactlyInAnyOrder(collection + "/a=1", collection + "/b=2");
        client().document(collection + "/a").set(Map.of("n", 10L)).get();
        client().document(collection + "/c").set(Map.of("n", 3L)).get();

        assertThat(load(initial, second))
                .containsExactlyInAnyOrder(
                        collection + "/a=10", collection + "/b=2", collection + "/c=3");
        assertThat(first).hasSize(1);
        assertThat(second).hasSize(1);
        assertThat(second.get(0).getReadTime()).isGreaterThan(first.get(0).getReadTime());
    }

    @Test
    void anEmptyCollectionLoadsNoRowsFromOneSplit() throws Exception {
        List<LoadSplit> splits = new ArrayList<>();

        assertThat(load(collectionFormat(uniqueCollection(), 10), splits)).isEmpty();
        assertThat(splits).hasSize(1);
    }

    @Test
    void aFailedPageFailsTheLoadAndCloseStillReleasesThePageReader() throws Exception {
        String collection = uniqueCollection();
        client().document(collection + "/a").set(Map.of("n", 1L)).get();
        client().document(collection + "/b").set(Map.of("n", 2L)).get();
        FirestoreFullCacheInputFormat initial =
                format(
                        builder ->
                                TestSources.withPageReader(
                                        builder.query(
                                                        new FirestoreCollectionQueryFactory(
                                                                collection,
                                                                new String[] {"n"},
                                                                List.of(),
                                                                null))
                                                .pageSize(1),
                                        new FailingPageReader(emulatorEndpoint(), 1, false)));
        LoadSplit split = initial.createInputSplits(1)[0];
        FirestoreFullCacheInputFormat copy = InstantiationUtil.clone(initial);
        copy.open(split);

        assertThat(copy.reachedEnd()).isFalse();
        assertThat(copy.nextRecord(null).getString(2).toString()).isEqualTo(collection + "/a");
        assertThatThrownBy(copy::reachedEnd)
                .isInstanceOf(IOException.class)
                .hasStackTraceContaining("scripted page failure");
        // Flink's loader closes the format in a finally block after any failure.
        copy.close();
        copy.close();

        assertThat(FailingPageReader.CLOSES.get()).isEqualTo(1);
    }

    @Test
    void aConfigurationWithAReadTimeIsRefused() {
        assertThatThrownBy(
                        () ->
                                format(
                                        builder ->
                                                builder.query(
                                                                new FirestoreCollectionQueryFactory(
                                                                        "c",
                                                                        new String[0],
                                                                        List.of(),
                                                                        null))
                                                        .readTime(java.time.Instant.now())))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("carries no read time");
    }

    /**
     * The production page reader against the emulator, which fails every page after the first few,
     * or refuses its credentials. The counts are static because each split reads through its own
     * deserialized copy.
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
        public Query query(DatabaseDestination database, RunQueryRequest request)
                throws IOException {
            return delegate.query(database, request);
        }

        @Override
        public List<? extends DocumentSnapshot> read(Query page, Timestamp readTime)
                throws IOException {
            if (PAGES.incrementAndGet() > freePages) {
                throw new IOException("scripted page failure");
            }
            return delegate.read(page, readTime);
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
