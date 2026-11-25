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

package io.github.flink.gcp.connector.firestore.common.table;

import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordsBySplits;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.util.Collector;

import com.google.cloud.Timestamp;
import io.github.flink.gcp.connector.base.rpc.GrpcEventLoops;
import io.github.flink.gcp.connector.testutils.LogCapture;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** The full-cache load's lifecycle over a scripted split reader. */
class FullCacheInputFormatTest {

    private static LogCapture captureLog() {
        return LogCapture.of(FullCacheInputFormat.class, LogCapture.Level.INFO);
    }

    @Test
    void plansTheLoadAsOneSplit() throws IOException {
        ScriptedFormat format = new ScriptedFormat(List.of("a"));

        try (LogCapture log = captureLog()) {
            Split[] splits = format.createInputSplits(4);

            assertThat(splits).hasSize(1);
            assertThat(log.getMessages())
                    .containsExactly(
                            "Planned a full lookup cache load of scripted table at read time "
                                    + Split.READ_TIME
                                    + ".");
        }
    }

    @Test
    void readsUntilEveryQueryFinishedAcrossFetchesThatReturnNothing() throws IOException {
        // Query "b" answers a fetch with nothing before its records, as a Datastore page that
        // moved its cursor without an entity does.
        ScriptedFormat format =
                new ScriptedFormat(
                        List.of("a", "b"),
                        batch("a", List.of("a1", "a2"), true),
                        batch(null, List.of(), false),
                        batch("b", List.of("b1"), false),
                        batch("b", List.of(), true));
        format.configure(new Configuration());
        List<String> rows = new ArrayList<>();

        try (LogCapture log = captureLog()) {
            format.open(new Split());
            assertThat(format.events).containsExactly("reader opened with [a, b]");
            assertThat(format.reachedEnd()).isFalse();
            // A row is handed back as soon as one fetch produced it, not after the whole load.
            assertThat(format.fetches).isEqualTo(1);
            while (!format.reachedEnd()) {
                rows.add(format.nextRecord(null).getString(0).toString());
            }
            format.close();

            assertThat(log.getMessages())
                    .containsExactly(
                            "Finished a full lookup cache load of scripted table at read time "
                                    + Split.READ_TIME
                                    + " after reading 3 records.");
        }
        assertThat(rows).containsExactly("a1", "a2", "b1");
        assertThat(format.fetches).isEqualTo(4);
        assertThat(format.events)
                .containsExactly("reader opened with [a, b]", "reader closed", "resource closed");
    }

    /**
     * Planning, opening, fetching and releasing a load each run inside {@link
     * GrpcEventLoops#interruptibleReleases()}, where a release of the connector's last gRPC lease
     * on an interrupted load stops waiting for the event-loop group after its first second: Flink
     * gives a load it interrupts 10 s to end.
     */
    @Test
    void everyPhaseOfALoadRunsWhereAnInterruptedReleaseCanStopWaiting() throws IOException {
        ScriptedFormat format = new ScriptedFormat(List.of("a"), batch("a", List.of("a1"), true));
        format.configure(new Configuration());

        Split[] splits = format.createInputSplits(1);
        format.open(splits[0]);
        while (!format.reachedEnd()) {
            format.nextRecord(null);
        }
        format.close();

        assertThat(format.scopes)
                .containsExactly(
                        Map.entry("plan", true),
                        Map.entry("open", true),
                        Map.entry("fetch", true),
                        Map.entry("reader close", true),
                        Map.entry("resource close", true));
        assertThat(GrpcEventLoops.inInterruptibleReleases())
                .as("the scope outlived the load's phases")
                .isFalse();
    }

    @Test
    void aFailedOpenReleasesWhatItOpenedAndIsStopped() {
        ScriptedFormat format = new ScriptedFormat(List.of("a"));
        format.failOpen = true;
        format.configure(new Configuration());

        try (LogCapture log = captureLog()) {
            assertThatThrownBy(() -> format.open(new Split()))
                    .isInstanceOf(IOException.class)
                    .hasMessage("scripted open failure");

            assertThat(log.getMessages()).singleElement().asString().startsWith("Stopped ");
        }
        // The failed open closed the format itself, without waiting for Flink's loader to.
        assertThat(format.events).containsExactly("resource closed");
    }

    @Test
    void aLoadWhoseQueriesCannotBeReadIsStopped() {
        ScriptedFormat format = new ScriptedFormat(List.of("a"));
        format.failQueries = true;
        format.configure(new Configuration());

        try (LogCapture log = captureLog()) {
            assertThatThrownBy(() -> format.open(new Split()))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("scripted query failure");

            // No query was registered, which an empty set of unfinished queries would otherwise
            // read as a finished load.
            assertThat(log.getMessages()).singleElement().asString().startsWith("Stopped ");
        }
        assertThat(format.events).isEmpty();
    }

    @Test
    void aReaderThatFailsToTakeItsQueriesIsStopped() {
        ScriptedFormat format = new ScriptedFormat(List.of("a"));
        format.failHandOff = true;
        format.configure(new Configuration());

        try (LogCapture log = captureLog()) {
            assertThatThrownBy(() -> format.open(new Split()))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("scripted hand-off failure");

            // The reader opened, but no query was read, so the load did not finish.
            assertThat(log.getMessages()).singleElement().asString().startsWith("Stopped ");
        }
        assertThat(format.events).containsExactly("reader closed", "resource closed");
    }

    @Test
    void aFailedPageStopsTheLoad() throws IOException {
        ScriptedFormat format = new ScriptedFormat(List.of("a"), batch("a", List.of("a1"), false));
        format.configure(new Configuration());

        try (LogCapture log = captureLog()) {
            format.open(new Split());
            assertThat(format.nextRecord(null).getString(0).toString()).isEqualTo("a1");
            // The script holds no second page: the next fetch fails, as a failed page would.
            assertThatThrownBy(format::reachedEnd)
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("fetch after the script ended");
            format.close();

            assertThat(log.getMessages())
                    .singleElement()
                    .asString()
                    .startsWith("Stopped ")
                    .endsWith(" after reading 1 records.");
        }
        assertThat(format.events)
                .containsExactly("reader opened with [a]", "reader closed", "resource closed");
    }

    @Test
    void refusesASplitItDidNotPlan() {
        ScriptedFormat format = new ScriptedFormat(List.of("a"));

        assertThatThrownBy(() -> format.open(new GenericInputSplit(0, 1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not a split of a full-cache load");
    }

    @Test
    void aReleaseFailureIsAnIOExceptionAndStillReleasesTheRest() throws IOException {
        // Flink's loader logs an IOException from close() and keeps the rows it read; an
        // unchecked one escaping its finally block would discard the load.
        ScriptedFormat format = new ScriptedFormat(List.of("a"), batch("a", List.of("a1"), true));
        format.failReaderClose = true;
        format.configure(new Configuration());
        format.open(new Split());
        while (!format.reachedEnd()) {
            format.nextRecord(null);
        }

        assertThatThrownBy(format::close)
                .isInstanceOf(IOException.class)
                .hasRootCauseInstanceOf(IllegalStateException.class)
                .hasRootCauseMessage("scripted close failure");
        assertThat(format.events)
                .containsExactly(
                        "reader opened with [a]", "reader close failed", "resource closed");
        format.close();
        assertThat(format.events).hasSize(3);
    }

    private static RecordsWithSplitIds<String> batch(
            String split, List<String> records, boolean finished) {
        RecordsBySplits.Builder<String> builder = new RecordsBySplits.Builder<>();
        for (String record : records) {
            builder.add(split, record);
        }
        if (finished) {
            builder.addFinishedSplit(split);
        }
        return builder.build();
    }

    private static final class Split extends FullCacheInputFormat.LoadSplit {

        private static final long serialVersionUID = 1L;

        static final Timestamp READ_TIME = Timestamp.ofTimeSecondsAndNanos(1_786_000_000L, 0);

        Split() {
            super(READ_TIME);
        }
    }

    private static final class Query implements SourceSplit {

        private final String id;

        Query(String id) {
            this.id = id;
        }

        @Override
        public String splitId() {
            return id;
        }
    }

    /**
     * Hands out the scripted batches in order, one per fetch, and records what is opened, handed
     * over and closed, in order.
     */
    private static final class ScriptedFormat extends FullCacheInputFormat<String, Query, Split> {

        private static final long serialVersionUID = 1L;

        private final List<String> queries;
        private final transient Deque<RecordsWithSplitIds<String>> batches;
        private final transient List<String> events = new ArrayList<>();

        /** Each phase, in order, with whether it ran inside an interruptible-releases scope. */
        private final transient Map<String, Boolean> scopes = new LinkedHashMap<>();

        private transient int fetches;
        private transient boolean failQueries;
        private transient boolean failOpen;
        private transient boolean failHandOff;
        private transient boolean failReaderClose;

        @SafeVarargs
        ScriptedFormat(List<String> queries, RecordsWithSplitIds<String>... batches) {
            super(Split.class, "records");
            this.queries = queries;
            this.batches = new ArrayDeque<>(List.of(batches));
        }

        @Override
        protected Split planLoad() {
            scopes.put("plan", GrpcEventLoops.inInterruptibleReleases());
            return new Split();
        }

        @Override
        protected SplitReader<String, Query> openReader() throws IOException {
            scopes.put("open", GrpcEventLoops.inInterruptibleReleases());
            // The page reader a subclass opens first, held before anything else can fail.
            closeWithLoad(
                    () -> {
                        scopes.put("resource close", GrpcEventLoops.inInterruptibleReleases());
                        events.add("resource closed");
                    });
            if (failOpen) {
                throw new IOException("scripted open failure");
            }
            return new SplitReader<String, Query>() {
                @Override
                public RecordsWithSplitIds<String> fetch() throws IOException {
                    scopes.put("fetch", GrpcEventLoops.inInterruptibleReleases());
                    fetches++;
                    RecordsWithSplitIds<String> batch = batches.poll();
                    if (batch == null) {
                        throw new IOException("fetch after the script ended");
                    }
                    return batch;
                }

                @Override
                public void handleSplitsChanges(SplitsChange<Query> splitsChanges) {
                    if (failHandOff) {
                        throw new IllegalStateException("scripted hand-off failure");
                    }
                    List<String> ids = new ArrayList<>();
                    for (Query query : splitsChanges.splits()) {
                        ids.add(query.splitId());
                    }
                    events.add("reader opened with " + ids);
                }

                @Override
                public void wakeUp() {}

                @Override
                public void close() {
                    if (failReaderClose) {
                        failReaderClose = false;
                        events.add("reader close failed");
                        throw new IllegalStateException("scripted close failure");
                    }
                    scopes.put("reader close", GrpcEventLoops.inInterruptibleReleases());
                    events.add("reader closed");
                }
            };
        }

        @Override
        protected List<Query> querySplits(Split split) {
            if (failQueries) {
                throw new IllegalArgumentException("scripted query failure");
            }
            List<Query> splits = new ArrayList<>();
            for (String id : queries) {
                splits.add(new Query(id));
            }
            return splits;
        }

        @Override
        protected void emit(String record, Collector<RowData> out) {
            out.collect(GenericRowData.of(StringData.fromString(record)));
        }

        @Override
        protected String describe(Split split) {
            return "scripted table";
        }
    }
}
