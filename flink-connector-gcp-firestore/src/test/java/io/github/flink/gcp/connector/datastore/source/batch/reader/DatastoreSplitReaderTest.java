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

package io.github.flink.gcp.connector.datastore.source.batch.reader;

import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsRemoval;

import com.google.cloud.Timestamp;
import com.google.datastore.v1.Query;
import com.google.datastore.v1.QueryResultBatch;
import com.google.datastore.v1.RunQueryRequest;
import com.google.protobuf.ByteString;
import com.google.protobuf.Int32Value;
import io.github.flink.gcp.connector.datastore.DatastoreMetricNames;
import io.github.flink.gcp.connector.datastore.source.TestSources;
import io.github.flink.gcp.connector.datastore.source.batch.FetchedEntity;
import io.github.flink.gcp.connector.datastore.source.batch.QuerySplit;
import io.github.flink.gcp.connector.datastore.source.batch.enumerator.ScriptedQueryPlanner;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.UnaryOperator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Paging a split: one call per fetch, started at the cursor the last page ended at. */
@Timeout(30)
class DatastoreSplitReaderTest {

    private static final Timestamp READ_TIME = Timestamp.ofTimeSecondsAndNanos(1_700_000_000L, 0);
    private static final List<String> SEVEN = List.of("a", "b", "c", "d", "e", "f", "g");

    private final TestReaderMetrics metrics = new TestReaderMetrics();

    private DatastoreSplitReader reader(ScriptedQueryPageReader pages, int pageSize) {
        return new DatastoreSplitReader(TestSources.DATABASE, pages, pageSize, metrics.metrics());
    }

    private static QuerySplit split(String id, UnaryOperator<Query.Builder> query) {
        RunQueryRequest request = ScriptedQueryPlanner.request("K");
        return new QuerySplit(
                id,
                request.toBuilder().setQuery(query.apply(request.getQuery().toBuilder())).build(),
                READ_TIME);
    }

    private static QuerySplit split(String id) {
        return split(id, UnaryOperator.identity());
    }

    /** Fetches until the split finishes, and returns the names read and the fetches it took. */
    private static List<String> readAll(DatastoreSplitReader reader, String splitId, int[] fetches)
            throws IOException {
        List<String> read = new ArrayList<>();
        Set<String> finished;
        do {
            RecordsWithSplitIds<FetchedEntity> batch = reader.fetch();
            fetches[0]++;
            for (String id = batch.nextSplit(); id != null; id = batch.nextSplit()) {
                for (FetchedEntity entity = batch.nextRecordFromSplit();
                        entity != null;
                        entity = batch.nextRecordFromSplit()) {
                    read.add(entity.getEntity().getKey().getName());
                    assertThat(entity.getCursor())
                            .isEqualTo(
                                    ScriptedQueryPageReader.cursorAfter(
                                            entity.getEntity().getKey().getName()));
                }
            }
            finished = batch.finishedSplits();
        } while (!finished.contains(splitId) && fetches[0] < 100);
        assertThat(finished).as("split %s finished within 100 fetches", splitId).contains(splitId);
        return read;
    }

    @Test
    void readsASplitInPagesEachStartedWhereTheLastEnded() throws IOException {
        ScriptedQueryPageReader pages = new ScriptedQueryPageReader(SEVEN);
        DatastoreSplitReader reader = reader(pages, 3);
        reader.handleSplitsChanges(new SplitsAddition<>(List.of(split("0"))));

        int[] fetches = {0};
        assertThat(readAll(reader, "0", fetches)).containsExactlyElementsOf(SEVEN);

        // Pages of three: 3, 3, then 1, which the service answers with no more results.
        assertThat(fetches[0]).isEqualTo(3);
        assertThat(pages.pages())
                .extracting(page -> page.getQuery().getLimit().getValue())
                .containsOnly(3);
        assertThat(pages.pages().get(0).getQuery().getStartCursor()).isEmpty();
        assertThat(pages.pages().get(1).getQuery().getStartCursor())
                .isEqualTo(ScriptedQueryPageReader.cursorAfter("c"));
        assertThat(pages.pages())
                .extracting(page -> page.getReadOptions().getReadTime())
                .containsOnly(READ_TIME.toProto());
        assertThat(pages.pages())
                .extracting(RunQueryRequest::getPartitionId)
                .containsOnly(ScriptedQueryPlanner.request("K").getPartitionId());
        assertThat(metrics.counter(DatastoreMetricNames.ENTITIES_READ)).isEqualTo(7);
    }

    @Test
    void aFullLastPageTakesOneMoreCallToFindTheEnd() throws IOException {
        ScriptedQueryPageReader pages = new ScriptedQueryPageReader(SEVEN.subList(0, 6));
        DatastoreSplitReader reader = reader(pages, 3);
        reader.handleSplitsChanges(new SplitsAddition<>(List.of(split("0"))));

        assertThat(readAll(reader, "0", new int[] {0})).hasSize(6);
        assertThat(pages.pages()).hasSize(3);
    }

    @Test
    void continuesABatchTheServiceCutShort() throws IOException {
        // The service may answer with fewer entities than the limit and say more remain.
        ScriptedQueryPageReader pages = new ScriptedQueryPageReader(SEVEN).answeringAtMost(2);
        DatastoreSplitReader reader = reader(pages, 5);
        reader.handleSplitsChanges(new SplitsAddition<>(List.of(split("0"))));

        assertThat(readAll(reader, "0", new int[] {0})).containsExactlyElementsOf(SEVEN);
        assertThat(pages.pages()).hasSize(4);
    }

    @Test
    void stopsAtTheQuerysOwnLimitWithoutAnotherCall() throws IOException {
        ScriptedQueryPageReader pages = new ScriptedQueryPageReader(SEVEN);
        DatastoreSplitReader reader = reader(pages, 2);
        reader.handleSplitsChanges(
                new SplitsAddition<>(List.of(split("0", q -> q.setLimit(Int32Value.of(5))))));

        assertThat(readAll(reader, "0", new int[] {0}))
                .containsExactlyElementsOf(SEVEN.subList(0, 5));
        assertThat(pages.pages())
                .extracting(page -> page.getQuery().getLimit().getValue())
                .containsExactly(2, 2, 1);
    }

    @Test
    void carriesWhatIsLeftOfTheOffsetUntilTheServiceHasSkippedIt() throws IOException {
        ScriptedQueryPageReader pages = new ScriptedQueryPageReader(SEVEN).skippingAtMost(2);
        DatastoreSplitReader reader = reader(pages, 3);
        reader.handleSplitsChanges(new SplitsAddition<>(List.of(split("0", q -> q.setOffset(3)))));

        assertThat(readAll(reader, "0", new int[] {0})).containsExactly("d", "e", "f", "g");
        assertThat(pages.pages())
                .extracting(page -> page.getQuery().getOffset())
                .containsExactly(3, 1, 0);
        assertThat(pages.pages().get(1).getQuery().getStartCursor())
                .isEqualTo(ScriptedQueryPageReader.cursorAfter("b"));
    }

    @Test
    void stopsAtTheQuerysOwnEndCursor() throws IOException {
        ScriptedQueryPageReader pages = new ScriptedQueryPageReader(SEVEN);
        DatastoreSplitReader reader = reader(pages, 2);
        reader.handleSplitsChanges(
                new SplitsAddition<>(
                        List.of(
                                split(
                                        "0",
                                        q ->
                                                q.setEndCursor(
                                                        ScriptedQueryPageReader.cursorAfter(
                                                                "e"))))));

        // Pages of two: a b, c d, then e and the end cursor, which finishes the split.
        assertThat(readAll(reader, "0", new int[] {0})).containsExactly("a", "b", "c", "d", "e");
        assertThat(pages.pages()).hasSize(3);
    }

    @Test
    void continuesABatchWithNoEntityWhoseCursorMoved() throws IOException {
        // The service may scan entries a filter rejects and answer with none, more to come.
        ScriptedQueryPageReader pages = new ScriptedQueryPageReader(SEVEN).emptyFirstBatch();
        DatastoreSplitReader reader = reader(pages, 10);
        reader.handleSplitsChanges(new SplitsAddition<>(List.of(split("0"))));

        assertThat(readAll(reader, "0", new int[] {0})).containsExactlyElementsOf(SEVEN);
        assertThat(pages.pages().get(1).getQuery().getStartCursor())
                .isEqualTo(ScriptedQueryPageReader.BEFORE_EVERY_NAME);
    }

    @Test
    void continuesAPartialSkipFromTheSkippedCursorWhenTheEndCursorIsUnset() throws IOException {
        ScriptedQueryPageReader pages =
                new ScriptedQueryPageReader(SEVEN).skippingAtMost(2).skippedCursorOnly();
        DatastoreSplitReader reader = reader(pages, 3);
        reader.handleSplitsChanges(new SplitsAddition<>(List.of(split("0", q -> q.setOffset(3)))));

        assertThat(readAll(reader, "0", new int[] {0})).containsExactly("d", "e", "f", "g");
        assertThat(pages.pages().get(1).getQuery().getStartCursor())
                .isEqualTo(ScriptedQueryPageReader.cursorAfter("b"));
    }

    @Test
    void closingTheSplitReaderLeavesTheSharedPageReaderOpen() throws IOException {
        ScriptedQueryPageReader pages = new ScriptedQueryPageReader(SEVEN);
        DatastoreSplitReader reader = reader(pages, 3);
        reader.handleSplitsChanges(new SplitsAddition<>(List.of(split("0"))));
        reader.fetch();

        reader.close();

        assertThat(pages.closeCalls()).isZero();
    }

    @Test
    void resumesARestoredSplitAtItsOwnStartCursor() throws IOException {
        ScriptedQueryPageReader pages = new ScriptedQueryPageReader(SEVEN);
        DatastoreSplitReader reader = reader(pages, 10);
        reader.handleSplitsChanges(
                new SplitsAddition<>(
                        List.of(
                                split(
                                        "0",
                                        q ->
                                                q.setStartCursor(
                                                        ScriptedQueryPageReader.cursorAfter(
                                                                "e"))))));

        assertThat(readAll(reader, "0", new int[] {0})).containsExactly("f", "g");
    }

    @Test
    void finishesWhenTheQuerysLimitIsSpentWhateverTheServiceSays() throws IOException {
        // A batch that fills the query's limit but says NOT_FINISHED must not lead to a page
        // asking for no entity.
        ScriptedQueryPageReader pages =
                new ScriptedQueryPageReader(SEVEN)
                        .answeringWith(
                                QueryResultBatch.newBuilder()
                                        .addEntityResults(result("a"))
                                        .addEntityResults(result("b"))
                                        .setEndCursor(ScriptedQueryPageReader.cursorAfter("b"))
                                        .setMoreResults(
                                                QueryResultBatch.MoreResultsType.NOT_FINISHED)
                                        .build());
        DatastoreSplitReader reader = reader(pages, 10);
        reader.handleSplitsChanges(
                new SplitsAddition<>(List.of(split("0", q -> q.setLimit(Int32Value.of(2))))));

        assertThat(readAll(reader, "0", new int[] {0})).containsExactly("a", "b");
        assertThat(pages.pages()).hasSize(1);
    }

    @Test
    void readsIndexValuesBackOnlyForAProjection() throws IOException {
        // As the client library does: a ProjectionEntity only for a query that projects.
        com.google.datastore.v1.Value micros =
                com.google.datastore.v1.Value.newBuilder()
                        .setIntegerValue(5L)
                        .setMeaning(ProjectedValues.INDEX_VALUE)
                        .build();
        QueryResultBatch batch =
                QueryResultBatch.newBuilder()
                        .addEntityResults(
                                result("a").toBuilder()
                                        .setEntity(
                                                result("a").getEntity().toBuilder()
                                                        .putProperties("ts", micros)))
                        .setEndCursor(ScriptedQueryPageReader.cursorAfter("a"))
                        .setMoreResults(QueryResultBatch.MoreResultsType.NO_MORE_RESULTS)
                        .build();
        DatastoreSplitReader whole =
                reader(new ScriptedQueryPageReader(SEVEN).answeringWith(batch), 10);
        DatastoreSplitReader projecting =
                reader(new ScriptedQueryPageReader(SEVEN).answeringWith(batch), 10);
        whole.handleSplitsChanges(new SplitsAddition<>(List.of(split("0"))));
        projecting.handleSplitsChanges(
                new SplitsAddition<>(
                        List.of(
                                split(
                                        "0",
                                        q ->
                                                q.addProjection(
                                                        com.google.datastore.v1.Projection
                                                                .newBuilder()
                                                                .setProperty(
                                                                        com.google.datastore.v1
                                                                                .PropertyReference
                                                                                .newBuilder()
                                                                                .setName(
                                                                                        "ts")))))));

        assertThat(firstEntity(whole).getLong("ts")).isEqualTo(5L);
        assertThat(firstEntity(projecting).getTimestamp("ts"))
                .isEqualTo(com.google.cloud.Timestamp.ofTimeMicroseconds(5L));
    }

    private static com.google.cloud.datastore.Entity firstEntity(DatastoreSplitReader reader)
            throws IOException {
        RecordsWithSplitIds<FetchedEntity> records = reader.fetch();
        records.nextSplit();
        return records.nextRecordFromSplit().getEntity();
    }

    @Test
    void refusesAProjectionResultWithoutAKey() {
        // The API allows one; an Entity cannot hold it.
        ScriptedQueryPageReader pages =
                new ScriptedQueryPageReader(SEVEN)
                        .answeringWith(
                                QueryResultBatch.newBuilder()
                                        .addEntityResults(
                                                com.google.datastore.v1.EntityResult.newBuilder()
                                                        .setEntity(
                                                                com.google.datastore.v1.Entity
                                                                        .newBuilder()
                                                                        .putProperties(
                                                                                "n",
                                                                                com.google.datastore
                                                                                        .v1.Value
                                                                                        .newBuilder()
                                                                                        .setIntegerValue(
                                                                                                1)
                                                                                        .build()))
                                                        .setCursor(
                                                                ScriptedQueryPageReader.cursorAfter(
                                                                        "a")))
                                        .setEndCursor(ScriptedQueryPageReader.cursorAfter("a"))
                                        .setMoreResults(
                                                QueryResultBatch.MoreResultsType.NO_MORE_RESULTS)
                                        .build());
        DatastoreSplitReader reader = reader(pages, 10);
        reader.handleSplitsChanges(new SplitsAddition<>(List.of(split("3"))));

        assertThatThrownBy(reader::fetch)
                .isInstanceOf(IOException.class)
                .hasMessageContaining("split 3")
                .hasMessageContaining("projection result that has no key");
    }

    private static com.google.datastore.v1.EntityResult result(String name) {
        return com.google.datastore.v1.EntityResult.newBuilder()
                .setEntity(
                        com.google.datastore.v1.Entity.newBuilder()
                                .setKey(
                                        com.google.datastore.v1.Key.newBuilder()
                                                .addPath(
                                                        com.google.datastore.v1.Key.PathElement
                                                                .newBuilder()
                                                                .setKind("K")
                                                                .setName(name))))
                .setCursor(ScriptedQueryPageReader.cursorAfter(name))
                .build();
    }

    @Test
    void finishesAnExhaustedSplitWithoutACall() throws IOException {
        ScriptedQueryPageReader pages = new ScriptedQueryPageReader(SEVEN);
        DatastoreSplitReader reader = reader(pages, 3);
        reader.handleSplitsChanges(
                new SplitsAddition<>(List.of(split("0", q -> q.setLimit(Int32Value.of(0))))));

        assertThat(reader.fetch().finishedSplits()).containsExactly("0");
        assertThat(pages.pages()).isEmpty();
    }

    @Test
    void namesTheSplitAndTheReadTimeWhenAPageFails() {
        ScriptedQueryPageReader pages = new ScriptedQueryPageReader(SEVEN).failingPage(0);
        DatastoreSplitReader reader = reader(pages, 3);
        reader.handleSplitsChanges(new SplitsAddition<>(List.of(split("4"))));

        assertThatThrownBy(reader::fetch)
                .isInstanceOf(IOException.class)
                .hasMessageContaining("split 4")
                .hasMessageContaining(READ_TIME.toString())
                .hasRootCauseMessage("scripted failure");
    }

    @Test
    void refusesAPageLongerThanItsLimit() {
        // Emitting the excess would break a query's own limit, and a checkpoint taken after it
        // would count entities the next page reads again.
        ScriptedQueryPageReader pages = new ScriptedQueryPageReader(SEVEN).overfilling(1);
        DatastoreSplitReader reader = reader(pages, 3);
        reader.handleSplitsChanges(new SplitsAddition<>(List.of(split("0"))));

        assertThatThrownBy(reader::fetch)
                .isInstanceOf(IOException.class)
                .hasMessageContaining("limited to 3 entities with 4");
    }

    @Test
    void refusesABatchThatSaysMoreRemainButDidNotMove() {
        // Without the check a split would ask for the same page for ever.
        ScriptedQueryPageReader pages =
                new ScriptedQueryPageReader(SEVEN)
                        .answeringWith(
                                QueryResultBatch.newBuilder()
                                        .setMoreResults(
                                                QueryResultBatch.MoreResultsType.NOT_FINISHED)
                                        .setEndCursor(ByteString.copyFromUtf8("x"))
                                        .build());
        DatastoreSplitReader reader = reader(pages, 3);
        reader.handleSplitsChanges(
                new SplitsAddition<>(
                        List.of(split("0", q -> q.setStartCursor(ByteString.copyFromUtf8("x"))))));

        assertThatThrownBy(reader::fetch)
                .isInstanceOf(IOException.class)
                .hasMessageContaining("no progress");
    }

    @Test
    void refusesABatchThatSaysMoreRemainWithoutACursor() {
        ScriptedQueryPageReader pages =
                new ScriptedQueryPageReader(SEVEN)
                        .answeringWith(
                                QueryResultBatch.newBuilder()
                                        .setMoreResults(
                                                QueryResultBatch.MoreResultsType.NOT_FINISHED)
                                        .build());
        DatastoreSplitReader reader = reader(pages, 3);
        reader.handleSplitsChanges(new SplitsAddition<>(List.of(split("0"))));

        assertThatThrownBy(reader::fetch)
                .isInstanceOf(IOException.class)
                .hasMessageContaining("no end cursor");
    }

    @Test
    void refusesAnUnknownMoreResultsState() {
        ScriptedQueryPageReader pages =
                new ScriptedQueryPageReader(SEVEN)
                        .answeringWith(QueryResultBatch.newBuilder().build());
        DatastoreSplitReader reader = reader(pages, 3);
        reader.handleSplitsChanges(new SplitsAddition<>(List.of(split("0"))));

        assertThatThrownBy(reader::fetch)
                .isInstanceOf(IOException.class)
                .hasMessageContaining("unknown more-results state");
    }

    @Test
    void forgetsARemovedSplit() throws IOException {
        ScriptedQueryPageReader pages = new ScriptedQueryPageReader(SEVEN);
        DatastoreSplitReader reader = reader(pages, 3);
        QuerySplit first = split("0");
        reader.handleSplitsChanges(new SplitsAddition<>(List.of(first)));
        reader.fetch();

        reader.handleSplitsChanges(new SplitsRemoval<>(List.of(first)));

        assertThat(reader.fetch().nextSplit()).isNull();
        assertThat(pages.pages()).hasSize(1);
    }

    @Test
    void readsQueuedSplitsOneAfterAnother() throws IOException {
        ScriptedQueryPageReader pages = new ScriptedQueryPageReader(SEVEN);
        DatastoreSplitReader reader = reader(pages, 10);
        reader.handleSplitsChanges(
                new SplitsAddition<>(
                        List.of(
                                split("0", q -> q.setLimit(Int32Value.of(2))),
                                split("1", q -> q.setLimit(Int32Value.of(1))))));

        assertThat(readAll(reader, "0", new int[] {0})).containsExactly("a", "b");
        assertThat(readAll(reader, "1", new int[] {0})).containsExactly("a");
    }
}
