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

package io.github.flink.gcp.connector.firestore.source.batch.reader;

import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsRemoval;

import com.google.cloud.Timestamp;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.Query;
import com.google.firestore.v1.StructuredQuery;
import io.github.flink.gcp.connector.firestore.FirestoreMetricNames;
import io.github.flink.gcp.connector.firestore.TestDocuments;
import io.github.flink.gcp.connector.firestore.source.TestSources;
import io.github.flink.gcp.connector.firestore.source.batch.FetchedDocument;
import io.github.flink.gcp.connector.firestore.source.batch.QuerySplit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Paging a split: one request per fetch, continued after the last document handed over. */
@Timeout(30)
class FirestoreSplitReaderTest {

    private static final Timestamp READ_TIME = Timestamp.ofTimeSecondsAndNanos(1_700_000_000L, 0);
    private static final List<String> SEVEN =
            List.of("c/a", "c/b", "c/c", "c/d", "c/e", "c/f", "c/g");

    private static Firestore client;

    private final TestReaderMetrics metrics = new TestReaderMetrics();

    @BeforeAll
    static void openClient() {
        client = TestDocuments.offlineClient("p");
    }

    @AfterAll
    static void closeClient() throws Exception {
        client.close();
    }

    private FirestoreSplitReader reader(ScriptedQueryPageReader pages, int pageSize) {
        return new FirestoreSplitReader(TestSources.DATABASE, pages, pageSize, metrics.metrics());
    }

    private static QuerySplit split(String id, Query query) {
        return new QuerySplit(id, query.toProto(), READ_TIME);
    }

    /** Fetches until the split finishes, and returns the paths read and the fetches it took. */
    private static List<String> readAll(FirestoreSplitReader reader, String splitId, int[] fetches)
            throws IOException {
        List<String> read = new ArrayList<>();
        Set<String> finished;
        do {
            RecordsWithSplitIds<FetchedDocument> batch = reader.fetch();
            fetches[0]++;
            for (String id = batch.nextSplit(); id != null; id = batch.nextSplit()) {
                for (FetchedDocument document = batch.nextRecordFromSplit();
                        document != null;
                        document = batch.nextRecordFromSplit()) {
                    read.add(document.getDocument().getReference().getPath());
                }
            }
            finished = batch.finishedSplits();
        } while (!finished.contains(splitId) && fetches[0] < 100);
        return read;
    }

    @Test
    void readsASplitInPagesEachContinuedAfterTheLastDocument() throws IOException {
        ScriptedQueryPageReader pages = new ScriptedQueryPageReader(client, SEVEN);
        FirestoreSplitReader reader = reader(pages, 3);
        reader.handleSplitsChanges(
                new SplitsAddition<>(List.of(split("0", client.collection("c")))));

        int[] fetches = {0};
        assertThat(readAll(reader, "0", fetches)).containsExactlyElementsOf(SEVEN);

        // Pages of three: 3, 3, then a short page of 1 that ends the split without a fourth
        // request.
        assertThat(fetches[0]).isEqualTo(3);
        assertThat(pages.pages()).extracting(page -> page.getLimit().getValue()).containsOnly(3);
        assertThat(pages.pages().get(0).hasStartAt()).isFalse();
        assertThat(pages.pages().get(1).getStartAt().getBefore()).isFalse();
        assertThat(pages.pages().get(1).getStartAt().getValues(0).getReferenceValue())
                .endsWith("/documents/c/c");
        assertThat(metrics.counter(FirestoreMetricNames.DOCUMENTS_READ)).isEqualTo(7);
        assertThat(pages.readTimes()).hasSize(3).containsOnly(READ_TIME);
    }

    @Test
    void cutsAPageThatCameBackLongerThanItsLimit() throws IOException {
        // The client library's mid-stream retry keeps the page's limit, so a retried page can hold
        // more; emitting the excess would break a query's own limit, and its documents would be
        // read again by the next page.
        ScriptedQueryPageReader pages = new ScriptedQueryPageReader(client, SEVEN).overfilling(2);
        FirestoreSplitReader reader = reader(pages, 3);
        reader.handleSplitsChanges(
                new SplitsAddition<>(List.of(split("0", client.collection("c").limit(4)))));

        int[] fetches = {0};
        assertThat(readAll(reader, "0", fetches)).containsExactlyElementsOf(SEVEN.subList(0, 4));
        assertThat(metrics.counter(FirestoreMetricNames.DOCUMENTS_READ)).isEqualTo(4);
    }

    @Test
    void aFullLastPageTakesOneMoreRequestToFindTheEnd() throws IOException {
        ScriptedQueryPageReader pages = new ScriptedQueryPageReader(client, SEVEN.subList(0, 6));
        FirestoreSplitReader reader = reader(pages, 3);
        reader.handleSplitsChanges(
                new SplitsAddition<>(List.of(split("0", client.collection("c")))));

        int[] fetches = {0};
        assertThat(readAll(reader, "0", fetches)).hasSize(6);
        assertThat(pages.pages()).hasSize(3);
    }

    @Test
    void stopsAtTheQuerysOwnLimitWithoutAnotherRequest() throws IOException {
        ScriptedQueryPageReader pages = new ScriptedQueryPageReader(client, SEVEN);
        FirestoreSplitReader reader = reader(pages, 2);
        reader.handleSplitsChanges(
                new SplitsAddition<>(List.of(split("0", client.collection("c").limit(5)))));

        int[] fetches = {0};
        assertThat(readAll(reader, "0", fetches)).containsExactlyElementsOf(SEVEN.subList(0, 5));
        assertThat(pages.pages())
                .extracting(page -> page.getLimit().getValue())
                .containsExactly(2, 2, 1);
    }

    @Test
    void appliesTheOffsetToTheFirstPageOnly() throws IOException {
        ScriptedQueryPageReader pages = new ScriptedQueryPageReader(client, SEVEN);
        FirestoreSplitReader reader = reader(pages, 3);
        reader.handleSplitsChanges(
                new SplitsAddition<>(List.of(split("0", client.collection("c").offset(2)))));

        readAll(reader, "0", new int[] {0});

        assertThat(pages.pages())
                .extracting(StructuredQuery::getOffset)
                .startsWith(2)
                .containsOnlyOnce(2);
    }

    @Test
    void finishesAnExhaustedSplitWithoutARequest() throws IOException {
        ScriptedQueryPageReader pages = new ScriptedQueryPageReader(client, SEVEN);
        FirestoreSplitReader reader = reader(pages, 3);
        reader.handleSplitsChanges(
                new SplitsAddition<>(List.of(split("0", client.collection("c").limit(0)))));

        assertThat(reader.fetch().finishedSplits()).containsExactly("0");
        assertThat(pages.pages()).isEmpty();
    }

    @Test
    void namesTheSplitAndTheReadTimeWhenAPageFails() {
        ScriptedQueryPageReader pages = new ScriptedQueryPageReader(client, SEVEN).failingPage(0);
        FirestoreSplitReader reader = reader(pages, 3);
        reader.handleSplitsChanges(
                new SplitsAddition<>(List.of(split("4", client.collection("c")))));

        assertThatThrownBy(reader::fetch)
                .isInstanceOf(IOException.class)
                .hasMessageContaining("split 4")
                .hasMessageContaining(READ_TIME.toString())
                .hasRootCauseMessage("scripted failure");
    }

    @Test
    void forgetsARemovedSplit() throws IOException {
        ScriptedQueryPageReader pages = new ScriptedQueryPageReader(client, SEVEN);
        FirestoreSplitReader reader = reader(pages, 3);
        QuerySplit first = split("0", client.collection("c"));
        reader.handleSplitsChanges(new SplitsAddition<>(List.of(first)));
        reader.fetch();

        reader.handleSplitsChanges(new SplitsRemoval<>(List.of(first)));

        assertThat(reader.fetch().nextSplit()).isNull();
        assertThat(pages.pages()).hasSize(1);
    }

    @Test
    void closingTheSplitReaderLeavesTheSharedPageReaderOpen() throws IOException {
        ScriptedQueryPageReader pages = new ScriptedQueryPageReader(client, SEVEN);
        FirestoreSplitReader reader = reader(pages, 3);
        reader.handleSplitsChanges(
                new SplitsAddition<>(List.of(split("0", client.collection("c")))));
        reader.fetch();

        reader.close();

        assertThat(pages.closeCalls()).isZero();
    }

    @Test
    void readsQueuedSplitsOneAfterAnother() throws IOException {
        ScriptedQueryPageReader pages = new ScriptedQueryPageReader(client, SEVEN);
        FirestoreSplitReader reader = reader(pages, 10);
        reader.handleSplitsChanges(
                new SplitsAddition<>(
                        List.of(
                                split("0", client.collection("c").limit(2)),
                                split("1", client.collection("c").limit(1)))));

        assertThat(readAll(reader, "0", new int[] {0})).containsExactly("c/a", "c/b");
        assertThat(readAll(reader, "1", new int[] {0})).containsExactly("c/a");
    }
}
