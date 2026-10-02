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

import com.google.auth.Credentials;
import com.google.cloud.Timestamp;
import com.google.cloud.firestore.DocumentSnapshot;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.Query;
import com.google.firestore.v1.RunQueryRequest;
import com.google.firestore.v1.StructuredQuery;
import com.google.firestore.v1.Value;
import io.github.flink.gcp.connector.firestore.DatabaseDestination;
import io.github.flink.gcp.connector.firestore.TestDocuments;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * A page reader over a scripted collection, answering name-ordered pages as the service would: from
 * the page's start cursor, up to its limit. It records every page it was asked for and the read
 * time each was read at, and fails the page it was told to. Only the start cursor's document name
 * and the limit are understood; an end cursor and an offset are ignored, and the tests that need
 * them read against the emulator.
 */
final class ScriptedQueryPageReader implements QueryPageReader {

    private static final long serialVersionUID = 1L;

    private final transient Firestore client;
    private final List<String> paths;
    private final transient List<StructuredQuery> pages = new ArrayList<>();
    private final transient List<Timestamp> readTimes = new ArrayList<>();
    private int failPage = -1;
    private int overfill;
    private int closeCalls;

    ScriptedQueryPageReader(Firestore client, List<String> paths) {
        this.client = client;
        this.paths = new ArrayList<>(paths);
        this.paths.sort(String::compareTo);
    }

    /** Fails the page with this index, counting from zero. */
    ScriptedQueryPageReader failingPage(int index) {
        this.failPage = index;
        return this;
    }

    /**
     * Answers every page with this many documents beyond its limit, as the client library does when
     * it retries a page broken mid-stream from its last document without lowering the limit.
     */
    ScriptedQueryPageReader overfilling(int documents) {
        this.overfill = documents;
        return this;
    }

    List<StructuredQuery> pages() {
        return pages;
    }

    List<Timestamp> readTimes() {
        return readTimes;
    }

    int closeCalls() {
        return closeCalls;
    }

    @Override
    public Query query(DatabaseDestination database, RunQueryRequest request) {
        return Query.fromProto(client, request);
    }

    @Override
    public List<? extends DocumentSnapshot> read(Query page, Timestamp readTime)
            throws IOException {
        StructuredQuery query = page.toProto().getStructuredQuery();
        readTimes.add(readTime);
        if (pages.size() == failPage) {
            pages.add(query);
            throw new IOException("scripted failure");
        }
        pages.add(query);
        int from = 0;
        if (query.hasStartAt()) {
            String start =
                    pathOf(query.getStartAt().getValues(query.getStartAt().getValuesCount() - 1));
            from = 0;
            while (from < paths.size()
                    && (query.getStartAt().getBefore()
                            ? paths.get(from).compareTo(start) < 0
                            : paths.get(from).compareTo(start) <= 0)) {
                from++;
            }
        }
        int to =
                query.hasLimit()
                        ? Math.min(paths.size(), from + query.getLimit().getValue() + overfill)
                        : paths.size();
        List<DocumentSnapshot> documents = new ArrayList<>();
        for (int i = from; i < to; i++) {
            documents.add(TestDocuments.document(client, paths.get(i), Map.of(), readTime));
        }
        return documents;
    }

    private static String pathOf(Value reference) {
        String name = reference.getReferenceValue();
        return name.substring(name.indexOf("/documents/") + "/documents/".length());
    }

    @Override
    public void useCredentials(@Nullable Credentials credentials) {}

    @Override
    public void close() {
        closeCalls++;
    }
}
