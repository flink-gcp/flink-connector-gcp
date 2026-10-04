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

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.LookupFunction;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

/**
 * Looks a document up by id, blocking, and reads it as a row. Public because the planner refuses a
 * function class that is not.
 */
@Internal
public final class FirestoreRowDataLookupFunction extends LookupFunction {
    private static final long serialVersionUID = 1L;

    private final RowDataDeserializationSchema deserializer;
    private final int maxRetries;
    private final FirestoreDocumentLookup lookup;

    FirestoreRowDataLookupFunction(
            RowDataDeserializationSchema deserializer,
            int maxRetries,
            FirestoreDocumentLookup lookup) {
        this.deserializer = deserializer;
        this.maxRetries = maxRetries;
        this.lookup = lookup;
    }

    @VisibleForTesting
    FirestoreDocumentLookup documentLookup() {
        return lookup;
    }

    @VisibleForTesting
    int maxRetries() {
        return maxRetries;
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        deserializer.open(null);
        lookup.open();
    }

    @Override
    public Collection<RowData> lookup(RowData keyRow) throws IOException {
        String id = FirestoreDocumentLookups.documentId(keyRow);
        if (id == null) {
            return Collections.emptyList();
        }
        for (int retry = 0; ; retry++) {
            try {
                return FirestoreDocumentLookups.rows(
                        deserializer, FirestoreDocumentLookups.await(lookup.readAsync(id), id));
            } catch (RuntimeException failure) {
                if (retry >= maxRetries || !FirestoreLookupErrorClassifier.isTransient(failure)) {
                    throw failure;
                }
            }
        }
    }

    @Override
    public void close() throws Exception {
        lookup.close();
    }
}
