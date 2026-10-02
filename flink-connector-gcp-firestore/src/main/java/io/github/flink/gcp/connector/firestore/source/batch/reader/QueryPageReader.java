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

import org.apache.flink.annotation.Internal;

import com.google.auth.Credentials;
import com.google.cloud.Timestamp;
import com.google.cloud.firestore.DocumentSnapshot;
import com.google.cloud.firestore.Query;
import com.google.firestore.v1.RunQueryRequest;
import io.github.flink.gcp.connector.firestore.DatabaseDestination;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

/**
 * Reads one page of a query at a snapshot time.
 *
 * <p>The seam the split reader reads through, so that paging and resuming can be tested without a
 * container. {@link Serializable} because the source configuration it travels in goes into the job
 * graph; an implementation creates its client on first use, so building a job needs no credentials,
 * and receives them through {@link #useCredentials(Credentials)} from the reader that owns it.
 */
@Internal
public interface QueryPageReader extends Serializable, AutoCloseable {

    /**
     * Rebuilds a query from its wire form, on this reader's client.
     *
     * @param database the database the query addresses
     * @param request the query's wire form
     * @return the query
     * @throws IOException if the client cannot be created or the query does not belong to the
     *     database
     */
    Query query(DatabaseDestination database, RunQueryRequest request) throws IOException;

    /**
     * Reads one page: the whole of the given query, which the caller has limited, at a snapshot
     * time.
     *
     * @param page the query to read, built by {@link #query} or from a query it built
     * @param readTime the snapshot time
     * @return the documents, in the query's order
     * @throws IOException if the read fails
     */
    List<? extends DocumentSnapshot> read(Query page, Timestamp readTime) throws IOException;

    /**
     * Receives the credentials the owning reader loaded, before the first {@link #query}.
     *
     * <p>Abstract rather than defaulted because an implementation that quietly skipped it would
     * read as the process's application default credentials instead of the configured service
     * account — a misconfiguration nothing would report.
     *
     * @param credentials the credentials to build clients with, or {@code null} to leave the
     *     client's application default credentials in place
     */
    void useCredentials(@Nullable Credentials credentials);

    @Override
    void close() throws IOException;
}
