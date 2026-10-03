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

import org.apache.flink.annotation.Internal;

import com.google.auth.Credentials;
import com.google.datastore.v1.QueryResultBatch;
import com.google.datastore.v1.RunQueryRequest;
import io.github.flink.gcp.connector.datastore.DatabaseDestination;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;

/**
 * Reads one page of a query: one {@code RunQuery} call.
 *
 * <p>The seam the split reader reads through, so that paging and resuming can be tested without a
 * container. {@link Serializable} because the source configuration it travels in goes into the job
 * graph; an implementation creates its client on first use, so building a job needs no credentials,
 * and receives them through {@link #useCredentials(Credentials)} from the reader that owns it.
 */
@Internal
public interface QueryPageReader extends Serializable, AutoCloseable {

    /**
     * Reads one page.
     *
     * @param database the database the request addresses
     * @param page the request, with its read time, cursor and limit set by the caller
     * @return the batch the service answered with
     * @throws IOException if the client cannot be created or the read fails
     */
    QueryResultBatch read(DatabaseDestination database, RunQueryRequest page) throws IOException;

    /**
     * Receives the credentials the owning reader loaded, before the first {@link #read}.
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
