/*
 * Copyright 2026 laughingman7743
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

package io.github.flink.gcp.connector.bigquery.source.enumerator;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;

import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.StatusCode;
import com.google.cloud.bigquery.storage.v1.BigQueryReadClient;
import com.google.cloud.bigquery.storage.v1.CreateReadSessionRequest;
import com.google.cloud.bigquery.storage.v1.ReadSession;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.base.rpc.StatusCodes;
import io.github.flink.gcp.connector.bigquery.source.BigQueryReadClients;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Locale;

/**
 * Creates read sessions through a {@link BigQueryReadClient}.
 *
 * <p>Named after the SDK resource its {@link #close()} releases, as this repository's other client
 * wrappers are. The client is opened on first use rather than in the constructor: this object is
 * built where the job graph is, and a client built there would demand credentials on the submitting
 * machine.
 *
 * <p>Creation and release are guarded, and not for tidiness: {@link #create} runs on a coordinator
 * worker thread while {@link #close} runs on the scheduler thread, and the two race whenever a job
 * is cancelled during session creation. Unguarded, {@code close} reads a field the worker has not
 * yet written, closes nothing, and the client — a gRPC channel and its executor — is leaked in the
 * JobManager with nothing left to reach it. The lock is held for the client's construction only,
 * never for the call itself. The monitor is this object's own: a lock field would be one more thing
 * to serialize into the job graph, and {@code Object} is not serializable.
 */
@Internal
public final class ReadClientSessionCreator implements ReadSessionCreator {

    private static final long serialVersionUID = 1L;

    @Nullable private final EmulatorEndpoint emulatorEndpoint;

    private transient BigQueryReadClient client;
    private transient boolean closed;

    /**
     * Creates the session creator.
     *
     * @param emulatorEndpoint the emulator's gRPC endpoint, or {@code null} for BigQuery itself
     */
    public ReadClientSessionCreator(@Nullable EmulatorEndpoint emulatorEndpoint) {
        this.emulatorEndpoint = emulatorEndpoint;
    }

    @Override
    public ReadSession create(CreateReadSessionRequest request) throws IOException {
        BigQueryReadClient open;
        synchronized (this) {
            if (closed) {
                throw new IOException(
                        "The BigQuery read session creator was closed; the source is shutting"
                                + " down.");
            }
            if (client == null) {
                client = BigQueryReadClients.createForSessions(emulatorEndpoint);
            }
            open = client;
        }
        try {
            return open.createReadSession(request);
        } catch (ApiException e) {
            String hint = viewHint(e);
            if (hint == null) {
                throw e;
            }
            throw new IOException(hint, e);
        }
    }

    /**
     * Returns the sentence to put in front of a failure that looks like a read of a view, or {@code
     * null} for any other failure.
     *
     * <p>This connector's rule is to match status codes and never message text, and the rule is not
     * being bent here so much as reaching its edge: {@code INVALID_ARGUMENT} is also what a bad
     * projection, an unparsable row restriction and a snapshot outside the time-travel window
     * answer with, so the code alone identifies nothing. What makes matching the text acceptable is
     * that the result is <em>only</em> a sentence added to a failure that is being thrown either
     * way — never a decision about retrying, dropping or routing, which is what that rule protects.
     * If BigQuery rewords this, the hint stops appearing and the error the user sees is exactly the
     * one they would have seen without it.
     *
     * <p>Measured 2026-08-10: a logical view and a materialized view both answer {@code
     * CreateReadSession} with {@code INVALID_ARGUMENT: request failed: non-table entities cannot be
     * read with the storage API} — the same code and the same words, so one match covers both.
     */
    @Nullable
    @VisibleForTesting
    static String viewHint(ApiException e) {
        if (StatusCodes.codeOf(e) != StatusCode.Code.INVALID_ARGUMENT) {
            return null;
        }
        String message = e.getMessage();
        if (message == null || !message.toLowerCase(Locale.ROOT).contains("non-table entities")) {
            return null;
        }
        return "BigQuery refused to read this as a table. The Storage Read API reads storage, and"
                + " a logical or materialized view has none — if that is what this names, read it"
                + " with query(...) instead of table(...), for example"
                + " query(\"SELECT * FROM `project.dataset.the_view`\").";
    }

    @Override
    public void close() {
        BigQueryReadClient open;
        synchronized (this) {
            closed = true;
            open = client;
            client = null;
        }
        if (open != null) {
            open.close();
        }
    }
}
