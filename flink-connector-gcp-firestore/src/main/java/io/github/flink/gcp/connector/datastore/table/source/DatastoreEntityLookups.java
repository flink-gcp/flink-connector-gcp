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

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.connector.source.lookup.LookupOptions;
import org.apache.flink.table.data.RowData;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.cloud.Timestamp;
import com.google.cloud.datastore.Entity;
import com.google.datastore.v1.EntityResult;
import com.google.datastore.v1.Key;
import com.google.datastore.v1.LookupResponse;
import io.github.flink.gcp.connector.datastore.source.serializer.EntityMetadata;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/** What both lookup functions share: one read of a key, and the row its answer makes. */
@Internal
final class DatastoreEntityLookups {

    private DatastoreEntityLookups() {}

    /**
     * The service deferred the key "due to resource constraints", as {@code datastore.proto} says,
     * and read nothing. It is a read failure the lookup may send again within {@code
     * lookup.max-retries}, never an answer: the client library's {@code Datastore.get} re-sends a
     * deferred key with no bound (ADR-0175), and a deferral read as a missing entity would join no
     * row for an entity that exists.
     */
    static final class DeferredException extends RuntimeException {
        private static final long serialVersionUID = 1L;

        DeferredException(Key key) {
            super(
                    "Datastore deferred the lookup of "
                            + describe(key)
                            + " for lack of resources and read nothing. A deferred lookup is"
                            + " sent again within '"
                            + LookupOptions.MAX_RETRIES.key()
                            + "'; raise it if deferrals outlast the budget.");
        }
    }

    /**
     * Sends one lookup of the key, failing its future with {@link DeferredException} when the
     * service defers the key.
     */
    static ApiFuture<LookupResponse> read(DatastoreEntityLookup lookup, Key key) {
        return ApiFutures.transform(
                lookup.lookupAsync(key),
                response -> {
                    if (response.getDeferredCount() > 0) {
                        throw new DeferredException(key);
                    }
                    return response;
                },
                Runnable::run);
    }

    /**
     * Converts an answer: no row for a missing entity, else the found entity's row, with the
     * answer's read time as its {@code read-time}.
     *
     * @throws IOException if the answer neither finds the key nor reports it missing, or if a value
     *     does not match its column and the policy cannot read it
     */
    static Collection<RowData> rows(
            RowDataDeserializationSchema deserializer, LookupResponse response, Key key)
            throws IOException {
        if (response.getFoundCount() == 1 && response.getMissingCount() == 0) {
            EntityResult found = response.getFound(0);
            return Collections.singletonList(
                    deserializer.read(
                            Entity.fromPb(found.getEntity()),
                            new EntityMetadata(
                                    found.getVersion(),
                                    found.hasCreateTime()
                                            ? Timestamp.fromProto(found.getCreateTime())
                                            : null,
                                    found.hasUpdateTime()
                                            ? Timestamp.fromProto(found.getUpdateTime())
                                            : null,
                                    response.hasReadTime()
                                            ? Timestamp.fromProto(response.getReadTime())
                                            : null)));
        }
        if (response.getFoundCount() == 0 && response.getMissingCount() == 1) {
            return Collections.emptyList();
        }
        throw new IOException(
                "Datastore answered the lookup of "
                        + describe(key)
                        + " with "
                        + response.getFoundCount()
                        + " found and "
                        + response.getMissingCount()
                        + " missing entities, where one lookup of one key has exactly one.");
    }

    /**
     * Waits for a read, rethrowing the client library's failure as it is, so the classifier sees
     * its status.
     */
    static LookupResponse await(Future<LookupResponse> read, Key key) {
        try {
            return read.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(
                    "Interrupted while looking up " + describe(key) + ".", e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            }
            throw new IllegalStateException("Failed to look up " + describe(key) + ".", cause);
        }
    }

    /** Names a lookup's key in a message: its kind and its name or id, without the partition. */
    private static String describe(Key key) {
        Key.PathElement element = key.getPath(key.getPathCount() - 1);
        return "the "
                + element.getKind()
                + " entity "
                + (element.getIdTypeCase() == Key.PathElement.IdTypeCase.ID
                        ? "with id " + element.getId()
                        : "named '" + element.getName() + "'");
    }
}
