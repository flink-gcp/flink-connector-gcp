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

package io.github.flink.gcp.connector.datastore.sink;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.util.Preconditions;

import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.PathElement;
import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.base.failure.FailedElement;
import io.github.flink.gcp.connector.base.failure.FailureHandler;
import io.github.flink.gcp.connector.datastore.DatabaseDestination;

import javax.annotation.Nullable;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.UncheckedIOException;

/**
 * A single entity mutation that terminally failed, as passed to a {@link FailureHandler
 * FailureHandler&lt;FailedMutation&gt;}.
 *
 * <p>Carries the {@link DatastoreMutation} the serializer produced rather than the original record:
 * the sink writer is stateless and retains only writes, so by the time the service refuses one the
 * original record object no longer exists. When serialization itself failed, {@link #getMutation()}
 * is {@code null}.
 *
 * <h2>What the payload bytes are, and why</h2>
 *
 * <p>{@link #getPayloadBytes()} is the <b>Java-serialized {@link DatastoreMutation}</b>, recovered
 * with an {@code ObjectInputStream} against this connector and the Datastore client library. It is
 * not the Datastore {@code Mutation} protobuf, and that is not a choice: the client library's
 * conversion from an entity to its protobuf is package-private (google-cloud-datastore 3.7.0), so
 * the supported API offers no route to the wire form. The entity and key types are serializable, so
 * their encoding is one the client library maintains.
 *
 * <p>A handler that wants the mutation itself should take {@code FailureHandler<FailedMutation>}
 * and read {@link #getMutation()}; the bytes exist for the cross-connector {@code DeadLetterQueue}
 * view, which sees only {@link FailedElement}.
 *
 * <p>Instances are created by the sink and are not serializable.
 */
@PublicEvolving
public final class FailedMutation implements FailedElement {

    private final DatabaseDestination database;
    @Nullable private final DatastoreMutation mutation;
    private final String errorMessage;
    @Nullable private final Throwable cause;

    private FailedMutation(
            DatabaseDestination database,
            @Nullable DatastoreMutation mutation,
            String errorMessage,
            @Nullable Throwable cause) {
        this.database = Preconditions.checkNotNull(database, "database must not be null");
        this.mutation = mutation;
        this.errorMessage =
                Preconditions.checkNotNull(errorMessage, "errorMessage must not be null");
        this.cause = cause;
    }

    /**
     * Creates a failed mutation. Intended for the sink implementation (and tests of custom
     * handlers).
     *
     * @param database the database the mutation was bound for
     * @param mutation the mutation, or {@code null} when serialization itself failed
     * @param errorMessage the failure description
     * @param cause the underlying failure, or {@code null}
     * @return the failed mutation
     */
    public static FailedMutation of(
            DatabaseDestination database,
            @Nullable DatastoreMutation mutation,
            String errorMessage,
            @Nullable Throwable cause) {
        return new FailedMutation(database, mutation, errorMessage, cause);
    }

    /** Returns the database the mutation was bound for. */
    public DatabaseDestination getDatabase() {
        return database;
    }

    /**
     * Returns the mutation the serializer produced, or {@code null} when the record could not be
     * serialized in the first place.
     */
    @Nullable
    public DatastoreMutation getMutation() {
        return mutation;
    }

    @Override
    public String getConnector() {
        return "datastore";
    }

    /**
     * Returns the destination as the key's own project and database, then its namespace, when it
     * has one, and its path: {@code projects/P/databases/D/namespaces/NS/Kind/name-or-id/…}, with
     * {@code (default)} for the default database and {@code null} for a key carrying no database
     * id. Read from the key rather than from the sink, so a mutation routed because its key
     * addresses another database names that database. The sink's database alone when the record
     * could not be serialized.
     */
    @Override
    public String describeDestination() {
        if (mutation == null) {
            return database.toString();
        }
        Key key = mutation.getKey();
        String databaseId = key.getDatabaseId();
        StringBuilder destination =
                new StringBuilder("projects/")
                        .append(key.getProjectId())
                        .append("/databases/")
                        .append(
                                databaseId == null
                                        ? "null"
                                        : databaseId.isEmpty()
                                                ? DatabaseDestination.DEFAULT_DATABASE_NAME
                                                : databaseId);
        if (!key.getNamespace().isEmpty()) {
            destination.append("/namespaces/").append(key.getNamespace());
        }
        for (PathElement element : key.getAncestors()) {
            appendElement(destination, element.getKind(), element.getNameOrId());
        }
        appendElement(destination, key.getKind(), key.getNameOrId());
        return destination.toString();
    }

    private static void appendElement(StringBuilder destination, String kind, Object nameOrId) {
        destination.append('/').append(kind).append('/').append(nameOrId);
    }

    /**
     * Returns the Java-serialized mutation — see the class documentation for why it is not a
     * protobuf — or {@code null} when serialization itself failed.
     */
    @Override
    @Nullable
    public ByteString getPayloadBytes() {
        if (mutation == null) {
            return null;
        }
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
            out.writeObject(mutation);
        } catch (IOException e) {
            // ByteArrayOutputStream does not do I/O, and a DatastoreMutation holds only the client
            // library's serializable entity and key types.
            throw new UncheckedIOException("Failed to serialize the Datastore mutation", e);
        }
        return ByteString.copyFrom(bytes.toByteArray());
    }

    @Override
    public String getErrorMessage() {
        return errorMessage;
    }

    @Override
    @Nullable
    public Throwable getCause() {
        return cause;
    }

    @Override
    public String toString() {
        return "FailedMutation{database="
                + database
                + ", mutation="
                + mutation
                + ", errorMessage="
                + errorMessage
                + "}";
    }
}
