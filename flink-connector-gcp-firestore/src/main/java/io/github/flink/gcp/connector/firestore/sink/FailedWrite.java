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

package io.github.flink.gcp.connector.firestore.sink;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.util.Preconditions;

import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.base.failure.FailedElement;
import io.github.flink.gcp.connector.base.failure.FailureHandler;
import io.github.flink.gcp.connector.firestore.DatabaseDestination;

import javax.annotation.Nullable;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.UncheckedIOException;

/**
 * A single document write that terminally failed, as passed to a {@link FailureHandler
 * FailureHandler&lt;FailedWrite&gt;}.
 *
 * <p>Carries the {@link FirestoreWrite} the serializer produced rather than the original record:
 * the sink writer is stateless and retains only writes, so by the time the service refuses one the
 * original record object no longer exists. When serialization itself failed, {@link #getWrite()} is
 * {@code null}.
 *
 * <h2>What the payload bytes are, and why</h2>
 *
 * <p>{@link #getPayloadBytes()} is the <b>Java-serialized {@link FirestoreWrite}</b>, recovered
 * with an {@code ObjectInputStream} against this connector. It is not the Firestore {@code Write}
 * protobuf, and that is not a choice: the client library's conversion from Java values to wire
 * values is package-private ({@code UserDataConverter}, google-cloud-firestore 3.46.0), and the
 * public encoders it has are marked internal, so the supported API offers no route to the wire
 * form. {@code FirestoreWrite} is this connector's own serializable value, so its encoding is one
 * the connector maintains.
 *
 * <p>A handler that wants the write itself should take {@code FailureHandler<FailedWrite>} and read
 * {@link #getWrite()}; the bytes exist for the cross-connector {@code DeadLetterQueue} view, which
 * sees only {@link FailedElement}.
 *
 * <p>Instances are created by the sink and are not serializable.
 */
@PublicEvolving
public final class FailedWrite implements FailedElement {

    private final DatabaseDestination database;
    @Nullable private final FirestoreWrite write;
    private final String errorMessage;
    @Nullable private final Throwable cause;

    private FailedWrite(
            DatabaseDestination database,
            @Nullable FirestoreWrite write,
            String errorMessage,
            @Nullable Throwable cause) {
        this.database = Preconditions.checkNotNull(database, "database must not be null");
        this.write = write;
        this.errorMessage =
                Preconditions.checkNotNull(errorMessage, "errorMessage must not be null");
        this.cause = cause;
    }

    /**
     * Creates a failed write. Intended for the sink implementation (and tests of custom handlers).
     *
     * @param database the database the write was bound for
     * @param write the write, or {@code null} when serialization itself failed
     * @param errorMessage the failure description
     * @param cause the underlying failure, or {@code null}
     * @return the failed write
     */
    public static FailedWrite of(
            DatabaseDestination database,
            @Nullable FirestoreWrite write,
            String errorMessage,
            @Nullable Throwable cause) {
        return new FailedWrite(database, write, errorMessage, cause);
    }

    /** Returns the database the write was bound for. */
    public DatabaseDestination getDatabase() {
        return database;
    }

    /**
     * Returns the write the serializer produced, or {@code null} when the record could not be
     * serialized in the first place.
     */
    @Nullable
    public FirestoreWrite getWrite() {
        return write;
    }

    @Override
    public String getConnector() {
        return "firestore";
    }

    /**
     * Returns the destination as {@code projects/P/databases/D/documents/PATH}, or without the
     * document segment when the record could not be serialized.
     */
    @Override
    public String describeDestination() {
        return write == null
                ? database.toString()
                : database + "/documents/" + write.getDocumentPath();
    }

    /**
     * Returns the Java-serialized write — see the class documentation for why it is not a protobuf
     * — or {@code null} when serialization itself failed.
     */
    @Override
    @Nullable
    public ByteString getPayloadBytes() {
        if (write == null) {
            return null;
        }
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
            out.writeObject(write);
        } catch (IOException e) {
            // ByteArrayOutputStream does not do I/O, and FirestoreWrite holds only serializable
            // values by construction, so reaching here means that construction was bypassed.
            throw new UncheckedIOException("Failed to serialize the Firestore write", e);
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
        return "FailedWrite{database="
                + database
                + ", write="
                + write
                + ", errorMessage="
                + errorMessage
                + "}";
    }
}
