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

package io.github.flink.gcp.connector.spanner.sink;

import org.apache.flink.annotation.Public;
import org.apache.flink.util.Preconditions;

import com.google.cloud.spanner.Mutation;
import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.base.failure.FailedElement;
import io.github.flink.gcp.connector.base.failure.FailureHandler;
import io.github.flink.gcp.connector.spanner.SpannerDatabase;

import javax.annotation.Nullable;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.UncheckedIOException;

/**
 * A single mutation that terminally failed to be applied to Spanner, as passed to a {@link
 * FailureHandler FailureHandler&lt;FailedMutation&gt;}.
 *
 * <p>Carries the {@link Mutation} the serializer produced rather than the original record: the sink
 * writer is stateless and retains only mutations, so by the time the service rejects one the
 * original record object no longer exists. When serialization itself failed, {@link #getMutation()}
 * and {@link #getTable()} are {@code null}.
 *
 * <h2>What the payload bytes are, and why</h2>
 *
 * <p>{@link #getPayloadBytes()} is the <b>Java-serialized {@link Mutation}</b>, recovered with an
 * {@code ObjectInputStream} against the same client library. It is not a protobuf, and that is not
 * a choice: the Spanner client library exposes no public route from a {@code Mutation} to its wire
 * form — {@code Mutation.toProtoAndReturnRandomMutation}, {@code Value.toProto()}, {@code
 * Key.toProto()} and {@code KeySet.appendToProto} are all package-private (checked against
 * google-cloud-spanner 6.119.0). Nor can the debug rendering stand in: {@code Mutation.toString()}
 * truncates every string value at 36 characters, so it would hand a dead-letter consumer a payload
 * that looks complete and is not. Java serialization is the one non-lossy encoding the public API
 * offers, and {@code Mutation}, {@code Value}, {@code Key} and {@code KeySet} each declare a {@code
 * serialVersionUID}, so it is an affordance the library maintains rather than an accident.
 *
 * <p>A handler that wants the mutation itself should take {@code FailureHandler<FailedMutation>}
 * and read {@link #getMutation()}; the bytes exist for the cross-connector {@code DeadLetterQueue}
 * view, which sees only {@link FailedElement}.
 *
 * <p>Instances are created by the sink and are not serializable.
 */
@Public
public final class FailedMutation implements FailedElement {

    private final SpannerDatabase database;
    @Nullable private final Mutation mutation;
    private final String errorMessage;
    @Nullable private final Throwable cause;

    private FailedMutation(
            SpannerDatabase database,
            @Nullable Mutation mutation,
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
     * @param database the database the mutation was routed to
     * @param mutation the mutation, or {@code null} when serialization itself failed
     * @param errorMessage the failure description
     * @param cause the underlying failure, or {@code null}
     * @return the failed mutation
     */
    public static FailedMutation of(
            SpannerDatabase database,
            @Nullable Mutation mutation,
            String errorMessage,
            @Nullable Throwable cause) {
        return new FailedMutation(database, mutation, errorMessage, cause);
    }

    /** Returns the database the mutation was routed to. */
    public SpannerDatabase getDatabase() {
        return database;
    }

    /**
     * Returns the mutation the serializer produced, or {@code null} when the record could not be
     * serialized in the first place.
     */
    @Nullable
    public Mutation getMutation() {
        return mutation;
    }

    /**
     * Returns the table the mutation applies to, or {@code null} when the record could not be
     * serialized.
     */
    @Nullable
    public String getTable() {
        return mutation == null ? null : mutation.getTable();
    }

    @Override
    public String getConnector() {
        return "spanner";
    }

    /**
     * Returns the destination as {@code projects/P/instances/I/databases/D/tables/T}, or without
     * the table segment when the record could not be serialized.
     */
    @Override
    public String describeDestination() {
        return mutation == null ? database.toString() : database + "/tables/" + mutation.getTable();
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
            // ByteArrayOutputStream does not do I/O, and Mutation is Serializable by contract,
            // so reaching here means the client library broke that contract.
            throw new UncheckedIOException("Failed to serialize the Spanner mutation", e);
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
                + ", table="
                + getTable()
                + ", errorMessage="
                + errorMessage
                + "}";
    }
}
