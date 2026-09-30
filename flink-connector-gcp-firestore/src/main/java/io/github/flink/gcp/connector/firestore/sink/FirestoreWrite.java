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

import com.google.cloud.Timestamp;
import com.google.cloud.firestore.Blob;
import com.google.cloud.firestore.GeoPoint;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Map;
import java.util.Objects;

/**
 * One document operation, as a {@code FirestoreWriteSerializationSchema} returns it for a record.
 *
 * <p>A write names its document by a path relative to the database's documents root — {@code
 * users/alice}, or {@code users/alice/orders/1001} for a document in a subcollection — so one sink
 * writes to as many collections as its serializer produces. A path alternates collection and
 * document ids, so it has an even number of {@code /}-separated segments, none of them empty.
 *
 * <h2>Operations</h2>
 *
 * <ul>
 *   <li>{@link #set(String, Map)} — replaces the document, creating it if it is missing.
 *   <li>{@link #setMerge(String, Map)} — merges the fields into the document, creating it if it is
 *       missing. A nested map is merged key by key rather than replaced.
 *   <li>{@link #create(String, Map)} — creates the document, and fails with {@code ALREADY_EXISTS}
 *       if it exists.
 *   <li>{@link #update(String, Map)} — replaces the named top-level fields of an existing document,
 *       and fails with {@code NOT_FOUND} if it is missing.
 *   <li>{@link #delete(String)} — deletes the document; deleting a missing document succeeds.
 * </ul>
 *
 * <p>Every field name is literal, in every operation: {@code a.b} names one top-level field whose
 * name contains a dot, never field {@code b} inside map {@code a}. The client library would read an
 * update's keys as dotted field paths instead, and one spelling meaning two things depending on the
 * operation is the confusion this rules out. To change one key of a nested map, use {@link
 * #setMerge(String, Map)}, which merges nested maps.
 *
 * <h2>Values</h2>
 *
 * <p>A field value is one of: {@code null}, {@link String}, {@link Long}, {@link Double}, {@link
 * Boolean}, {@link Timestamp}, {@link GeoPoint}, {@link Blob}, a {@link java.util.List} of values,
 * or a {@link Map} from non-empty {@link String} field names to values, nested at most 500 levels
 * deep. Anything else is rejected when the write is built, which the sink reports as a failure to
 * serialize that record. The list is closed on purpose: it is the set of values the client library
 * is known to encode, and a value it cannot encode would fail the job rather than one record.
 * Firestore's own rules about the values are the service's to enforce: a document over 1 MiB, or a
 * map nested more than 20 levels, is refused with {@code INVALID_ARGUMENT} for that write.
 *
 * <h2>Preconditions</h2>
 *
 * <p>{@link #update(String, Map, Timestamp)} and {@link #delete(String, Timestamp)} apply only if
 * the document was last updated at exactly the given time; otherwise Firestore refuses the write
 * with {@code FAILED_PRECONDITION}. That is the only precondition the client library exposes
 * publicly. What the sink does with such a refusal is {@code FirestoreSinkBuilder}'s {@code
 * preconditionFailurePolicy}.
 *
 * <p>Instances are immutable and serializable. The fields map and every map and list inside it are
 * copied when the write is built.
 */
@PublicEvolving
public final class FirestoreWrite implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * How deep field values may nest: a top-level field's value is at depth one, and each enclosing
     * map or list adds one. The client library's own recursion bound, which it enforces
     * synchronously, sits one level deeper (google-cloud-firestore 3.46.0), so nothing this accepts
     * reaches it.
     */
    static final int MAX_NESTING_DEPTH = 500;

    /** The document operation a write performs. */
    @PublicEvolving
    public enum Operation {
        /** Replace the document, creating it if it is missing. */
        SET,
        /** Merge the fields into the document, creating it if it is missing. */
        SET_MERGE,
        /** Create the document; fails with {@code ALREADY_EXISTS} if it exists. */
        CREATE,
        /** Replace the named fields of an existing document; fails with {@code NOT_FOUND}. */
        UPDATE,
        /** Delete the document. */
        DELETE
    }

    private final String documentPath;
    private final Operation operation;
    private final Map<String, Object> fields;
    @Nullable private final Timestamp lastUpdateTime;

    private FirestoreWrite(
            String documentPath,
            Operation operation,
            Map<String, Object> fields,
            @Nullable Timestamp lastUpdateTime) {
        this.documentPath = documentPath;
        this.operation = operation;
        this.fields = fields;
        this.lastUpdateTime = lastUpdateTime;
    }

    /**
     * Replaces the document, creating it if it is missing.
     *
     * @param documentPath the document path relative to the database's documents root
     * @param fields the document's fields
     * @return the write
     * @throws IllegalArgumentException if the path is malformed or a field name or value is not
     *     accepted
     */
    public static FirestoreWrite set(String documentPath, Map<String, ?> fields) {
        return of(documentPath, Operation.SET, fields, null);
    }

    /**
     * Merges the fields into the document, creating it if it is missing. Fields the map does not
     * name keep their values, and a nested map is merged key by key.
     *
     * @param documentPath the document path relative to the database's documents root
     * @param fields the fields to merge
     * @return the write
     * @throws IllegalArgumentException if the path is malformed or a field name or value is not
     *     accepted
     */
    public static FirestoreWrite setMerge(String documentPath, Map<String, ?> fields) {
        return of(documentPath, Operation.SET_MERGE, fields, null);
    }

    /**
     * Creates the document. Firestore refuses it with {@code ALREADY_EXISTS} if the document exists
     * — including when an earlier attempt of the same write created it, which is how a replay after
     * a restart surfaces.
     *
     * @param documentPath the document path relative to the database's documents root
     * @param fields the document's fields
     * @return the write
     * @throws IllegalArgumentException if the path is malformed or a field name or value is not
     *     accepted
     */
    public static FirestoreWrite create(String documentPath, Map<String, ?> fields) {
        return of(documentPath, Operation.CREATE, fields, null);
    }

    /**
     * Replaces the named top-level fields of an existing document. Firestore refuses it with {@code
     * NOT_FOUND} if the document is missing.
     *
     * @param documentPath the document path relative to the database's documents root
     * @param fields the fields to replace; at least one
     * @return the write
     * @throws IllegalArgumentException if the path is malformed, the map is empty, or a field name
     *     or value is not accepted
     */
    public static FirestoreWrite update(String documentPath, Map<String, ?> fields) {
        return of(documentPath, Operation.UPDATE, fields, null);
    }

    /**
     * Replaces the named top-level fields of the document if it was last updated at exactly {@code
     * lastUpdateTime}. Firestore refuses it with {@code FAILED_PRECONDITION} otherwise.
     *
     * @param documentPath the document path relative to the database's documents root
     * @param fields the fields to replace; at least one
     * @param lastUpdateTime the update time the document must still have
     * @return the write
     * @throws IllegalArgumentException if the path is malformed, the map is empty, or a field name
     *     or value is not accepted
     */
    public static FirestoreWrite update(
            String documentPath, Map<String, ?> fields, Timestamp lastUpdateTime) {
        return of(
                documentPath,
                Operation.UPDATE,
                fields,
                Preconditions.checkNotNull(lastUpdateTime, "lastUpdateTime must not be null"));
    }

    /**
     * Deletes the document. Deleting a document that does not exist succeeds.
     *
     * @param documentPath the document path relative to the database's documents root
     * @return the write
     * @throws IllegalArgumentException if the path is malformed
     */
    public static FirestoreWrite delete(String documentPath) {
        return of(documentPath, Operation.DELETE, Map.of(), null);
    }

    /**
     * Deletes the document if it was last updated at exactly {@code lastUpdateTime}. Firestore
     * refuses it with {@code FAILED_PRECONDITION} otherwise.
     *
     * @param documentPath the document path relative to the database's documents root
     * @param lastUpdateTime the update time the document must still have
     * @return the write
     * @throws IllegalArgumentException if the path is malformed
     */
    public static FirestoreWrite delete(String documentPath, Timestamp lastUpdateTime) {
        return of(
                documentPath,
                Operation.DELETE,
                Map.of(),
                Preconditions.checkNotNull(lastUpdateTime, "lastUpdateTime must not be null"));
    }

    private static FirestoreWrite of(
            String documentPath,
            Operation operation,
            Map<String, ?> fields,
            @Nullable Timestamp lastUpdateTime) {
        FirestoreWriteChecks.checkDocumentPath(documentPath);
        Preconditions.checkNotNull(fields, "fields must not be null");
        Preconditions.checkArgument(
                operation != Operation.UPDATE || !fields.isEmpty(),
                "An update must name at least one field.");
        return new FirestoreWrite(
                documentPath, operation, FirestoreWriteChecks.copyFields(fields), lastUpdateTime);
    }

    /** Returns the document path relative to the database's documents root. */
    public String getDocumentPath() {
        return documentPath;
    }

    /** Returns the operation. */
    public Operation getOperation() {
        return operation;
    }

    /**
     * Returns the fields, unmodifiable; empty for a {@link Operation#DELETE}. Nested maps and lists
     * are unmodifiable too.
     */
    public Map<String, Object> getFields() {
        return fields;
    }

    /**
     * Returns the update time the document must still have for the write to apply, or {@code null}
     * when the write carries no precondition.
     */
    @Nullable
    public Timestamp getLastUpdateTime() {
        return lastUpdateTime;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FirestoreWrite that = (FirestoreWrite) o;
        return documentPath.equals(that.documentPath)
                && operation == that.operation
                && fields.equals(that.fields)
                && Objects.equals(lastUpdateTime, that.lastUpdateTime);
    }

    @Override
    public int hashCode() {
        return Objects.hash(documentPath, operation, fields, lastUpdateTime);
    }

    /**
     * Returns the operation, the path, the field names and the precondition — never the field
     * values, which may be large or sensitive and which a log line has no use for.
     */
    @Override
    public String toString() {
        return "FirestoreWrite{operation="
                + operation
                + ", documentPath="
                + documentPath
                + ", fieldNames="
                + fields.keySet()
                + (lastUpdateTime == null ? "" : ", lastUpdateTime=" + lastUpdateTime)
                + "}";
    }
}
