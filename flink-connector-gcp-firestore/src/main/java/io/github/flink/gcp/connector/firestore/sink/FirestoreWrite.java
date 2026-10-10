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
import java.security.SecureRandom;
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
 *   <li>{@link #add(String, Map)} — creates a new document in a collection, under an id the write
 *       draws. It never replaces an existing document: if the id names one, the sink creates the
 *       document again under a new id.
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
 * Boolean}, {@link Timestamp}, {@link GeoPoint}, a {@link Blob} of subtype 0, a {@link
 * FirestoreDocumentReference}, a {@link java.util.List} of values, or a {@link Map} from non-empty
 * {@link String} field names to values, nested at most 500 levels deep. A {@code
 * FirestoreDocumentReference} is sent as a reference to that document in the database the sink
 * writes to. Anything else is rejected when the write is built, which the sink reports as a failure
 * to serialize that record. A BSON binary {@code Blob} of another subtype is rejected because the
 * client library encodes it as a reserved map rather than as bytes, and the sink's request-size
 * accounting does not count that form. The list is closed on purpose: it is the set of values the
 * client library is known to encode, once the sink has replaced each {@code
 * FirestoreDocumentReference} with the library's own reference, and a value it cannot encode would
 * fail the job rather than one record. Firestore's own rules about the values are the service's to
 * enforce: a document over 1 MiB, or a map nested more than 20 levels, is refused with {@code
 * INVALID_ARGUMENT} for that write.
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

    /** The characters of a drawn document id: the client library's own for an added document. */
    private static final String DRAWN_ID_ALPHABET =
            "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

    /** The length of a drawn document id, the client library's own: about 119 bits. */
    private static final int DRAWN_ID_LENGTH = 20;

    private static final SecureRandom RANDOM = new SecureRandom();

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
    private final boolean drawnId;

    private FirestoreWrite(
            String documentPath,
            Operation operation,
            Map<String, Object> fields,
            @Nullable Timestamp lastUpdateTime,
            boolean drawnId) {
        this.documentPath = documentPath;
        this.operation = operation;
        this.fields = fields;
        this.lastUpdateTime = lastUpdateTime;
        this.drawnId = drawnId;
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
     * Creates a new document in the collection, under an id the write draws: 20 letters and digits
     * from a {@link SecureRandom}, as the client library draws one for {@code
     * CollectionReference.add}.
     *
     * <p>The write is a {@link Operation#CREATE} of that document, so it never replaces an existing
     * one. If the drawn id names a document that exists, Firestore refuses it with {@code
     * ALREADY_EXISTS} and the sink sends the document again under a new id, rather than routing the
     * record. That refusal also answers the client library's own retry of a create that was applied
     * but whose answer was lost, and the sink cannot tell the two apart without a read, so such a
     * retry leaves the document twice, under two ids. A replay after a restart draws new ids too,
     * and so creates its documents again.
     *
     * @param collectionPath the collection path relative to the database's documents root, such as
     *     {@code users} or {@code users/alice/orders}
     * @param fields the document's fields
     * @return the write, whose {@link #getDocumentPath()} names the drawn id
     * @throws IllegalArgumentException if the path is malformed or a field name or value is not
     *     accepted
     */
    public static FirestoreWrite add(String collectionPath, Map<String, ?> fields) {
        FirestoreWriteChecks.checkCollectionPath(collectionPath);
        return of(collectionPath + "/" + drawId(), Operation.CREATE, fields, null, true);
    }

    private static String drawId() {
        StringBuilder id = new StringBuilder(DRAWN_ID_LENGTH);
        for (int i = 0; i < DRAWN_ID_LENGTH; i++) {
            id.append(DRAWN_ID_ALPHABET.charAt(RANDOM.nextInt(DRAWN_ID_ALPHABET.length())));
        }
        return id.toString();
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
        return of(documentPath, operation, fields, lastUpdateTime, false);
    }

    private static FirestoreWrite of(
            String documentPath,
            Operation operation,
            Map<String, ?> fields,
            @Nullable Timestamp lastUpdateTime,
            boolean drawnId) {
        FirestoreWriteChecks.checkDocumentPath(documentPath);
        Preconditions.checkNotNull(fields, "fields must not be null");
        Preconditions.checkArgument(
                operation != Operation.UPDATE || !fields.isEmpty(),
                "An update must name at least one field.");
        return new FirestoreWrite(
                documentPath,
                operation,
                FirestoreWriteChecks.copyFields(fields),
                lastUpdateTime,
                drawnId);
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

    /**
     * Returns whether the write drew its document id, as {@link #add(String, Map)} does: such a
     * write is sent again under a new id when its id names an existing document.
     */
    public boolean hasDrawnId() {
        return drawnId;
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
                && Objects.equals(lastUpdateTime, that.lastUpdateTime)
                && drawnId == that.drawnId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(documentPath, operation, fields, lastUpdateTime, drawnId);
    }

    /**
     * Returns the operation, the path, the field names, the precondition and whether the id was
     * drawn — never the field values, which may be large or sensitive and which a log line has no
     * use for.
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
                + (drawnId ? ", drawnId=true" : "")
                + "}";
    }
}
