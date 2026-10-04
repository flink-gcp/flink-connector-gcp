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

package io.github.flink.gcp.connector.firestore.table.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.table.data.RowData;

import io.github.flink.gcp.connector.firestore.sink.FirestoreWrite;
import io.github.flink.gcp.connector.firestore.sink.serializer.FirestoreWriteSerializationSchema;
import io.github.flink.gcp.connector.firestore.table.FirestoreTableSchema;
import io.github.flink.gcp.connector.firestore.table.WriteMode;

import javax.annotation.Nullable;

import java.io.IOException;
import java.security.SecureRandom;
import java.util.Map;
import java.util.Objects;
import java.util.Random;

/**
 * Turns a table row into the document write the {@code firestore} table sink sends.
 *
 * <p>With a PRIMARY KEY, the key column is the document id within the collection and every other
 * column a field: an insert or an update-after is a {@code set}, a {@code merge} or an {@code
 * update} as {@code sink.write-mode} says, and a delete deletes the document. Without one, every
 * row is written to a new document whose id this class mints, as the client library does for {@code
 * CollectionReference.add}: 20 characters drawn from letters and digits.
 */
@Internal
public final class RowDataSerializationSchema
        implements FirestoreWriteSerializationSchema<RowData> {

    private static final long serialVersionUID = 1L;

    private static final String AUTO_ID_ALPHABET =
            "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    private static final int AUTO_ID_LENGTH = 20;

    private final FirestoreTableSchema schema;
    private final String collection;
    private final WriteMode writeMode;
    @Nullable private final String keyColumn;
    private final RowDataToFirestoreConverter document;

    private transient Random random;

    /**
     * Creates the schema.
     *
     * @param schema the checked table schema
     * @param collection the collection path the documents are written into
     * @param writeMode what an insert or an update-after does, for a table with a PRIMARY KEY
     */
    public RowDataSerializationSchema(
            FirestoreTableSchema schema, String collection, WriteMode writeMode) {
        this.schema = schema;
        this.collection = collection;
        this.writeMode = writeMode;
        this.keyColumn =
                schema.hasPrimaryKey()
                        ? schema.getRowType().getFieldNames().get(schema.getKeyIndex())
                        : null;
        this.document = RowDataToFirestoreConverter.forDocument(schema);
    }

    @Override
    public void open(SerializationSchema.InitializationContext context) {
        random = new SecureRandom();
    }

    @Override
    public FirestoreWrite serialize(RowData row, SinkWriter.Context context) throws IOException {
        try {
            switch (row.getRowKind()) {
                case INSERT:
                case UPDATE_AFTER:
                    return schema.hasPrimaryKey() ? upsert(row) : create(row);
                case DELETE:
                    if (writeMode == WriteMode.UPDATE) {
                        // The sink declares no deletes under update, but the planner still sends
                        // one when it materializes an upsert whose key differs from the table's.
                        throw new IOException(
                                "A DELETE reached a firestore table whose sink.write-mode is"
                                        + " 'update', which takes no deletes. The planner produces"
                                        + " one when the query's key differs from the table's"
                                        + " PRIMARY KEY; key the query by the PRIMARY KEY, or use"
                                        + " 'set' or 'merge'. Nothing was deleted.");
                    }
                    if (!schema.hasPrimaryKey()) {
                        throw new IOException(
                                "A DELETE reached a firestore table without a PRIMARY KEY, which"
                                        + " has no document to delete.");
                    }
                    return FirestoreWrite.delete(documentPath(row));
                default:
                    // The upsert changelog mode the sink declares never delivers one.
                    throw new IOException(
                            "A firestore table sink does not consume " + row.getRowKind() + ".");
            }
        } catch (IllegalArgumentException e) {
            throw new IOException(e.getMessage(), e);
        }
    }

    private FirestoreWrite upsert(RowData row) {
        String path = documentPath(row);
        Map<String, Object> fields = fields(row);
        switch (writeMode) {
            case MERGE:
                return FirestoreWrite.setMerge(path, fields);
            case UPDATE:
                return FirestoreWrite.update(path, fields);
            default:
                return FirestoreWrite.set(path, fields);
        }
    }

    /**
     * A {@code set} under a fresh id rather than a {@code create}: the result is the same new
     * document, but a {@code create} the client library retries after an answer was lost would be
     * refused with {@code ALREADY_EXISTS} and fail the job, where a {@code set} applies again.
     */
    private FirestoreWrite create(RowData row) {
        return FirestoreWrite.set(collection + "/" + autoId(), fields(row));
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> fields(RowData row) {
        return (Map<String, Object>) document.convertNonNull(row);
    }

    /**
     * The document's path from the key column: refused rather than written when the id is empty or
     * holds a {@code '/'}, since either would make the path name another document, or a document in
     * another collection, without an error.
     */
    private String documentPath(RowData row) {
        int key = schema.getKeyIndex();
        String id = row.isNullAt(key) ? null : row.getString(key).toString();
        if (id == null || id.isEmpty() || id.indexOf('/') >= 0) {
            throw new IllegalArgumentException(
                    "The PRIMARY KEY column '"
                            + keyColumn
                            + "' holds "
                            + (id == null ? "NULL" : "'" + id + "'")
                            + ", which is not a document id: an id is not empty and has no '/'.");
        }
        return collection + "/" + id;
    }

    @VisibleForTesting
    String autoId() {
        StringBuilder id = new StringBuilder(AUTO_ID_LENGTH);
        for (int i = 0; i < AUTO_ID_LENGTH; i++) {
            id.append(AUTO_ID_ALPHABET.charAt(random.nextInt(AUTO_ID_ALPHABET.length())));
        }
        return id.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RowDataSerializationSchema that = (RowDataSerializationSchema) o;
        return schema.equals(that.schema)
                && collection.equals(that.collection)
                && writeMode == that.writeMode;
    }

    @Override
    public int hashCode() {
        return Objects.hash(schema, collection, writeMode);
    }
}
