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

package io.github.flink.gcp.connector.datastore.table.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.table.data.RowData;

import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.IncompleteKey;
import com.google.cloud.datastore.Key;
import io.github.flink.gcp.connector.datastore.DatabaseDestination;
import io.github.flink.gcp.connector.datastore.sink.DatastoreKeyAllocator;
import io.github.flink.gcp.connector.datastore.sink.DatastoreMutation;
import io.github.flink.gcp.connector.datastore.sink.serializer.KeyAllocatingSerializationSchema;
import io.github.flink.gcp.connector.datastore.table.DatastoreTableSchema;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Objects;

/**
 * Turns a table row into the entity write the {@code datastore} table sink sends.
 *
 * <p>With a PRIMARY KEY, the key column is the key's name or numeric id under the table's kind, in
 * its namespace, and every other column a property: an insert or an update-after is an {@code
 * upsert}, and a delete deletes the entity. Without one, every row is upserted under a new key
 * whose numeric id the service allocated, drawn from batches the writer's allocator fetches: the
 * service never hands an allocated id out again, so a row cannot replace an entity another row
 * wrote.
 *
 * <p>Every write is an {@code upsert} or a {@code delete}, the two a replay leaves as they were: an
 * {@code insert} replayed after a restart would be refused with {@code ALREADY_EXISTS} and an
 * {@code update} of a deleted entity with {@code NOT_FOUND}, and either fails the job again on
 * every restart.
 */
@Internal
public final class RowDataSerializationSchema implements KeyAllocatingSerializationSchema<RowData> {

    private static final long serialVersionUID = 1L;

    private final DatastoreTableSchema schema;
    private final DatabaseDestination database;
    private final String namespace;
    private final String kind;
    @Nullable private final String keyColumn;
    private final RowDataToDatastoreConverter.Properties properties;

    private transient DatastoreKeyAllocator allocator;
    private transient Deque<Key> allocated;
    private transient IncompleteKey template;

    /**
     * Creates the schema.
     *
     * @param schema the checked table schema
     * @param database the database the keys address
     * @param namespace the entities' namespace, empty for the default one
     * @param kind the entities' kind
     */
    public RowDataSerializationSchema(
            DatastoreTableSchema schema,
            DatabaseDestination database,
            String namespace,
            String kind) {
        this.schema = schema;
        this.database = database;
        this.namespace = namespace;
        this.kind = kind;
        this.keyColumn =
                schema.hasPrimaryKey()
                        ? schema.getRowType().getFieldNames().get(schema.getKeyIndex())
                        : null;
        this.properties = RowDataToDatastoreConverter.forEntity(schema);
    }

    @Override
    public void setKeyAllocator(DatastoreKeyAllocator allocator) {
        this.allocator = allocator;
        this.allocated = new ArrayDeque<>();
    }

    @Override
    public DatastoreMutation serialize(RowData row, SinkWriter.Context context) throws IOException {
        try {
            switch (row.getRowKind()) {
                case INSERT:
                case UPDATE_AFTER:
                    Key key = schema.hasPrimaryKey() ? key(row) : newKey();
                    return DatastoreMutation.upsert(
                            properties.setAll(row, Entity.newBuilder(key)).build());
                case DELETE:
                    if (!schema.hasPrimaryKey()) {
                        throw new IOException(
                                "A DELETE reached a datastore table without a PRIMARY KEY, which"
                                        + " has no entity to delete.");
                    }
                    return DatastoreMutation.delete(key(row));
                default:
                    // The upsert changelog mode the sink declares never delivers one.
                    throw new IOException(
                            "A datastore table sink does not consume " + row.getRowKind() + ".");
            }
        } catch (IllegalArgumentException e) {
            throw new IOException(e.getMessage(), e);
        }
    }

    /**
     * The entity's key from the key column. A NULL key, an empty name and the id 0 address no
     * entity, so they are refused rather than written.
     */
    private Key key(RowData row) {
        int index = schema.getKeyIndex();
        if (row.isNullAt(index)) {
            throw new IllegalArgumentException(
                    "The PRIMARY KEY column '" + keyColumn + "' holds NULL, which is not a key.");
        }
        Key.Builder key;
        if (schema.isKeyId()) {
            long id = row.getLong(index);
            if (id == 0) {
                throw new IllegalArgumentException(
                        "The PRIMARY KEY column '"
                                + keyColumn
                                + "' holds 0, which is not a key id: an id is not zero.");
            }
            key = Key.newBuilder(database.getProject(), kind, id, database.getDatabaseId());
        } else {
            String name = row.getString(index).toString();
            if (name.isEmpty()) {
                throw new IllegalArgumentException(
                        "The PRIMARY KEY column '"
                                + keyColumn
                                + "' holds '', which is not a key name: a name is not empty.");
            }
            key = Key.newBuilder(database.getProject(), kind, name, database.getDatabaseId());
        }
        return key.setNamespace(namespace).build();
    }

    /**
     * The next allocated key, fetching a block of them when none is left. Only the id is taken from
     * the answer; project, database, namespace and kind are this schema's own, so the key cannot
     * address another partition than the one the table names.
     */
    private Key newKey() throws IOException {
        if (allocator == null) {
            throw new IOException(
                    "A datastore table without a PRIMARY KEY writes under ids the service"
                            + " allocates, and no allocator was given: the schema runs only inside"
                            + " the Datastore sink's writer.");
        }
        if (template == null) {
            template =
                    IncompleteKey.newBuilder(database.getProject(), kind)
                            .setDatabaseId(database.getDatabaseId())
                            .setNamespace(namespace)
                            .build();
        }
        if (allocated.isEmpty()) {
            allocated.addAll(allocator.allocate(template));
        }
        Key answered = allocated.poll();
        if (!answered.hasId()) {
            throw new IOException(
                    "Datastore answered an id allocation with " + answered + ", which has no id.");
        }
        return Key.newBuilder(template, answered.getId()).build();
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
                && database.equals(that.database)
                && namespace.equals(that.namespace)
                && kind.equals(that.kind);
    }

    @Override
    public int hashCode() {
        return Objects.hash(schema, database, namespace, kind);
    }
}
