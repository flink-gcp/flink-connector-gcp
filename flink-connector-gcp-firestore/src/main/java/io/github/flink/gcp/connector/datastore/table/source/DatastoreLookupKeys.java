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
import org.apache.flink.table.data.RowData;

import com.google.datastore.v1.Key;
import com.google.datastore.v1.PartitionId;
import io.github.flink.gcp.connector.datastore.DatabaseDestination;
import io.github.flink.gcp.connector.datastore.table.DatastoreTableSchema;

import javax.annotation.Nullable;

import java.io.Serializable;

/**
 * Builds the root key of the table's kind in its namespace that a PRIMARY KEY value names: the key
 * a lookup reads, and the key a pushed key equality compares with ({@code
 * DatastoreFilterPushDown}). A {@code STRING} key column names it, a {@code BIGINT} one numbers it.
 */
@Internal
final class DatastoreLookupKeys implements Serializable {

    private static final long serialVersionUID = 1L;

    private final DatabaseDestination database;
    private final String namespace;
    private final String kind;
    private final boolean id;
    @Nullable private transient volatile PartitionId partition;

    /**
     * Creates the builder.
     *
     * @param database the database
     * @param namespace the namespace, empty for the default one
     * @param kind the kind
     * @param id whether the key column is a numeric id rather than a name
     */
    DatastoreLookupKeys(DatabaseDestination database, String namespace, String kind, boolean id) {
        this.database = database;
        this.namespace = namespace;
        this.kind = kind;
        this.id = id;
    }

    /**
     * Returns the key a lookup key row names, or {@code null} when it can name no entity, so the
     * lookup joins no row without a read: a NULL key, the id {@code 0}, or a name that is empty, of
     * the form {@code __…__} or longer than 1,500 bytes. The emulator refuses a lookup of the empty
     * name, the id {@code 0} or an over-long name with {@code INVALID_ARGUMENT}, which would fail
     * the job for one stream value, and stores no entity under a reserved name. A negative id is an
     * id Datastore stores, and is read.
     */
    @Nullable
    Key key(RowData keyRow) {
        if (keyRow.isNullAt(0)) {
            return null;
        }
        return id ? key(keyRow.getLong(0)) : key(keyRow.getString(0).toString());
    }

    /**
     * Returns the root key numbered by the id, or {@code null} for the id {@code 0}, which names no
     * entity.
     */
    @Nullable
    Key key(long number) {
        if (number == 0) {
            return null;
        }
        return Key.newBuilder()
                .setPartitionId(partition())
                .addPath(Key.PathElement.newBuilder().setKind(kind).setId(number))
                .build();
    }

    /**
     * Returns the root key named by the name, or {@code null} for a name that is empty, of the form
     * {@code __…__} or longer than 1,500 bytes, none of which names an entity.
     */
    @Nullable
    Key key(String name) {
        if (name.isEmpty()
                || DatastoreTableSchema.isReserved(name)
                || DatastoreTableSchema.isTooLong(name)) {
            return null;
        }
        return Key.newBuilder()
                .setPartitionId(partition())
                .addPath(Key.PathElement.newBuilder().setKind(kind).setName(name))
                .build();
    }

    /** The partition every key names, built once per instance after deserialization. */
    private PartitionId partition() {
        PartitionId built = partition;
        if (built == null) {
            built =
                    PartitionId.newBuilder()
                            .setProjectId(database.getProject())
                            .setDatabaseId(database.getDatabaseId())
                            .setNamespaceId(namespace)
                            .build();
            partition = built;
        }
        return built;
    }
}
