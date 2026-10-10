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
 * Builds the key a lookup reads from the PRIMARY KEY value: a root key of the table's kind in its
 * namespace, named by a {@code STRING} key column or numbered by a {@code BIGINT} one.
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
        Key.PathElement.Builder element = Key.PathElement.newBuilder().setKind(kind);
        if (id) {
            long value = keyRow.getLong(0);
            if (value == 0) {
                return null;
            }
            element.setId(value);
        } else {
            String name = keyRow.getString(0).toString();
            if (name.isEmpty()
                    || DatastoreTableSchema.isReserved(name)
                    || DatastoreTableSchema.isTooLong(name)) {
                return null;
            }
            element.setName(name);
        }
        return Key.newBuilder().setPartitionId(partition()).addPath(element).build();
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
