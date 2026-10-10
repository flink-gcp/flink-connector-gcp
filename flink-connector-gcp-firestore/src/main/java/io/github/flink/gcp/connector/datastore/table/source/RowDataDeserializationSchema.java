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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Collector;

import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.Key;
import io.github.flink.gcp.connector.datastore.source.serializer.DatastoreEntityMetadataDeserializationSchema;
import io.github.flink.gcp.connector.datastore.source.serializer.EntityMetadata;
import io.github.flink.gcp.connector.datastore.table.DatastoreTableSchema;
import io.github.flink.gcp.connector.datastore.table.TypeMismatchPolicy;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Turns an entity into the row a {@code datastore} table scan produces: the read physical columns
 * in the planner's order, then the read metadata. The PRIMARY KEY column is the key's name or
 * numeric id; every other column is the top-level property of its name, NULL when the entity has
 * none. A stored value the column cannot represent is handled by the table's {@link
 * TypeMismatchPolicy}.
 */
@Internal
final class RowDataDeserializationSchema
        implements DatastoreEntityMetadataDeserializationSchema<RowData> {

    private static final long serialVersionUID = 1L;

    private final DatastoreTableSchema schema;
    private final TypeMismatchPolicy policy;
    private final int[] columns;
    private final int keyIndex;
    private final String[] names;
    private final DatastoreToRowDataConverter[] converters;
    private final ReadableMetadata[] metadata;
    private final TypeInformation<RowData> producedType;

    /**
     * Creates the schema.
     *
     * @param schema the checked table schema
     * @param columns the physical columns to read, by index, in the produced order
     * @param metadataKeys the metadata keys to read after them
     * @param policy what a mismatched value does
     * @param producedType the produced row's type information
     */
    RowDataDeserializationSchema(
            DatastoreTableSchema schema,
            int[] columns,
            List<String> metadataKeys,
            TypeMismatchPolicy policy,
            TypeInformation<RowData> producedType) {
        RowType rowType = schema.getRowType();
        this.schema = schema;
        this.policy = policy;
        this.columns = columns.clone();
        this.keyIndex = schema.getKeyIndex();
        this.names = new String[columns.length];
        this.converters = new DatastoreToRowDataConverter[columns.length];
        for (int i = 0; i < columns.length; i++) {
            int column = columns[i];
            names[i] = rowType.getFieldNames().get(column);
            if (column != keyIndex) {
                converters[i] =
                        DatastoreToRowDataConverter.create(
                                names[i], rowType.getTypeAt(column), policy);
            }
        }
        this.metadata =
                metadataKeys.stream().map(ReadableMetadata::of).toArray(ReadableMetadata[]::new);
        this.producedType = producedType;
    }

    @Override
    public void deserialize(Entity entity, EntityMetadata entityMetadata, Collector<RowData> out)
            throws IOException {
        out.collect(read(entity, entityMetadata));
    }

    /**
     * Reads one entity as its row.
     *
     * @param entity the entity
     * @param entityMetadata what its query result carries
     * @return the row
     * @throws IOException if a value does not match its column and the policy cannot read it
     */
    RowData read(Entity entity, EntityMetadata entityMetadata) throws IOException {
        // Checked whether or not the query reads the key column: the planner prunes it from a
        // query it rewrites on the key's uniqueness, such as a GROUP BY over the key.
        Object key = schema.hasPrimaryKey() ? key(entity.getKey()) : null;
        GenericRowData row = new GenericRowData(columns.length + metadata.length);
        for (int i = 0; i < columns.length; i++) {
            if (columns[i] == keyIndex) {
                row.setField(i, key);
                continue;
            }
            try {
                row.setField(
                        i,
                        converters[i].convert(
                                DatastoreToRowDataConverter.property(entity, names[i])));
            } catch (DatastoreToRowDataConverter.Mismatch mismatch) {
                throw new IOException(
                        "Entity "
                                + entity.getKey()
                                + " cannot be read into the table: "
                                + mismatch.getMessage()
                                + (mismatch.readableAsNull()
                                        ? " Fix the entity, or set 'type-mismatch-policy' = 'null'"
                                                + " to read such a value as NULL."
                                        : " The column '"
                                                + names[i]
                                                + "' is NOT NULL, so no policy can read it as"
                                                + " NULL; fix the entity, or declare the column"
                                                + " nullable."),
                        mismatch);
            }
        }
        for (int i = 0; i < metadata.length; i++) {
            row.setField(columns.length + i, metadata[i].read(entity, entityMetadata));
        }
        return row;
    }

    /**
     * The key column's value: the key's name into a STRING column, its numeric id into a BIGINT
     * one. A key of the other form fails the read whatever the policy: the key column is NOT NULL.
     * So does a key with a parent, which a kind query also returns: the column holds only the last
     * name or id, so two children of different parents would read as one key, which the planner
     * trusts to be unique.
     */
    private Object key(Key key) throws IOException {
        String column = schema.getRowType().getFieldNames().get(keyIndex);
        if (key.getParent() != null) {
            throw new IOException(
                    "Entity "
                            + key
                            + " cannot be read into the table: its key has the parent "
                            + key.getParent()
                            + ", but the PRIMARY KEY column '"
                            + column
                            + "' holds the key of an entity without one. Read the kind through a"
                            + " table without a PRIMARY KEY.");
        }
        if (schema.isKeyId()) {
            if (key.hasId()) {
                return key.getId();
            }
        } else if (key.hasName()) {
            return StringData.fromString(key.getName());
        }
        throw new IOException(
                "Entity "
                        + key
                        + " cannot be read into the table: its key has "
                        + (key.hasId()
                                ? "the numeric id " + key.getId()
                                : "the name '" + key.getName() + "'")
                        + ", but the PRIMARY KEY column '"
                        + column
                        + "' is "
                        + (schema.isKeyId() ? "BIGINT, a numeric id" : "STRING, a name")
                        + ". Declare the column for the keys the kind holds, or read them through"
                        + " the 'key-name' and 'key-id' metadata of a table without a PRIMARY KEY.");
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return producedType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RowDataDeserializationSchema that = (RowDataDeserializationSchema) o;
        return schema.equals(that.schema)
                && policy == that.policy
                && Arrays.equals(columns, that.columns)
                && Arrays.equals(metadata, that.metadata)
                && producedType.equals(that.producedType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(schema, policy, Arrays.hashCode(columns), Arrays.hashCode(metadata));
    }
}
