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

package io.github.flink.gcp.connector.firestore.table.source;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Collector;

import com.google.cloud.firestore.DocumentSnapshot;
import com.google.cloud.firestore.FieldPath;
import io.github.flink.gcp.connector.firestore.source.serializer.FirestoreDocumentDeserializationSchema;
import io.github.flink.gcp.connector.firestore.table.FirestoreTableSchema;
import io.github.flink.gcp.connector.firestore.table.TypeMismatchPolicy;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Turns a document into the row a {@code firestore} table scan produces: the read physical columns
 * in the planner's order, then the read metadata. The PRIMARY KEY column is the document id; every
 * other column is the top-level field of its name, NULL when the document has none. A stored value
 * the column cannot represent is handled by the table's {@link TypeMismatchPolicy}.
 */
@Internal
final class RowDataDeserializationSchema
        implements FirestoreDocumentDeserializationSchema<RowData> {

    private static final long serialVersionUID = 1L;

    private final FirestoreTableSchema schema;
    private final TypeMismatchPolicy policy;
    private final int[] columns;
    private final int keyIndex;
    private final String[] names;
    private final FirestoreToRowDataConverter[] converters;
    private final ReadableMetadata[] metadata;
    private final TypeInformation<RowData> producedType;

    /** The field paths of the columns, built once per reader: a FieldPath is not serializable. */
    private transient FieldPath[] paths;

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
            FirestoreTableSchema schema,
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
        this.converters = new FirestoreToRowDataConverter[columns.length];
        for (int i = 0; i < columns.length; i++) {
            int column = columns[i];
            names[i] = rowType.getFieldNames().get(column);
            if (column != keyIndex) {
                converters[i] =
                        FirestoreToRowDataConverter.create(
                                schema, names[i], rowType.getTypeAt(column), policy);
            }
        }
        this.metadata =
                metadataKeys.stream().map(ReadableMetadata::of).toArray(ReadableMetadata[]::new);
        this.producedType = producedType;
    }

    @Override
    public void open(DeserializationSchema.InitializationContext context) {
        paths = new FieldPath[names.length];
        for (int i = 0; i < names.length; i++) {
            paths[i] = FieldPath.of(names[i]);
        }
    }

    @Override
    public void deserialize(DocumentSnapshot document, Collector<RowData> out) throws IOException {
        out.collect(read(document));
    }

    /**
     * Reads one document as its row, the one row {@link #deserialize(DocumentSnapshot, Collector)}
     * collects.
     *
     * @param document the document, which exists
     * @return the row
     * @throws IOException if a value does not match its column and the policy cannot read it
     */
    RowData read(DocumentSnapshot document) throws IOException {
        if (paths == null) {
            open(null);
        }
        GenericRowData row = new GenericRowData(columns.length + metadata.length);
        for (int i = 0; i < columns.length; i++) {
            if (columns[i] == keyIndex) {
                row.setField(i, StringData.fromString(document.getId()));
                continue;
            }
            try {
                // A missing field reads as null, like a field that holds null.
                row.setField(i, converters[i].convert(document.get(paths[i])));
            } catch (FirestoreToRowDataConverter.Mismatch mismatch) {
                throw new IOException(
                        "Document '"
                                + document.getReference().getPath()
                                + "' cannot be read into the table: "
                                + mismatch.getMessage()
                                + (mismatch.readableAsNull()
                                        ? " Fix the document, or set 'type-mismatch-policy' ="
                                                + " 'null' to read such a value as NULL."
                                        : " The column '"
                                                + names[i]
                                                + "' is NOT NULL, so no policy can read it as"
                                                + " NULL; fix the document, or declare the column"
                                                + " nullable."),
                        mismatch);
            }
        }
        for (int i = 0; i < metadata.length; i++) {
            row.setField(columns.length + i, metadata[i].read(document));
        }
        return row;
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
