/*
 * Copyright 2026 laughingman7743
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

package io.github.flink.gcp.connector.bigtable.table.source;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import io.github.flink.gcp.connector.bigtable.source.changestream.ChangeStreamMutation;
import io.github.flink.gcp.connector.bigtable.source.serializer.BigtableChangeStreamDeserializationSchema;

import java.io.IOException;

/** Delegates mutation conversion while retaining the planner-produced type information. */
@Internal
final class ChangeStreamMutationRowDataDeserializationSchema
        implements BigtableChangeStreamDeserializationSchema<RowData> {

    private static final long serialVersionUID = 1L;

    private final ChangeStreamMutationToRowDataConverter converter =
            new ChangeStreamMutationToRowDataConverter();
    private final ChangeStreamReadableMetadata[] metadata;
    private final TypeInformation<RowData> producedType;

    ChangeStreamMutationRowDataDeserializationSchema(TypeInformation<RowData> producedType) {
        this(new ChangeStreamReadableMetadata[0], producedType);
    }

    ChangeStreamMutationRowDataDeserializationSchema(
            ChangeStreamReadableMetadata[] metadata, TypeInformation<RowData> producedType) {
        this.metadata = Preconditions.checkNotNull(metadata, "metadata must not be null").clone();
        this.producedType =
                Preconditions.checkNotNull(producedType, "producedType must not be null");
    }

    @Override
    public void deserialize(ChangeStreamMutation mutation, Collector<RowData> out)
            throws IOException {
        RowData physical = converter.convert(mutation);
        if (metadata.length == 0) {
            out.collect(physical);
            return;
        }
        GenericRowData metadataRow = new GenericRowData(metadata.length);
        for (int index = 0; index < metadata.length; index++) {
            metadataRow.setField(index, metadata[index].read(mutation));
        }
        out.collect(new JoinedRowData(physical.getRowKind(), physical, metadataRow));
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return producedType;
    }
}
