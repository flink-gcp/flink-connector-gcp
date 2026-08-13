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
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import com.google.cloud.bigtable.data.v2.models.ChangeStreamMutation;
import io.github.flink.gcp.connector.bigtable.source.serializer.BigtableChangeStreamDeserializationSchema;

import java.io.IOException;

/** Delegates mutation conversion while retaining the planner-produced type information. */
@Internal
final class ChangeStreamMutationRowDataDeserializationSchema
        implements BigtableChangeStreamDeserializationSchema<RowData> {

    private static final long serialVersionUID = 1L;

    private final ChangeStreamMutationToRowDataConverter converter =
            new ChangeStreamMutationToRowDataConverter();
    private final TypeInformation<RowData> producedType;

    ChangeStreamMutationRowDataDeserializationSchema(TypeInformation<RowData> producedType) {
        this.producedType =
                Preconditions.checkNotNull(producedType, "producedType must not be null");
    }

    @Override
    public void deserialize(ChangeStreamMutation mutation, Collector<RowData> out)
            throws IOException {
        out.collect(converter.convert(mutation));
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return producedType;
    }
}
