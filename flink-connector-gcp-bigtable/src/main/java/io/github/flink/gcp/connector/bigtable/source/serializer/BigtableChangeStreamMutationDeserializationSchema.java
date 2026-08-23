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

package io.github.flink.gcp.connector.bigtable.source.serializer;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;

import io.github.flink.gcp.connector.bigtable.source.changestream.BigtableChangeStreamMutation;

/** Emits the connector-owned {@link BigtableChangeStreamMutation} unchanged. */
@PublicEvolving
public final class BigtableChangeStreamMutationDeserializationSchema
        implements BigtableChangeStreamDeserializationSchema<BigtableChangeStreamMutation> {

    private static final long serialVersionUID = 1L;
    private static final TypeInformation<BigtableChangeStreamMutation> TYPE_INFORMATION =
            TypeInformation.of(BigtableChangeStreamMutation.class);

    @Override
    public void deserialize(
            BigtableChangeStreamMutation mutation, Collector<BigtableChangeStreamMutation> out) {
        out.collect(mutation);
    }

    @Override
    public TypeInformation<BigtableChangeStreamMutation> getProducedType() {
        return TYPE_INFORMATION;
    }
}
