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

package io.github.flink.gcp.connector.bigtable;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.streaming.api.lineage.LineageVertex;
import org.apache.flink.streaming.api.lineage.SourceLineageVertex;

import io.github.flink.gcp.connector.base.lineage.ResourceIdentifier;
import io.github.flink.gcp.connector.base.lineage.internal.Lineage;
import io.github.flink.gcp.connector.base.lineage.internal.LineageIdentifiers;
import io.github.flink.gcp.connector.bigtable.sink.DestinationResolver;
import io.github.flink.gcp.connector.bigtable.sink.FixedDestinationResolver;

import javax.annotation.Nullable;

import java.util.List;

/** Configured table identities shared by the Bigtable sources and sink families. */
@Internal
public final class BigtableLineage {
    private BigtableLineage() {}

    /** Reports the configured table with the source's actual boundedness. */
    public static SourceLineageVertex source(
            TableDestination table, Boundedness boundedness, @Nullable String logicalName) {
        ResourceIdentifier resource = resource(table);
        return logicalName == null
                ? Lineage.source(boundedness, List.of(resource))
                : Lineage.tableSource(
                        logicalName, resource.namespace(), boundedness, List.of(resource));
    }

    /** Inspects a fixed resolver without evaluating a destination against a record. */
    public static LineageVertex sink(
            DestinationResolver<?> resolver, @Nullable String logicalName) {
        List<ResourceIdentifier> resources =
                resolver instanceof FixedDestinationResolver
                        ? List.of(resource(((FixedDestinationResolver) resolver).getDestination()))
                        : List.of();
        return logicalName == null
                ? Lineage.sink(resources)
                : Lineage.tableSink(
                        logicalName,
                        resources.isEmpty() ? "bigtable" : resources.get(0).namespace(),
                        resources);
    }

    private static ResourceIdentifier resource(TableDestination table) {
        return LineageIdentifiers.bigtableTable(
                table.getProject(), table.getInstance(), table.getTable());
    }
}
