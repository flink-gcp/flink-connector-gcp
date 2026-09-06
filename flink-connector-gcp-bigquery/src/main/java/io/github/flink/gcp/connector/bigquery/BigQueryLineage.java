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

package io.github.flink.gcp.connector.bigquery;

import org.apache.flink.annotation.Internal;

import io.github.flink.gcp.connector.base.lineage.ResourceIdentifier;
import io.github.flink.gcp.connector.base.lineage.internal.LineageIdentifiers;
import io.github.flink.gcp.connector.bigquery.sink.DestinationResolver;
import io.github.flink.gcp.connector.bigquery.sink.FixedDestinationResolver;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;

import javax.annotation.Nullable;

import java.util.List;

/** Inspects configured BigQuery resources without evaluating a resolver or opening a client. */
@Internal
public final class BigQueryLineage {

    private BigQueryLineage() {}

    /** Returns the configured input, or no physical resource for an arbitrary query. */
    public static List<ResourceIdentifier> resources(@Nullable TableDestination table) {
        return table == null
                ? List.of()
                : List.of(
                        LineageIdentifiers.bigQueryTable(
                                table.getProject(), table.getDataset(), table.getTable()));
    }

    /** Returns a fixed output without invoking user destination resolution. */
    public static List<ResourceIdentifier> resources(DestinationResolver<?> resolver) {
        return resolver instanceof FixedDestinationResolver
                ? resources(((FixedDestinationResolver) resolver).getDestination())
                : List.of();
    }
}
