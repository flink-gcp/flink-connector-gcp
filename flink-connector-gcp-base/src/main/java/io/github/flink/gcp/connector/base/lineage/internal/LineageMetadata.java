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

package io.github.flink.gcp.connector.base.lineage.internal;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.streaming.api.lineage.LineageVertex;
import org.apache.flink.streaming.api.lineage.SourceLineageVertex;

import io.github.flink.gcp.connector.base.lineage.ResourceIdentifier;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.List;

/**
 * Immutable logical Table identity and DataStream/Table selection for runtime configuration.
 * Physical identities and namespaces remain the caller's; vertices are assembled on inspection.
 */
@Internal
public final class LineageMetadata implements Serializable {
    private static final long serialVersionUID = 1L;

    @Nullable private final String logicalTableName;

    private LineageMetadata(@Nullable String logicalTableName) {
        this.logicalTableName = logicalTableName;
    }

    /** Creates metadata for a logical Table name, or for DataStream when the name is null. */
    public static LineageMetadata of(@Nullable String logicalTableName) {
        return new LineageMetadata(logicalTableName);
    }

    /** Reports configured source resources with the source's actual boundedness. */
    public SourceLineageVertex source(
            Boundedness boundedness, String tableNamespace, List<ResourceIdentifier> resources) {
        return logicalTableName == null
                ? Lineage.source(boundedness, resources)
                : Lineage.tableSource(logicalTableName, tableNamespace, boundedness, resources);
    }

    /** Reports configured sink resources, retaining a logical Table even when they are unknown. */
    public LineageVertex sink(String tableNamespace, List<ResourceIdentifier> resources) {
        return logicalTableName == null
                ? Lineage.sink(resources)
                : Lineage.tableSink(logicalTableName, tableNamespace, resources);
    }
}
