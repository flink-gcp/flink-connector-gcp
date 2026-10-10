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

package io.github.flink.gcp.connector.datastore.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceSplit;

import io.github.flink.gcp.connector.base.lineage.ResourceIdentifier;
import io.github.flink.gcp.connector.base.lineage.internal.TableLineageSink;
import io.github.flink.gcp.connector.base.lineage.internal.TableLineageSource;
import io.github.flink.gcp.connector.datastore.DatabaseDestination;
import io.github.flink.gcp.connector.datastore.DatastoreLineage;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

/**
 * The physical identity of a {@code datastore} table, carried from the factory to the runtime
 * providers it wraps (ADR-0160): the kind the table names, in its namespace and database, reported
 * under the table's catalog name.
 */
@Internal
public final class DatastoreTableLineage implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String logicalName;
    private final String namespace;
    private final List<ResourceIdentifier> resources;

    private DatastoreTableLineage(String logicalName, ResourceIdentifier resource) {
        this.logicalName = logicalName;
        this.namespace = resource.namespace();
        this.resources = List.of(resource);
    }

    /**
     * Captures the configured kind, before any runtime client exists.
     *
     * @param logicalName the table's catalog name
     * @param database the database the table is in
     * @param namespace the entities' namespace, empty for the default one
     * @param kind the kind the table names
     * @return the lineage
     */
    public static DatastoreTableLineage of(
            String logicalName, DatabaseDestination database, String namespace, String kind) {
        return new DatastoreTableLineage(
                logicalName, DatastoreLineage.kind(database, namespace, kind));
    }

    /**
     * Wraps a source so that it reports this table's lineage, keeping its runtime operations and
     * produced type.
     *
     * @param source the source
     * @param type the produced type
     * @param <T> the produced type
     * @param <S> the split type
     * @param <E> the enumerator checkpoint type
     * @return the wrapped source
     */
    public <T, S extends SourceSplit, E> Source<T, S, E> source(
            Source<T, S, E> source, TypeInformation<T> type) {
        return TableLineageSource.of(source, type, logicalName, namespace, resources);
    }

    /**
     * Wraps a sink so that it reports this table's lineage, keeping its writer creation path.
     *
     * @param sink the sink
     * @param <T> the sink's input type
     * @return the wrapped sink
     */
    public <T> Sink<T> sink(Sink<T> sink) {
        return TableLineageSink.of(sink, logicalName, namespace, resources);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof DatastoreTableLineage)) {
            return false;
        }
        DatastoreTableLineage that = (DatastoreTableLineage) other;
        return logicalName.equals(that.logicalName)
                && namespace.equals(that.namespace)
                && resources.equals(that.resources);
    }

    @Override
    public int hashCode() {
        return Objects.hash(logicalName, namespace, resources);
    }
}
