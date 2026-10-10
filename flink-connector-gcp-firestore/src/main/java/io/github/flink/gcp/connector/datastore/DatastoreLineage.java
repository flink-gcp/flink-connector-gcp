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

package io.github.flink.gcp.connector.datastore;

import org.apache.flink.annotation.Internal;

import io.github.flink.gcp.connector.base.lineage.ResourceIdentifier;
import io.github.flink.gcp.connector.base.lineage.internal.LineageIdentifiers;

/**
 * The lineage identity of a kind, shared by the source and the table sink so that both report the
 * same resource for it: the Datastore API names the default database with the empty id, and lineage
 * spells it {@value DatabaseDestination#DEFAULT_DATABASE_NAME}.
 */
@Internal
public final class DatastoreLineage {

    private DatastoreLineage() {}

    /**
     * Names a kind in a namespace of a database.
     *
     * @param database the database
     * @param namespace the namespace, empty for the default one
     * @param kind the kind
     * @return the {@code datastore-kind} resource
     */
    public static ResourceIdentifier kind(
            DatabaseDestination database, String namespace, String kind) {
        return LineageIdentifiers.datastoreKind(
                database.getProject(),
                database.getDatabaseId().isEmpty()
                        ? DatabaseDestination.DEFAULT_DATABASE_NAME
                        : database.getDatabaseId(),
                namespace,
                kind);
    }
}
