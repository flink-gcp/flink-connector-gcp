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

package io.github.flink.gcp.connector.firestore;

import org.apache.flink.annotation.PublicEvolving;

import io.github.flink.gcp.connector.base.options.ResourceNames;

import java.io.Serializable;
import java.util.Objects;

/**
 * A Firestore database in Native mode: a project and a database id.
 *
 * <p>A database, not a collection, is as far as the sink's destination goes: every write names its
 * own document path, and collections are implicit in Firestore — the first write into one creates
 * it. That is why the sink takes no destination resolver and has nothing to create.
 *
 * <p>Instances are pure database <em>identity</em>: {@link #equals(Object)} and {@link #hashCode()}
 * are defined over exactly (project, database id). How the client reaches the database — an
 * emulator endpoint, a service-account key — belongs to the direction that takes it.
 *
 * <p>This type sits at the module root rather than under {@code sink} because the later source and
 * table directions take the same value.
 *
 * <p>Instances are immutable and cheap to reuse.
 */
@PublicEvolving
public final class DatabaseDestination implements Serializable {

    private static final long serialVersionUID = 1L;

    /** The id of the database every project is created with. */
    public static final String DEFAULT_DATABASE_ID = "(default)";

    private final String project;
    private final String databaseId;

    private DatabaseDestination(String project, String databaseId) {
        this.project = project;
        this.databaseId = databaseId;
    }

    /**
     * Creates a reference to a project's {@value #DEFAULT_DATABASE_ID} database.
     *
     * @param project the Google Cloud project id, as a bare id rather than a resource path
     * @return the database reference
     * @throws IllegalArgumentException if the project is null or blank, has leading or trailing
     *     whitespace, or contains {@code '/'} — a separator would make the composed resource path
     *     address a different resource
     */
    public static DatabaseDestination of(String project) {
        return of(project, DEFAULT_DATABASE_ID);
    }

    /**
     * Creates a reference to a named database.
     *
     * @param project the Google Cloud project id, as a bare id rather than a resource path
     * @param databaseId the database id, for example {@value #DEFAULT_DATABASE_ID}
     * @return the database reference
     * @throws IllegalArgumentException if a component is null or blank, has leading or trailing
     *     whitespace, or contains {@code '/'} — a separator would make the composed resource path
     *     address a different resource
     */
    public static DatabaseDestination of(String project, String databaseId) {
        ResourceNames.checkComponent(project, "project");
        ResourceNames.checkComponent(databaseId, "databaseId");
        return new DatabaseDestination(project, databaseId);
    }

    /** Returns the Google Cloud project id. */
    public String getProject() {
        return project;
    }

    /** Returns the database id. */
    public String getDatabaseId() {
        return databaseId;
    }

    /**
     * Returns the database as the resource name Firestore itself uses, {@code
     * projects/P/databases/D}.
     */
    @Override
    public String toString() {
        return "projects/" + project + "/databases/" + databaseId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DatabaseDestination that = (DatabaseDestination) o;
        return project.equals(that.project) && databaseId.equals(that.databaseId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(project, databaseId);
    }
}
