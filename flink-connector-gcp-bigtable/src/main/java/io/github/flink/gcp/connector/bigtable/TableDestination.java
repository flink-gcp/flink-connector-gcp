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

import org.apache.flink.annotation.Public;

import io.github.flink.gcp.connector.base.options.ResourceNames;

import java.io.Serializable;
import java.util.Objects;

/**
 * A fully-qualified Bigtable table reference: project, instance and table.
 *
 * <p>The instance is part of the identity because a table id is unique only within one instance,
 * and a project may hold several instances.
 *
 * <p>Instances are pure table <em>identity</em>: {@link #equals(Object)} and {@link #hashCode()}
 * are defined over exactly (project, instance, table). Nothing about how the client reaches the
 * table — the application profile that selects routing, the emulator endpoint — belongs here; those
 * are sink options, because they choose a path to the data rather than the data's address.
 *
 * <p>This type sits at the module root rather than under {@code sink} because both directions take
 * the same value; the module's detailed agent guidance records that deviation.
 *
 * <p>Instances are immutable and cheap to reuse.
 */
@Public
public final class TableDestination implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String project;
    private final String instance;
    private final String table;
    private final int hash;

    private TableDestination(String project, String instance, String table) {
        this.project = project;
        this.instance = instance;
        this.table = table;
        this.hash = Objects.hash(project, instance, table);
    }

    /**
     * Creates a {@link TableDestination} from bare ids, not resource paths.
     *
     * @param project the Google Cloud project id
     * @param instance the Bigtable instance id
     * @param table the Bigtable table id
     * @return the destination
     * @throws IllegalArgumentException if a component is null or blank, has leading or trailing
     *     whitespace, or contains {@code '/'} — a separator would make the composed resource path
     *     address a different resource
     */
    public static TableDestination of(String project, String instance, String table) {
        ResourceNames.checkComponent(project, "project");
        ResourceNames.checkComponent(instance, "instance");
        ResourceNames.checkComponent(table, "table");
        return new TableDestination(project, instance, table);
    }

    /** Returns the Google Cloud project id, given as a bare id rather than a resource path. */
    public String getProject() {
        return project;
    }

    /** Returns the Bigtable instance id, given as a bare id rather than a resource path. */
    public String getInstance() {
        return instance;
    }

    /** Returns the Bigtable table id, given as a bare id rather than a resource path. */
    public String getTable() {
        return table;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TableDestination that = (TableDestination) o;
        return project.equals(that.project)
                && instance.equals(that.instance)
                && table.equals(that.table);
    }

    @Override
    public int hashCode() {
        return hash;
    }

    /** Returns the table as {@code project.instance.table}. */
    @Override
    public String toString() {
        return project + "." + instance + "." + table;
    }
}
