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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

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
 * <p>This type sits at the module root rather than under {@code sink} because the scan source will
 * take the same value ({@code #216}); the module's detailed agent guidance records that deviation.
 *
 * <p>Instances are immutable and cheap to reuse.
 */
@PublicEvolving
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
     * Creates a {@link TableDestination}.
     *
     * @param project the Google Cloud project id
     * @param instance the Bigtable instance id
     * @param table the Bigtable table id
     * @return the destination
     */
    public static TableDestination of(String project, String instance, String table) {
        checkComponent(project, "project");
        checkComponent(instance, "instance");
        checkComponent(table, "table");
        return new TableDestination(project, instance, table);
    }

    private static void checkComponent(String value, String name) {
        Preconditions.checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(value), "%s must not be blank", name);
        Preconditions.checkArgument(
                value.equals(value.trim()),
                "%s must not have leading or trailing whitespace: '%s'",
                name,
                value);
        Preconditions.checkArgument(
                value.indexOf('/') < 0, "%s must not contain '/': '%s'", name, value);
    }

    /** Returns the Google Cloud project id. */
    public String getProject() {
        return project;
    }

    /** Returns the Bigtable instance id. */
    public String getInstance() {
        return instance;
    }

    /** Returns the Bigtable table id. */
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
