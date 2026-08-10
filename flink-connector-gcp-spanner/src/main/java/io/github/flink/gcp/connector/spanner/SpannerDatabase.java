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

package io.github.flink.gcp.connector.spanner;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import java.io.Serializable;
import java.util.Objects;

/**
 * A fully-qualified Spanner database reference: project, instance and database.
 *
 * <p>The instance is part of the identity because a database id is unique only within one instance,
 * and a project may hold several instances.
 *
 * <p>A database, not a table, is as far as the sink's destination goes: a {@code Mutation} names
 * the table it applies to, so one sink writes to as many tables as its serializer produces. The
 * other sinks of this project take a destination resolver for that; here the mutation already
 * carries the answer, which is why there is none — and why the mutation-cell weights are read for
 * the whole database rather than for one table.
 *
 * <p>Instances are pure database <em>identity</em>: {@link #equals(Object)} and {@link #hashCode()}
 * are defined over exactly (project, instance, database). Nothing about how the client reaches the
 * database — the emulator endpoint, the RPC priority — belongs here; those are options of the
 * direction that takes them, because they choose a path to the data rather than the data's address.
 *
 * <p>This type sits at the module root rather than under {@code sink} because the source takes the
 * same value, as the planned change-stream source will.
 *
 * <p>Instances are immutable and cheap to reuse.
 */
@PublicEvolving
public final class SpannerDatabase implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String project;
    private final String instance;
    private final String database;
    private final int hash;

    private SpannerDatabase(String project, String instance, String database) {
        this.project = project;
        this.instance = instance;
        this.database = database;
        this.hash = Objects.hash(project, instance, database);
    }

    /**
     * Creates a {@link SpannerDatabase}.
     *
     * @param project the Google Cloud project id
     * @param instance the Spanner instance id
     * @param database the Spanner database id
     * @return the database reference
     */
    public static SpannerDatabase of(String project, String instance, String database) {
        checkComponent(project, "project");
        checkComponent(instance, "instance");
        checkComponent(database, "database");
        return new SpannerDatabase(project, instance, database);
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

    /** Returns the Spanner instance id. */
    public String getInstance() {
        return instance;
    }

    /** Returns the Spanner database id. */
    public String getDatabase() {
        return database;
    }

    /**
     * Returns the database as the resource name Spanner itself uses, {@code
     * projects/P/instances/I/databases/D}.
     */
    @Override
    public String toString() {
        return "projects/" + project + "/instances/" + instance + "/databases/" + database;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SpannerDatabase that = (SpannerDatabase) o;
        return project.equals(that.project)
                && instance.equals(that.instance)
                && database.equals(that.database);
    }

    @Override
    public int hashCode() {
        return hash;
    }
}
