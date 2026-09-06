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

package io.github.flink.gcp.connector.base.lineage;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.PublicEvolving;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * A configured physical resource reported in a {@link PhysicalResourceFacet}.
 *
 * <p>The namespace and name identify a lineage dataset. The identity retains resource components
 * that a rendered name can obscure, including configured Spanner identifier quoting. It contains
 * resource identifiers only, never credentials, SQL, record data, or a connector option map.
 * Equality includes all four properties, so distinct configured identities are not conflated.
 *
 * <p>This read-only listener API keeps its package name in every SQL connector jar.
 */
@PublicEvolving
public final class ResourceIdentifier implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String kind;
    private final String namespace;
    private final String name;
    private final Map<String, String> identity;

    /**
     * Constructs a snapshot for connector-internal metadata assembly.
     *
     * <p>This is not a user-facing dataset declaration API. The constructor is public so relocated
     * connector helpers can construct the unrelocated listener value.
     *
     * @param kind the resource kind supplied by the connector's resource factory
     * @param namespace the canonical dataset namespace
     * @param name the canonical dataset name
     * @param identity resource identity components only; keys and values must be non-null
     */
    @Internal
    public ResourceIdentifier(
            String kind, String namespace, String name, Map<String, String> identity) {
        this.kind = Objects.requireNonNull(kind, "kind");
        this.namespace = Objects.requireNonNull(namespace, "namespace");
        this.name = Objects.requireNonNull(name, "name");
        TreeMap<String, String> snapshot = new TreeMap<>();
        identity.forEach(
                (key, value) ->
                        snapshot.put(
                                Objects.requireNonNull(key, "identity key"),
                                Objects.requireNonNull(value, "identity value")));
        this.identity = Collections.unmodifiableMap(snapshot);
    }

    /**
     * Returns the physical resource kind: {@code bigquery-table}, {@code pubsub-topic}, {@code
     * pubsub-subscription}, {@code bigtable-table}, {@code spanner-table}, {@code
     * spanner-change-stream}, or {@code cloudtasks-queue}. A BigQuery table identity can also name
     * an explicitly configured view; extraction does not discover its service-side object type.
     */
    public String kind() {
        return kind;
    }

    /** Returns the canonical physical dataset namespace. */
    public String namespace() {
        return namespace;
    }

    /** Returns the canonical physical dataset name. */
    public String name() {
        return name;
    }

    /**
     * Returns immutable configured resource components, ordered by key.
     *
     * <p>Keys are the applicable subset of {@code project}, {@code dataset}, {@code table}, {@code
     * topic}, {@code subscription}, {@code instance}, {@code database}, {@code schema}, {@code
     * stream}, {@code location}, and {@code queue}. An absent schema is omitted. Spanner schema and
     * table values retain their configured syntax rather than being split from {@link #name()}.
     */
    public Map<String, String> identity() {
        return identity;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof ResourceIdentifier)) {
            return false;
        }
        ResourceIdentifier that = (ResourceIdentifier) other;
        return kind.equals(that.kind)
                && namespace.equals(that.namespace)
                && name.equals(that.name)
                && identity.equals(that.identity);
    }

    @Override
    public int hashCode() {
        return Objects.hash(kind, namespace, name, identity);
    }
}
