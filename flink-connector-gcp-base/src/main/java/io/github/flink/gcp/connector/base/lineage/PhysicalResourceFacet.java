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
import org.apache.flink.streaming.api.lineage.LineageDatasetFacet;

import java.io.Serializable;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Immutable physical resource metadata under the {@code gcp} dataset facet key.
 *
 * <p>A DataStream dataset contains its own resource. A Table dataset contains every configured
 * physical resource of that logical table, even when Flink replaces the dataset name with a catalog
 * identifier. An empty resource list declares no physical resource.
 *
 * <p>A listener must explicitly consume this facet; its presence does not imply that an unmodified
 * OpenLineage listener understands it. This class and {@link ResourceIdentifier} retain their
 * package names in SQL connector jars, and must share a class loader with the listener's copies
 * when accessed through this API.
 */
@PublicEvolving
public final class PhysicalResourceFacet implements LineageDatasetFacet, Serializable {

    private static final long serialVersionUID = 1L;

    private final List<ResourceIdentifier> resources;

    /**
     * Constructs a snapshot for connector-internal metadata assembly.
     *
     * <p>This public internal constructor also serves relocated helpers. It copies the list,
     * eliminates equal identifiers, and orders by namespace, name, kind, then identity entries.
     *
     * @param resources the configured resources; the list and its entries must be non-null
     */
    @Internal
    public PhysicalResourceFacet(List<ResourceIdentifier> resources) {
        this.resources =
                resources.stream()
                        .map(resource -> Objects.requireNonNull(resource, "resource"))
                        .distinct()
                        .sorted(
                                Comparator.comparing(ResourceIdentifier::namespace)
                                        .thenComparing(ResourceIdentifier::name)
                                        .thenComparing(ResourceIdentifier::kind)
                                        .thenComparing(
                                                ResourceIdentifier::identity,
                                                PhysicalResourceFacet::compareIdentity))
                        .collect(Collectors.toUnmodifiableList());
    }

    @Override
    public String name() {
        return "gcp";
    }

    /**
     * Returns the immutable, deterministically ordered resource snapshot with equal identifiers
     * removed. A listener can read the canonical physical names here after SQL planning.
     */
    public List<ResourceIdentifier> resources() {
        return resources;
    }

    private static int compareIdentity(Map<String, String> left, Map<String, String> right) {
        Iterator<Map.Entry<String, String>> a = left.entrySet().iterator();
        Iterator<Map.Entry<String, String>> b = right.entrySet().iterator();
        while (a.hasNext() && b.hasNext()) {
            Map.Entry<String, String> x = a.next();
            Map.Entry<String, String> y = b.next();
            int key = x.getKey().compareTo(y.getKey());
            if (key != 0) {
                return key;
            }
            int value = x.getValue().compareTo(y.getValue());
            if (value != 0) {
                return value;
            }
        }
        return Boolean.compare(a.hasNext(), b.hasNext());
    }
}
