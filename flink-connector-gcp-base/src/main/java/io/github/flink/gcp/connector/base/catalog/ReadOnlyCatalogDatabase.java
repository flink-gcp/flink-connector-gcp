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

package io.github.flink.gcp.connector.base.catalog;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.catalog.CatalogDatabase;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * A service database as a Flink catalog database: its description, if the service keeps one, is the
 * comment.
 *
 * <p>Its own class rather than Flink's {@code CatalogDatabaseImpl}, which is {@code @Internal}
 * (docs/adr/0168).
 */
@Internal
public final class ReadOnlyCatalogDatabase implements CatalogDatabase {

    private final Map<String, String> properties;
    @Nullable private final String comment;

    /**
     * Creates a database value.
     *
     * @param properties the database's properties, copied
     * @param comment the service's description of the database, or {@code null}
     */
    public ReadOnlyCatalogDatabase(Map<String, String> properties, @Nullable String comment) {
        this.properties = Collections.unmodifiableMap(new HashMap<>(properties));
        this.comment = comment;
    }

    @Override
    public Map<String, String> getProperties() {
        return properties;
    }

    @Override
    @Nullable
    public String getComment() {
        return comment;
    }

    @Override
    public CatalogDatabase copy() {
        return copy(properties);
    }

    @Override
    public CatalogDatabase copy(Map<String, String> properties) {
        return new ReadOnlyCatalogDatabase(properties, comment);
    }

    @Override
    public Optional<String> getDescription() {
        return Optional.ofNullable(comment);
    }

    @Override
    public Optional<String> getDetailedDescription() {
        return Optional.ofNullable(comment);
    }
}
