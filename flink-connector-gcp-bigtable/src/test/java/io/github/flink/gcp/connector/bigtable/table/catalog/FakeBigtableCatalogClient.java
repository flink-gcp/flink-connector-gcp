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

package io.github.flink.gcp.connector.bigtable.table.catalog;

import com.google.bigtable.admin.v2.Type;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** An in-memory instance: tables and their families' value types, and every request made. */
final class FakeBigtableCatalogClient implements BigtableCatalogClient {

    private final Map<String, Map<String, Type>> tables = new LinkedHashMap<>();
    final List<String> requests = new ArrayList<>();
    @Nullable RuntimeException failure;

    /** Fails listings only, as a missing instance does once a lookup has found nothing. */
    @Nullable RuntimeException listFailure;

    boolean closed;

    /**
     * Adds a table whose families have the given value types.
     *
     * @param table the table id
     * @param familiesAndTypes alternating family names and {@link Type}s
     * @return this client
     */
    FakeBigtableCatalogClient table(String table, Object... familiesAndTypes) {
        Map<String, Type> families = new LinkedHashMap<>();
        for (int i = 0; i < familiesAndTypes.length; i += 2) {
            families.put((String) familiesAndTypes[i], (Type) familiesAndTypes[i + 1]);
        }
        tables.put(table, families);
        return this;
    }

    @Override
    public List<String> listTables() {
        requests.add("listTables");
        throwIfFailing();
        if (listFailure != null) {
            throw listFailure;
        }
        return new ArrayList<>(tables.keySet());
    }

    @Nullable
    @Override
    public Map<String, Type> columnFamilies(String table) {
        requests.add("columnFamilies " + table);
        throwIfFailing();
        Map<String, Type> families = tables.get(table);
        return families == null ? null : new LinkedHashMap<>(families);
    }

    private void throwIfFailing() {
        if (failure != null) {
            throw failure;
        }
    }

    @Override
    public void close() {
        closed = true;
    }
}
