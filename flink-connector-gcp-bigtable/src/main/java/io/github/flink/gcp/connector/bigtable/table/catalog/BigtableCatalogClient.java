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

import org.apache.flink.annotation.Internal;

import com.google.bigtable.admin.v2.Type;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;

/**
 * The metadata requests the {@code bigtable} catalog makes of one instance: its table ids, and one
 * table's column families. A seam, so the catalog's naming and mapping rules are tested against a
 * hand-written fake; the emulator cannot create an aggregate family.
 *
 * <p>Every method may throw an {@code ApiException}; the catalog turns it into a {@code
 * CatalogException} naming what it asked for.
 */
@Internal
interface BigtableCatalogClient extends AutoCloseable {

    /** The instance's table ids. */
    List<String> listTables();

    /**
     * The table's column families and each one's value type, as the admin API's protobuf reports
     * it, or {@code null} when the instance has no such table. A family with no value type maps to
     * {@link Type#getDefaultInstance()}.
     */
    @Nullable
    Map<String, Type> columnFamilies(String table);

    @Override
    void close();
}
