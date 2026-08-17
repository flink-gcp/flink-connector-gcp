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

package io.github.flink.gcp.connector.spanner.table.source;

import org.apache.flink.annotation.Internal;

import java.io.Serializable;
import java.util.Objects;

/**
 * One column of a table's declared primary key, or of the index a scan reads through: its name, its
 * position among the physical columns, whether the key sorts it descending, and whether it is
 * nullable.
 *
 * <p>Key metadata rather than a filter concept, which is why it is here rather than nested in
 * {@link SpannerFilterPushDown}. Filter push-down is one of three consumers — {@link
 * SpannerTableReadResolver} builds these from the live key metadata, and the two lookup functions
 * carry them — and naming a push-down class in their signatures said the key ordering belonged to
 * push-down, which it does not.
 */
@Internal
final class KeyColumn implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String name;
    private final int physicalIndex;
    private final boolean descending;
    private final boolean nullable;

    KeyColumn(String name, int physicalIndex, boolean descending, boolean nullable) {
        this.name = name;
        this.physicalIndex = physicalIndex;
        this.descending = descending;
        this.nullable = nullable;
    }

    String name() {
        return name;
    }

    int physicalIndex() {
        return physicalIndex;
    }

    boolean isDescending() {
        return descending;
    }

    boolean isNullable() {
        return nullable;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        KeyColumn keyColumn = (KeyColumn) o;
        return physicalIndex == keyColumn.physicalIndex
                && descending == keyColumn.descending
                && nullable == keyColumn.nullable
                && name.equals(keyColumn.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, physicalIndex, descending, nullable);
    }
}
