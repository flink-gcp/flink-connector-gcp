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

package io.github.flink.gcp.connector.bigtable.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.bigtable.TableDestination;

/**
 * One row of one table: the key {@link OrderingMode#PER_KEY} orders writes by.
 *
 * <p>Its {@code toString} names the table and the key's length but not the key, which is the row's
 * own data and would otherwise reach the sequencer's invariant failures.
 */
@Internal
public final class TableRowKey {

    private final TableDestination table;
    private final ByteString rowKey;

    /**
     * Creates the key.
     *
     * @param table the table
     * @param rowKey the row key
     */
    public TableRowKey(TableDestination table, ByteString rowKey) {
        this.table = Preconditions.checkNotNull(table, "table must not be null");
        this.rowKey = Preconditions.checkNotNull(rowKey, "rowKey must not be null");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof TableRowKey)) {
            return false;
        }
        TableRowKey that = (TableRowKey) o;
        return table.equals(that.table) && rowKey.equals(that.rowKey);
    }

    @Override
    public int hashCode() {
        return 31 * table.hashCode() + rowKey.hashCode();
    }

    @Override
    public String toString() {
        // A structured rendering, since the sequencer's messages say "key" just before it.
        return "TableRowKey{table=" + table + ", rowKeyBytes=" + rowKey.size() + "}";
    }
}
