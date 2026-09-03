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

package io.github.flink.gcp.connector.bigtable.sink.singlerow;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeinfo.TypeInfo;
import org.apache.flink.util.Preconditions;

import com.google.protobuf.ByteString;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * A Bigtable row as a request-response RPC returned it: its key and its cells, in the order the
 * service listed them (by family, then qualifier, then descending timestamp).
 *
 * <p>Connector-owned rather than the client's {@code Row}, which is marked for internal extension
 * only and whose shape a client release may change; the runtime converts on the thread that
 * receives the response, so no client type reaches a stream. A {@code ReadModifyWriteRow} answer
 * carries only the cells the request touched, not the whole row.
 *
 * <p>Instances are immutable, compare by value, and are serialized by a connector-owned Flink
 * serializer, so a {@code DataStream<BigtableRow>} needs no Kryo fallback. There is deliberately
 * <b>no {@code toString}</b>: the key and the cell values are the row's own data, and a value type
 * whose {@code toString} prints user data is one accidental log line away from putting it where it
 * does not belong — the rule {@code BigtableChangeStreamMutation} documents. A caller that wants to
 * render a row chooses what to print, through the accessors.
 */
@PublicEvolving
@TypeInfo(BigtableRowTypeInfoFactory.class)
public final class BigtableRow implements Serializable {

    private static final long serialVersionUID = 1L;

    private final ByteString key;
    private final List<Cell> cells;

    /**
     * Creates a row from its key and ordered cells. The runtime builds instances from the service's
     * answer; user code normally only reads them, but a test may construct its own.
     *
     * @param key the row key
     * @param cells the cells in service order; copied, and must not be or contain null
     */
    public BigtableRow(ByteString key, List<Cell> cells) {
        this.key = Preconditions.checkNotNull(key, "key must not be null");
        Preconditions.checkNotNull(cells, "cells must not be null");
        Preconditions.checkArgument(!cells.contains(null), "cells must not contain null");
        this.cells = Collections.unmodifiableList(new ArrayList<>(cells));
    }

    /**
     * Returns the row key.
     *
     * @return the key
     */
    public ByteString getKey() {
        return key;
    }

    /**
     * Returns the cells in service order, unmodifiable.
     *
     * @return the cells
     */
    public List<Cell> getCells() {
        return cells;
    }

    /**
     * Compares the key and every cell by value.
     *
     * @param o the object to compare with
     * @return whether the rows are equal
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BigtableRow that = (BigtableRow) o;
        return key.equals(that.key) && cells.equals(that.cells);
    }

    /**
     * Hashes the key and the cells.
     *
     * @return the hash code
     */
    @Override
    public int hashCode() {
        return Objects.hash(key, cells);
    }

    /**
     * One cell of a {@link BigtableRow}: a value at a family, qualifier and timestamp, with the
     * labels the service attached to it.
     *
     * <p>Immutable and compared by value; has no {@code toString}, for the row's reason.
     */
    @PublicEvolving
    public static final class Cell implements Serializable {

        private static final long serialVersionUID = 1L;

        private final String family;
        private final ByteString qualifier;
        private final long timestampMicros;
        private final ByteString value;
        private final List<String> labels;

        /**
         * Creates a cell.
         *
         * @param family the column family name
         * @param qualifier the column qualifier
         * @param timestampMicros the cell's timestamp in microseconds since the epoch
         * @param value the cell's value
         * @param labels the labels the service attached, usually empty; copied, and must not be or
         *     contain null
         */
        public Cell(
                String family,
                ByteString qualifier,
                long timestampMicros,
                ByteString value,
                List<String> labels) {
            this.family = Preconditions.checkNotNull(family, "family must not be null");
            this.qualifier = Preconditions.checkNotNull(qualifier, "qualifier must not be null");
            this.timestampMicros = timestampMicros;
            this.value = Preconditions.checkNotNull(value, "value must not be null");
            Preconditions.checkNotNull(labels, "labels must not be null");
            Preconditions.checkArgument(!labels.contains(null), "labels must not contain null");
            this.labels = Collections.unmodifiableList(new ArrayList<>(labels));
        }

        /**
         * Returns the column family name.
         *
         * @return the family
         */
        public String getFamily() {
            return family;
        }

        /**
         * Returns the column qualifier.
         *
         * @return the qualifier
         */
        public ByteString getQualifier() {
            return qualifier;
        }

        /**
         * Returns the cell's timestamp in microseconds since the epoch.
         *
         * @return the timestamp
         */
        public long getTimestampMicros() {
            return timestampMicros;
        }

        /**
         * Returns the cell's value.
         *
         * @return the value
         */
        public ByteString getValue() {
            return value;
        }

        /**
         * Returns the labels the service attached to the cell, unmodifiable and usually empty.
         *
         * @return the labels
         */
        public List<String> getLabels() {
            return labels;
        }

        /**
         * Compares every field by value.
         *
         * @param o the object to compare with
         * @return whether the cells are equal
         */
        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Cell that = (Cell) o;
            return timestampMicros == that.timestampMicros
                    && family.equals(that.family)
                    && qualifier.equals(that.qualifier)
                    && value.equals(that.value)
                    && labels.equals(that.labels);
        }

        /**
         * Hashes every field.
         *
         * @return the hash code
         */
        @Override
        public int hashCode() {
            return Objects.hash(family, qualifier, timestampMicros, value, labels);
        }
    }
}
