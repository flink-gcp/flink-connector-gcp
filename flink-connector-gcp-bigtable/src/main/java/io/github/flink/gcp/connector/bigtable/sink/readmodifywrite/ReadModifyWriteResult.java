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

package io.github.flink.gcp.connector.bigtable.sink.readmodifywrite;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeinfo.TypeInfo;
import org.apache.flink.util.Preconditions;

import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.BigtableRow;

import java.io.Serializable;
import java.util.Objects;

/** Successful response containing the resolved destination and the final changed cells. */
@PublicEvolving
@TypeInfo(ReadModifyWriteResultTypeInfoFactory.class)
public final class ReadModifyWriteResult implements Serializable {
    private static final long serialVersionUID = 1L;
    private final TableDestination destination;
    private final BigtableRow row;

    /**
     * Creates an immutable result from a successful request.
     *
     * @param destination the actual resolved destination
     * @param row the changed cells, not a complete row snapshot
     */
    public ReadModifyWriteResult(TableDestination destination, BigtableRow row) {
        this.destination = Preconditions.checkNotNull(destination, "destination must not be null");
        this.row = Preconditions.checkNotNull(row, "row must not be null");
    }

    /**
     * Returns the actual destination.
     *
     * @return the destination
     */
    public TableDestination getDestination() {
        return destination;
    }

    /**
     * Returns the row key and final changed cells with raw values and service timestamps.
     *
     * @return the changed row
     */
    public BigtableRow getRow() {
        return row;
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof ReadModifyWriteResult)) {
            return false;
        }
        ReadModifyWriteResult that = (ReadModifyWriteResult) other;
        return destination.equals(that.destination) && row.equals(that.row);
    }

    @Override
    public int hashCode() {
        return Objects.hash(destination, row);
    }
}
