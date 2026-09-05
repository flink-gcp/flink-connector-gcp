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

package io.github.flink.gcp.connector.bigtable.sink.conditional;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeinfo.TypeInfo;
import org.apache.flink.util.Preconditions;

import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.bigtable.TableDestination;

import java.io.Serializable;
import java.util.Objects;

/**
 * A successful conditional response, including the destination actually used for this invocation.
 */
@PublicEvolving
@TypeInfo(ConditionalResultTypeInfoFactory.class)
public final class ConditionalResult implements Serializable {
    private static final long serialVersionUID = 1L;
    private final TableDestination destination;
    private final ByteString rowKey;
    private final boolean predicateMatched;
    private final boolean selectedBranchHasMutations;

    /**
     * Creates a successful response value.
     *
     * @param destination the resolved destination
     * @param rowKey the addressed row key
     * @param predicateMatched whether the predicate selected any cell
     * @param selectedBranchHasMutations whether the selected mutation list was nonempty
     */
    public ConditionalResult(
            TableDestination destination,
            ByteString rowKey,
            boolean predicateMatched,
            boolean selectedBranchHasMutations) {
        this.destination = Preconditions.checkNotNull(destination, "destination must not be null");
        this.rowKey = Preconditions.checkNotNull(rowKey, "rowKey must not be null");
        this.predicateMatched = predicateMatched;
        this.selectedBranchHasMutations = selectedBranchHasMutations;
    }

    /**
     * Returns the destination resolved before serialization, without resolving it again.
     *
     * @return the destination
     */
    public TableDestination getDestination() {
        return destination;
    }

    /**
     * Returns the row key sent to Bigtable.
     *
     * @return the row key
     */
    public ByteString getRowKey() {
        return rowKey;
    }

    /**
     * Returns whether the predicate matched. False is also a successful RPC response.
     *
     * @return the predicate outcome
     */
    public boolean isPredicateMatched() {
        return predicateMatched;
    }

    /**
     * Returns whether the selected list was nonempty; this does not prove that stored bytes
     * changed.
     *
     * @return whether the selected list had mutations
     */
    public boolean isSelectedBranchHasMutations() {
        return selectedBranchHasMutations;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof ConditionalResult)) {
            return false;
        }
        ConditionalResult that = (ConditionalResult) other;
        return predicateMatched == that.predicateMatched
                && selectedBranchHasMutations == that.selectedBranchHasMutations
                && destination.equals(that.destination)
                && rowKey.equals(that.rowKey);
    }

    @Override
    public int hashCode() {
        return Objects.hash(destination, rowKey, predicateMatched, selectedBranchHasMutations);
    }
}
