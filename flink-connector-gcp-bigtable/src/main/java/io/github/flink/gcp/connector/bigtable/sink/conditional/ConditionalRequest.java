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
import org.apache.flink.util.Preconditions;

import com.google.protobuf.ByteString;

import java.io.Serializable;
import java.util.List;

/**
 * Immutable row predicate and ordered true/false branches, independent of the destination table.
 */
@PublicEvolving
public final class ConditionalRequest implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final int MAX_BRANCH_MUTATIONS = 100_000;
    private final ByteString rowKey;
    private final ConditionalFilter predicate;
    private final List<ConditionalMutation> thenMutations;
    private final List<ConditionalMutation> otherwiseMutations;

    private ConditionalRequest(
            ByteString rowKey,
            ConditionalFilter predicate,
            List<ConditionalMutation> thenMutations,
            List<ConditionalMutation> otherwiseMutations) {
        this.rowKey = Preconditions.checkNotNull(rowKey, "rowKey must not be null");
        Preconditions.checkArgument(!rowKey.isEmpty(), "rowKey must not be empty");
        this.predicate = Preconditions.checkNotNull(predicate, "predicate must not be null");
        this.thenMutations = List.copyOf(thenMutations);
        this.otherwiseMutations = List.copyOf(otherwiseMutations);
        Preconditions.checkArgument(
                !this.thenMutations.isEmpty() || !this.otherwiseMutations.isEmpty(),
                "At least one conditional mutation branch must be nonempty");
        Preconditions.checkArgument(
                this.thenMutations.size() <= MAX_BRANCH_MUTATIONS
                        && this.otherwiseMutations.size() <= MAX_BRANCH_MUTATIONS,
                "Each conditional branch must contain at most 100000 mutations");
    }

    /**
     * Creates a request, copying both lists and retaining mutation order.
     *
     * @param rowKey the nonempty row key
     * @param predicate the cell-selection predicate
     * @param thenMutations mutations selected when the predicate matches
     * @param otherwiseMutations mutations selected when the predicate does not match
     * @return the request
     */
    public static ConditionalRequest of(
            ByteString rowKey,
            ConditionalFilter predicate,
            List<ConditionalMutation> thenMutations,
            List<ConditionalMutation> otherwiseMutations) {
        return new ConditionalRequest(rowKey, predicate, thenMutations, otherwiseMutations);
    }

    /**
     * Returns the addressed row key.
     *
     * @return the row key
     */
    public ByteString getRowKey() {
        return rowKey;
    }

    /**
     * Returns the predicate.
     *
     * @return the predicate
     */
    public ConditionalFilter getPredicate() {
        return predicate;
    }

    /**
     * Returns the immutable true branch.
     *
     * @return the true branch
     */
    public List<ConditionalMutation> getThenMutations() {
        return thenMutations;
    }

    /**
     * Returns the immutable false branch.
     *
     * @return the false branch
     */
    public List<ConditionalMutation> getOtherwiseMutations() {
        return otherwiseMutations;
    }
}
