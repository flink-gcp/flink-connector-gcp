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

/**
 * The single-row request-response RPCs of Bigtable that this family issues, named as the service
 * names them.
 *
 * <p>Neither goes through the {@code MutateRows} batcher: each is one request for one row whose
 * answer is a value — whether the predicate matched, or the row after an atomic append or increment
 * — and the value is why the RPC is called at all.
 */
@PublicEvolving
public enum RowOperation {

    /**
     * {@code CheckAndMutateRow}: applies one of two mutation sets to a row depending on whether a
     * filter matches any cell of it, and answers with which set was applied.
     */
    CHECK_AND_MUTATE_ROW("CheckAndMutateRow"),

    /**
     * {@code ReadModifyWriteRow}: atomically appends to or increments cells of a row, and answers
     * with the cells after the change.
     */
    READ_MODIFY_WRITE_ROW("ReadModifyWriteRow");

    private final String rpcName;

    RowOperation(String rpcName) {
        this.rpcName = rpcName;
    }

    /**
     * Returns the RPC's name as the Bigtable API spells it, for messages that name the call.
     *
     * @return the RPC name
     */
    public String getRpcName() {
        return rpcName;
    }
}
