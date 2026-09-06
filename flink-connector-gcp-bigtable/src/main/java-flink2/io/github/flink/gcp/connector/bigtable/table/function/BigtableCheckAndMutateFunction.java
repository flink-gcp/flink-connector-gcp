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

package io.github.flink.gcp.connector.bigtable.table.function;

import org.apache.flink.annotation.PublicEvolving;

import java.util.concurrent.CompletableFuture;

/**
 * Async SQL conditional write, returning whether the configured predicate matched.
 *
 * <p>Register the class with CREATE FUNCTION and configure a named request through {@code
 * bigtable.functions.<name>.*} session settings. SQL arguments are the literal settings name, row
 * key, and operands. The row key and every supplied operand must have a NOT NULL SQL type; nullable
 * expressions are rejected during planning. A false result is successful and may have applied the
 * otherwise branch. The function requires one Flink async attempt; query recovery can still repeat
 * a write. Available only in the Flink 2.x artifact and streaming execution mode.
 */
@PublicEvolving
public final class BigtableCheckAndMutateFunction extends AbstractBigtableWriteFunction<Boolean> {
    private static final long serialVersionUID = 1L;

    /** Creates the function definition for registration through Flink SQL. */
    public BigtableCheckAndMutateFunction() {
        this(null);
    }

    BigtableCheckAndMutateFunction(SqlWriteTemplate template) {
        super(true, template);
    }

    @Override
    AbstractBigtableWriteFunction<Boolean> specialized(SqlWriteTemplate compiled) {
        return new BigtableCheckAndMutateFunction(compiled);
    }

    /**
     * Evaluates one configured write asynchronously.
     *
     * @param future the result, completed exceptionally on invalid input or request failure
     * @param settings the literal name of the SET configuration
     * @param rowKey the row key in its planner-selected internal representation
     * @param values operands in their planner-selected internal representations
     */
    public void eval(
            CompletableFuture<Boolean> future, String settings, Object rowKey, Object... values) {
        evaluate(future, rowKey, values);
    }
}
