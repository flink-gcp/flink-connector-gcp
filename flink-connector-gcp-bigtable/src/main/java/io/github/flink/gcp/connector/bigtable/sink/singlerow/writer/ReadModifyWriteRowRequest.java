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

package io.github.flink.gcp.connector.bigtable.sink.singlerow.writer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

import com.google.api.core.ApiFuture;
import com.google.cloud.bigtable.data.v2.models.ReadModifyWriteRow;
import com.google.cloud.bigtable.data.v2.models.TableId;
import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.BigtableRow;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.RowOperation;

import java.util.ArrayList;
import java.util.List;

/**
 * A {@code ReadModifyWriteRow}: one or more append or increment rules over one row, applied
 * atomically in order. The answer is the row's cells that the rules touched, as a {@link
 * BigtableRow}.
 *
 * <p>The conversion from the client's {@code Row} runs on the thread that receives the response,
 * inside the future this request returns, so the client type never leaves this class. The rules are
 * connector-owned values rather than the client's builder because the builder is mutable and
 * carries the table id, which the runtime supplies at start time.
 */
@Internal
public final class ReadModifyWriteRowRequest implements RowRequest<BigtableRow> {

    private final ByteString rowKey;
    private final List<Rule> rules;

    /**
     * Creates the request.
     *
     * @param rowKey the row to read, modify and write
     * @param rules the rules, at least one, applied in order
     */
    public ReadModifyWriteRowRequest(ByteString rowKey, List<Rule> rules) {
        this.rowKey = Preconditions.checkNotNull(rowKey, "rowKey must not be null");
        Preconditions.checkNotNull(rules, "rules must not be null");
        Preconditions.checkArgument(
                !rules.isEmpty(), "A ReadModifyWriteRow request needs at least one rule");
        Preconditions.checkArgument(!rules.contains(null), "rules must not contain null");
        this.rules = new ArrayList<>(rules);
    }

    @Override
    public RowOperation operation() {
        return RowOperation.READ_MODIFY_WRITE_ROW;
    }

    @Override
    public ByteString rowKey() {
        return rowKey;
    }

    @Override
    public ApiFuture<BigtableRow> start(SingleRowClient client, TableDestination destination) {
        ReadModifyWriteRow mutation =
                ReadModifyWriteRow.create(TableId.of(destination.getTable()), rowKey);
        for (Rule rule : rules) {
            rule.applyTo(mutation);
        }
        // Not ApiFutures.transform: see ConvertedAnswer for why a cancel through it would not
        // reach the wire.
        return new ConvertedAnswer<>(client.readModifyWriteRow(mutation), BigtableRows::fromRow);
    }

    /**
     * One rule of a {@code ReadModifyWriteRow}: an append of bytes to a cell, or an increment of a
     * cell holding a 64-bit big-endian signed integer (an unset cell counts as empty or zero).
     *
     * <p>A rule is checked here for what the client's builder would otherwise refuse at start time
     * — an empty family, an empty append value — so that a malformed rule fails inside the
     * serializer that built it, where the record is still at hand, rather than inside the runtime.
     */
    @Internal
    public abstract static class Rule {

        final String family;
        final ByteString qualifier;

        private Rule(String family, ByteString qualifier) {
            this.family = Preconditions.checkNotNull(family, "family must not be null");
            Preconditions.checkArgument(!family.isEmpty(), "family must not be empty");
            this.qualifier = Preconditions.checkNotNull(qualifier, "qualifier must not be null");
        }

        /**
         * Creates an append rule.
         *
         * @param family the column family, not empty
         * @param qualifier the column qualifier
         * @param value the bytes to append, not empty: the service rejects an empty append
         * @return the rule
         */
        public static Rule append(String family, ByteString qualifier, ByteString value) {
            return new Append(family, qualifier, value);
        }

        /**
         * Creates an increment rule.
         *
         * @param family the column family, not empty
         * @param qualifier the column qualifier
         * @param amount the amount to add
         * @return the rule
         */
        public static Rule increment(String family, ByteString qualifier, long amount) {
            return new Increment(family, qualifier, amount);
        }

        abstract void applyTo(ReadModifyWriteRow mutation);
    }

    private static final class Append extends Rule {

        private final ByteString value;

        private Append(String family, ByteString qualifier, ByteString value) {
            super(family, qualifier);
            this.value = Preconditions.checkNotNull(value, "value must not be null");
            Preconditions.checkArgument(!value.isEmpty(), "value must not be empty");
        }

        @Override
        void applyTo(ReadModifyWriteRow mutation) {
            mutation.append(family, qualifier, value);
        }
    }

    private static final class Increment extends Rule {

        private final long amount;

        private Increment(String family, ByteString qualifier, long amount) {
            super(family, qualifier);
            this.amount = amount;
        }

        @Override
        void applyTo(ReadModifyWriteRow mutation) {
            mutation.increment(family, qualifier, amount);
        }
    }
}
