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
import org.apache.flink.util.Preconditions;

import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.writer.ReadModifyWriteRowRequest;

import java.io.Serializable;
import java.util.List;
import java.util.stream.Collectors;

/** Immutable row key and ordered rules, independent of the resolved destination. */
@PublicEvolving
public final class ReadModifyWriteRequest implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final int MAX_RULES = 100_000;
    private final ByteString rowKey;
    private final List<ReadModifyWriteRule> rules;

    private ReadModifyWriteRequest(ByteString rowKey, List<ReadModifyWriteRule> rules) {
        this.rowKey = Preconditions.checkNotNull(rowKey, "rowKey must not be null");
        Preconditions.checkArgument(!rowKey.isEmpty(), "rowKey must not be empty");
        Preconditions.checkNotNull(rules, "rules must not be null");
        Preconditions.checkArgument(
                !rules.isEmpty() && rules.size() <= MAX_RULES,
                "A read-modify-write request must contain between 1 and 100000 rules");
        this.rules = List.copyOf(rules);
    }

    /**
     * Copies the list, preserving repeated cells and operation order.
     *
     * @param rowKey the nonempty row key
     * @param rules between one and 100,000 nonnull rules
     * @return the request
     */
    public static ReadModifyWriteRequest of(ByteString rowKey, List<ReadModifyWriteRule> rules) {
        return new ReadModifyWriteRequest(rowKey, rules);
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
     * Returns the immutable ordered rules.
     *
     * @return the rules
     */
    public List<ReadModifyWriteRule> getRules() {
        return rules;
    }

    ReadModifyWriteRowRequest toRequest() {
        return new ReadModifyWriteRowRequest(
                rowKey,
                rules.stream().map(ReadModifyWriteRule::toRule).collect(Collectors.toList()));
    }
}
