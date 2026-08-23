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

import org.apache.flink.annotation.Public;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * The settings for the table the sink creates under {@link CreateDisposition#CREATE_IF_NEEDED}: its
 * column families and, per family, an optional {@link GcRule garbage-collection rule}.
 *
 * <p>Unlike the Pub/Sub sink's topic settings, this object is <em>required</em> beside {@code
 * CREATE_IF_NEEDED} rather than additive: a topic can meaningfully be created with defaults, but a
 * Bigtable table's schema is its column families, which the sink cannot guess — a table created
 * without them rejects every mutation and is a table someone has to fix later. Combining options
 * with {@link CreateDisposition#CREATE_NEVER} is rejected at {@code build()}, and so is {@code
 * CREATE_IF_NEEDED} without options.
 *
 * <p><b>Creation only, per family.</b> A table that already exists is written to as it is: the
 * families declared here that it lacks are added — with their rules — but an existing family's
 * garbage-collection rule is neither compared nor updated. A family declared without a rule keeps
 * Bigtable's default of collecting nothing; see {@link GcRule} for why that choice matters for an
 * at-least-once sink.
 *
 * <p>Validation stops at what would otherwise fail obscurely (a blank name, an empty family set);
 * Bigtable's own name grammar and limits are left to the service, whose rejection names the field
 * and the limit. Instances are immutable and serializable.
 */
@Public
public final class TableCreateOptions implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Family name to garbage-collection rule; a {@code null} value is a family without one. */
    private final LinkedHashMap<String, GcRule> columnFamilies;

    private TableCreateOptions(Builder builder) {
        this.columnFamilies = new LinkedHashMap<>(builder.columnFamilies);
    }

    /**
     * Creates a new {@link Builder}.
     *
     * @return a new builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Returns the column families to create, in declaration order, each mapped to its
     * garbage-collection rule or to {@code null} for none.
     */
    public Map<String, GcRule> getColumnFamilies() {
        return Collections.unmodifiableMap(columnFamilies);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TableCreateOptions that = (TableCreateOptions) o;
        return columnFamilies.equals(that.columnFamilies);
    }

    @Override
    public int hashCode() {
        return Objects.hash(columnFamilies);
    }

    @Override
    public String toString() {
        return "TableCreateOptions{columnFamilies=" + columnFamilies + "}";
    }

    /** Builder for {@link TableCreateOptions}. */
    @Public
    public static final class Builder {

        private final LinkedHashMap<String, GcRule> columnFamilies = new LinkedHashMap<>();

        private Builder() {}

        /**
         * Declares a column family without a garbage-collection rule, keeping Bigtable's default of
         * collecting nothing.
         *
         * @param name the family name, non-blank
         * @return this builder
         */
        public Builder columnFamily(String name) {
            return put(name, null);
        }

        /**
         * Declares a column family with the given garbage-collection rule.
         *
         * @param name the family name, non-blank
         * @param rule the rule to create the family with
         * @return this builder
         */
        public Builder columnFamily(String name, GcRule rule) {
            Preconditions.checkNotNull(rule, "rule must not be null");
            return put(name, rule);
        }

        private Builder put(String name, @Nullable GcRule rule) {
            Preconditions.checkNotNull(name, "name must not be null");
            Preconditions.checkArgument(!name.isBlank(), "name must not be blank");
            // Last writer wins on a repeated name, as this project's builders do by design.
            columnFamilies.put(name, rule);
            return this;
        }

        /**
         * Builds the options.
         *
         * @return the options
         * @throws IllegalStateException if no column family was declared
         */
        public TableCreateOptions build() {
            Preconditions.checkState(
                    !columnFamilies.isEmpty(),
                    "At least one columnFamily(...) is required: a Bigtable table's schema is its"
                            + " column families, and one created without any rejects every"
                            + " mutation.");
            return new TableCreateOptions(this);
        }
    }
}
