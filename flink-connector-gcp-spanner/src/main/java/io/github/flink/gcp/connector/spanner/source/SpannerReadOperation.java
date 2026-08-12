/*
 * Copyright 2026 laughingman7743
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

package io.github.flink.gcp.connector.spanner.source;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.util.Preconditions;

import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.Statement;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * What the batch source reads: either a query, or a table with its columns and key set, optionally
 * through an index.
 *
 * <pre>{@code
 * SpannerReadOperation.query(Statement.of("SELECT id, name FROM singers"))
 * SpannerReadOperation.read("singers", KeySet.all(), Arrays.asList("id", "name"))
 * SpannerReadOperation.readUsingIndex(
 *         "singers", "singers_by_name", KeySet.all(), Arrays.asList("id", "name"))
 * }</pre>
 *
 * <p>The two shapes are exclusive because Spanner's own API makes them so — {@code partitionQuery}
 * takes a statement and {@code partitionRead} takes a table — and holding them in one value object
 * is what lets the builder take a single option rather than five that only make sense in two
 * combinations.
 *
 * <p><b>Not every query can be read this way.</b> Spanner partitions a query only when its
 * execution plan begins with a distributed union — in practice a scan of one table, with predicates
 * and projections but no aggregate, no {@code ORDER BY} and no {@code LIMIT}. A query that is not
 * root-partitionable is refused by the service when the source plans, with a message naming the
 * reason, and the source fails rather than silently reading it on one subtask.
 */
@PublicEvolving
public final class SpannerReadOperation implements Serializable {

    private static final long serialVersionUID = 1L;

    @Nullable private final Statement statement;
    @Nullable private final String table;
    @Nullable private final String index;
    @Nullable private final KeySet keys;
    @Nullable private final List<String> columns;
    @Nullable private final SpannerReadOperationResolver resolver;

    private SpannerReadOperation(
            @Nullable Statement statement,
            @Nullable String table,
            @Nullable String index,
            @Nullable KeySet keys,
            @Nullable List<String> columns,
            @Nullable SpannerReadOperationResolver resolver) {
        this.statement = statement;
        this.table = table;
        this.index = index;
        this.keys = keys;
        this.columns = columns;
        this.resolver = resolver;
    }

    /**
     * Reads the rows a query returns.
     *
     * @param statement the query, with its parameter bindings if it has any
     * @return the read operation
     */
    public static SpannerReadOperation query(Statement statement) {
        Preconditions.checkNotNull(statement, "statement must not be null");
        return new SpannerReadOperation(statement, null, null, null, null, null);
    }

    /**
     * Reads columns of a table over a key set.
     *
     * @param table the table name
     * @param keys the keys and key ranges to read; {@link KeySet#all()} for the whole table
     * @param columns the columns to return, at least one
     * @return the read operation
     */
    public static SpannerReadOperation read(String table, KeySet keys, List<String> columns) {
        return tableRead(table, null, keys, columns);
    }

    /**
     * Reads columns of a table over a key set, through a secondary index.
     *
     * <p>The key set is interpreted in the <em>index's</em> key space, not the table's, and a read
     * can return only index key columns, base-table primary-key columns, and columns included with
     * {@code STORING} or {@code INCLUDE}.
     *
     * @param table the table name
     * @param index the index name
     * @param keys the keys and key ranges to read, in the index's key space
     * @param columns the columns to return, at least one
     * @return the read operation
     */
    public static SpannerReadOperation readUsingIndex(
            String table, String index, KeySet keys, List<String> columns) {
        checkName(index, "index");
        return tableRead(table, index, keys, columns);
    }

    private static SpannerReadOperation tableRead(
            String table, @Nullable String index, KeySet keys, List<String> columns) {
        checkName(table, "table");
        Preconditions.checkNotNull(keys, "keys must not be null");
        Preconditions.checkNotNull(columns, "columns must not be null");
        Preconditions.checkArgument(
                !columns.isEmpty(), "columns must not be empty: a read returns named columns.");
        List<String> copy = Collections.unmodifiableList(new ArrayList<>(columns));
        for (String column : copy) {
            checkName(column, "column");
        }
        return new SpannerReadOperation(null, table, index, keys, copy, null);
    }

    static SpannerReadOperation deferred(SpannerReadOperationResolver resolver) {
        return new SpannerReadOperation(null, null, null, null, null, resolver);
    }

    @Nullable
    SpannerReadOperationResolver getResolver() {
        return resolver;
    }

    private static void checkName(String value, String what) {
        Preconditions.checkNotNull(value, what + " must not be null");
        Preconditions.checkArgument(!value.trim().isEmpty(), what + " must not be blank");
    }

    /**
     * Returns whether this is a query rather than a table read.
     *
     * @return whether this operation is a query
     */
    public boolean isQuery() {
        return statement != null;
    }

    /**
     * Returns the query, when this is one.
     *
     * @return the query, or {@code null} when this is a table read
     */
    @Nullable
    public Statement getStatement() {
        return statement;
    }

    /**
     * Returns the table to read, when this is a table read.
     *
     * @return the table name, or {@code null} when this is a query
     */
    @Nullable
    public String getTable() {
        return table;
    }

    /**
     * Returns the index to read through.
     *
     * @return the index name, or {@code null} for a read of the table itself or for a query
     */
    @Nullable
    public String getIndex() {
        return index;
    }

    /**
     * Returns the keys to read, when this is a table read.
     *
     * @return the key set, or {@code null} when this is a query
     */
    @Nullable
    public KeySet getKeys() {
        return keys;
    }

    /**
     * Returns the columns to return, when this is a table read.
     *
     * @return the columns, or {@code null} when this is a query
     */
    @Nullable
    public List<String> getColumns() {
        return columns;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof SpannerReadOperation)) {
            return false;
        }
        SpannerReadOperation that = (SpannerReadOperation) o;
        return Objects.equals(statement, that.statement)
                && Objects.equals(table, that.table)
                && Objects.equals(index, that.index)
                && Objects.equals(keys, that.keys)
                && Objects.equals(columns, that.columns)
                && Objects.equals(resolver, that.resolver);
    }

    @Override
    public int hashCode() {
        return Objects.hash(statement, table, index, keys, columns, resolver);
    }

    @Override
    public String toString() {
        if (isQuery()) {
            return "query [" + statement.getSql() + "]";
        }
        if (resolver != null) {
            return "deferred read [" + resolver + "]";
        }
        return "read of table "
                + table
                + (index == null ? "" : " through index " + index)
                + " columns "
                + columns;
    }
}
