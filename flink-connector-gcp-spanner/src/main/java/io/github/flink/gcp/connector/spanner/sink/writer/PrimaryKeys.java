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

package io.github.flink.gcp.connector.spanner.sink.writer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Type;
import com.google.cloud.spanner.Value;
import io.github.flink.gcp.connector.spanner.SpannerTableName;
import io.github.flink.gcp.connector.spanner.SpannerTypeSpelling;

import javax.annotation.Nullable;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;

/**
 * Which rows a mutation addresses: its table and the values of the table's primary-key columns,
 * read with each column's type from the database's {@code INFORMATION_SCHEMA} when the writer
 * opens.
 *
 * <p>A {@link Mutation} names its table and its columns but not which of them form the key, so the
 * writer cannot name a write's row without the schema. A delete names its rows as a {@link KeySet}.
 *
 * <p>Two mutations must be found to share a row whenever Spanner would apply them to one row;
 * finding two rows the same when they are not costs only an earlier flush. So every doubt resolves
 * toward sharing, and a mutation whose rows cannot be named is treated as touching every row of its
 * table ({@link Footprint#isWholeTable()}).
 *
 * <p>A key value is compared as the key column's type reads it, and only in the representation that
 * type has in the client library: an {@code INT64} key as a {@code Long}, a {@code STRING} key as a
 * {@code String}, and so on. Spanner receives a mutation's values untyped and reads them by the
 * column's type, so a key value carried in another representation, such as a {@code UUID} written
 * as a string, could address the same row in a spelling no comparison here would match; such a
 * value names the whole table instead. A {@code NUMERIC} compares by value whatever its scale. A
 * {@code FLOAT32} compares at float precision, which only merges; the emulator refuses a {@code
 * FLOAT32} primary-key column, so the case is a guard rather than a path a job takes.
 *
 * <p>A table name the schema did not list is ordered against every mutation of the batch ({@link
 * Footprint#isWholeDatabase()}): it may be a table created since the read, which the writer then
 * reads the schema again for, or a synonym, which names a listed table under another name.
 *
 * <p>Table names compare without case in both dialects. Whether the service matches a PostgreSQL
 * mutation's table name exactly is not measured, and folding can only merge. It never merges two
 * tables: GoogleSQL names are case-insensitive, and the service refuses a PostgreSQL table whose
 * name differs from another's only by case.
 */
@Internal
public final class PrimaryKeys {

    private static final PrimaryKeys EMPTY =
            new PrimaryKeys(
                    Dialect.GOOGLE_STANDARD_SQL, Collections.emptyMap(), Collections.emptySet());

    /** Stands in for a key value that cannot be compared, which makes the whole table the row. */
    private static final Object UNNAMEABLE = new Object();

    /** Stands in for the commit-timestamp placeholder, which is one value in a request. */
    private static final Object COMMIT_TIMESTAMP = new Object();

    private final Dialect dialect;

    /** Folded table key to its key columns in key order, for every table that can be named. */
    private final Map<String, List<KeyColumn>> keyColumns;

    /** Folded key of every table the schema listed, whether or not its rows can be named. */
    private final Set<String> tables;

    private PrimaryKeys(
            Dialect dialect, Map<String, List<KeyColumn>> keyColumns, Set<String> tables) {
        this.dialect = dialect;
        this.keyColumns = keyColumns;
        this.tables = tables;
    }

    /**
     * Returns primary keys that know no table, so every mutation is ordered against every other.
     *
     * @return the empty primary keys
     */
    @VisibleForTesting
    public static PrimaryKeys empty() {
        return EMPTY;
    }

    /**
     * Returns the query listing (schema, table, column, position in the key, type) for every
     * primary-key column, and one row with a null column, position and type for a table with none.
     *
     * <p>Both dialects list a table's key columns in {@code INDEX_COLUMNS} under the index named
     * and typed {@code PRIMARY_KEY}, with {@code ORDINAL_POSITION} giving the column's place in the
     * key, and the column's type in {@code COLUMNS.SPANNER_TYPE}; the Table catalog reads the same
     * two views. The query covers every visible user schema, as the cell-weight query does.
     *
     * <p>It starts from the base table in {@code TABLES} and outer-joins the key columns and their
     * types to it, because a GoogleSQL table declared with {@code PRIMARY KEY ()} has no key column
     * in {@code INDEX_COLUMNS} and, on the service, no {@code PRIMARY_KEY} row in {@code INDEXES}
     * either. A key column whose type the join does not find arrives with a null type too, rather
     * than dropping out of the key; {@link #read} says what such a row means. The {@code WHERE}
     * clause filters the {@code TABLES} row alone, and the key-index conditions sit in the join,
     * since a condition on a joined view would discard the rows the outer joins exist to keep.
     *
     * @param dialect the database's dialect
     * @return the SQL to run
     */
    static String query(Dialect dialect) {
        switch (dialect) {
            case POSTGRESQL:
                return "SELECT t.table_schema, t.table_name, k.column_name,"
                        + " k.ordinal_position, c.spanner_type"
                        + " FROM information_schema.tables AS t"
                        + " LEFT JOIN information_schema.index_columns AS k"
                        + " ON k.table_catalog = t.table_catalog"
                        + " AND k.table_schema = t.table_schema"
                        + " AND k.table_name = t.table_name"
                        + " AND k.index_name = 'PRIMARY_KEY'"
                        + " AND k.index_type = 'PRIMARY_KEY'"
                        + " LEFT JOIN information_schema.columns AS c"
                        + " ON c.table_catalog = k.table_catalog"
                        + " AND c.table_schema = k.table_schema"
                        + " AND c.table_name = k.table_name"
                        + " AND c.column_name = k.column_name"
                        + " WHERE t.table_type = 'BASE TABLE'"
                        + " AND t.table_schema NOT IN"
                        + " ('pg_catalog', 'information_schema', 'spanner_sys')";
            case GOOGLE_STANDARD_SQL:
                return "SELECT t.TABLE_SCHEMA, t.TABLE_NAME, k.COLUMN_NAME,"
                        + " k.ORDINAL_POSITION, c.SPANNER_TYPE"
                        + " FROM INFORMATION_SCHEMA.TABLES AS t"
                        + " LEFT JOIN INFORMATION_SCHEMA.INDEX_COLUMNS AS k"
                        + " ON k.TABLE_CATALOG = t.TABLE_CATALOG"
                        + " AND k.TABLE_SCHEMA = t.TABLE_SCHEMA"
                        + " AND k.TABLE_NAME = t.TABLE_NAME"
                        + " AND k.INDEX_NAME = 'PRIMARY_KEY'"
                        + " AND k.INDEX_TYPE = 'PRIMARY_KEY'"
                        + " LEFT JOIN INFORMATION_SCHEMA.COLUMNS AS c"
                        + " ON c.TABLE_CATALOG = k.TABLE_CATALOG"
                        + " AND c.TABLE_SCHEMA = k.TABLE_SCHEMA"
                        + " AND c.TABLE_NAME = k.TABLE_NAME"
                        + " AND c.COLUMN_NAME = k.COLUMN_NAME"
                        + " WHERE t.TABLE_CATALOG = ''"
                        + " AND t.TABLE_TYPE = 'BASE TABLE'"
                        + " AND t.TABLE_SCHEMA NOT IN ('INFORMATION_SCHEMA', 'SPANNER_SYS')";
            default:
                // Unreachable today; a dialect added to the client library lands here rather than
                // silently reading nothing, which would order every mutation per table.
                throw new IllegalStateException(
                        "Unsupported Spanner dialect: "
                                + dialect
                                + ". This connector reads primary keys for GOOGLE_STANDARD_SQL and"
                                + " POSTGRESQL only.");
        }
    }

    /**
     * Returns the query of {@link #query(Dialect)}, narrowed to one table when {@code table} names
     * one. The table is matched without case in both dialects, as table names compare; that reads
     * one table at most, because GoogleSQL names are case-insensitive and the service refuses a
     * PostgreSQL table whose name differs from another's only by case.
     *
     * @param dialect the database's dialect
     * @param table the table's native API name, {@code table} or {@code schema.table}, or {@code
     *     null} for every table of the database
     * @return the statement to run
     * @throws IllegalArgumentException if {@code table} is not one of those two shapes
     */
    static Statement query(Dialect dialect, @Nullable String table) {
        String sql = query(dialect);
        if (table == null) {
            return Statement.of(sql);
        }
        SpannerTableName name = SpannerTableName.ofApiName(table, dialect);
        // query(dialect) has already refused a dialect it has no statement for.
        if (dialect == Dialect.POSTGRESQL) {
            return Statement.newBuilder(
                            sql
                                    + " AND LOWER(t.table_schema) = LOWER($1)"
                                    + " AND LOWER(t.table_name) = LOWER($2)")
                    .bind("p1")
                    .to(name.schema())
                    .bind("p2")
                    .to(name.table())
                    .build();
        }
        return Statement.newBuilder(
                        sql
                                + " AND LOWER(t.TABLE_SCHEMA) = LOWER(@schema_name)"
                                + " AND LOWER(t.TABLE_NAME) = LOWER(@table_name)")
                .bind("schema_name")
                .to(name.schema())
                .bind("table_name")
                .to(name.table())
                .build();
    }

    /**
     * Reads the primary keys from the result of either query. Does not close the result set.
     *
     * @param resultSet the query result, positioned before the first row
     * @param dialect the database's dialect
     * @return the primary keys
     */
    static PrimaryKeys read(ResultSet resultSet, Dialect dialect) {
        Builder keys = builder(dialect);
        while (resultSet.next()) {
            // Read positionally: the two dialects' queries select the same five values under
            // different spellings.
            if (resultSet.isNull(0) || resultSet.isNull(1)) {
                continue;
            }
            if (resultSet.isNull(2) || resultSet.isNull(3) || resultSet.isNull(4)) {
                // A table with no key column, which holds one row at most, or a key read in part,
                // which would compare fewer columns or the wrong ones: either way the table is
                // listed and its rows are not named, so its mutations are ordered against it.
                keys.unnameable(resultSet.getString(0), resultSet.getString(1));
                continue;
            }
            keys.keyColumn(
                    resultSet.getString(0),
                    resultSet.getString(1),
                    resultSet.getString(2),
                    resultSet.getLong(3),
                    resultSet.getString(4));
        }
        return keys.build();
    }

    /** Creates a builder whose keys follow the database dialect. */
    static Builder builder(Dialect dialect) {
        return new Builder(dialect);
    }

    /** Returns how many tables the primary keys carry, for the writer's open-time log line. */
    int tableCount() {
        return keyColumns.size();
    }

    /**
     * Returns the rows the mutation addresses.
     *
     * @param mutation the mutation
     * @return its footprint
     */
    Footprint footprintOf(Mutation mutation) {
        String table = tableKey(mutation.getTable(), dialect);
        if (!tables.contains(table)) {
            // A synonym addresses a listed table's rows under a name that is not that table's.
            return Footprint.wholeDatabase();
        }
        switch (mutation.getOperation()) {
            case DELETE:
                return deleteFootprint(table, mutation.getKeySet());
            case INSERT:
            case UPDATE:
            case INSERT_OR_UPDATE:
            case REPLACE:
                return writeFootprint(table, mutation);
            default:
                // An operation added to the client library later: nothing here can say which rows
                // it touches.
                return Footprint.wholeTable(table);
        }
    }

    private Footprint deleteFootprint(String table, KeySet keySet) {
        List<KeyColumn> columns = keyColumns.get(table);
        if (columns == null || keySet.isAll() || keySet.getRanges().iterator().hasNext()) {
            return Footprint.wholeTable(table);
        }
        Set<List<Object>> rows = new HashSet<>();
        for (Key key : keySet.getKeys()) {
            if (key.size() != columns.size()) {
                return Footprint.wholeTable(table);
            }
            List<Object> row = new ArrayList<>(columns.size());
            Iterator<KeyColumn> column = columns.iterator();
            for (Object part : key.getParts()) {
                Object compared = keyPart(part, column.next().kind);
                if (compared == UNNAMEABLE) {
                    return Footprint.wholeTable(table);
                }
                row.add(compared);
            }
            rows.add(row);
        }
        return Footprint.rows(table, rows);
    }

    private Footprint writeFootprint(String table, Mutation mutation) {
        List<KeyColumn> columns = keyColumns.get(table);
        if (columns == null) {
            return Footprint.wholeTable(table);
        }
        Object[] row = new Object[columns.size()];
        boolean[] seen = new boolean[columns.size()];
        Iterator<Value> values = mutation.getValues().iterator();
        for (String column : mutation.getColumns()) {
            Value value = values.next();
            String columnKey = SpannerTableName.catalogIdentifierKey(column, dialect);
            for (int i = 0; i < row.length; i++) {
                if (!seen[i] && columns.get(i).name.equals(columnKey)) {
                    Object compared = keyPart(value, columns.get(i).kind);
                    if (compared == UNNAMEABLE) {
                        return Footprint.wholeTable(table);
                    }
                    row[i] = compared;
                    seen[i] = true;
                }
            }
        }
        for (boolean found : seen) {
            if (!found) {
                return Footprint.wholeTable(table);
            }
        }
        List<Object> key = new ArrayList<>(row.length);
        Collections.addAll(key, row);
        return Footprint.rows(table, Collections.singleton(key));
    }

    /** Returns a write value as the key column's type compares it, {@code null} for SQL NULL. */
    @Nullable
    private static Object keyPart(Value value, SpannerTypeSpelling.Kind kind) {
        Type type = value.getType();
        if (type == null) {
            // An untyped value: its wire form is known, but not which spelling the column reads.
            return UNNAMEABLE;
        }
        if (value.isNull()) {
            return null;
        }
        Type.Code code = type.getCode();
        switch (kind) {
            case BOOL:
                return code == Type.Code.BOOL ? value.getBool() : UNNAMEABLE;
            case INT64:
                return code == Type.Code.INT64 ? value.getInt64() : UNNAMEABLE;
            case FLOAT32:
            case FLOAT64:
                if (code == Type.Code.FLOAT32) {
                    return floating(value.getFloat32(), kind);
                }
                return code == Type.Code.FLOAT64 ? floating(value.getFloat64(), kind) : UNNAMEABLE;
            case NUMERIC:
                if (code == Type.Code.NUMERIC) {
                    return numeric(value.getNumeric());
                }
                return code == Type.Code.PG_NUMERIC ? numeric(value.getString()) : UNNAMEABLE;
            case STRING:
                return code == Type.Code.STRING ? value.getString() : UNNAMEABLE;
            case BYTES:
                return code == Type.Code.BYTES ? value.getBytes() : UNNAMEABLE;
            case DATE:
                return code == Type.Code.DATE ? value.getDate() : UNNAMEABLE;
            case TIMESTAMP:
                if (code != Type.Code.TIMESTAMP) {
                    return UNNAMEABLE;
                }
                // Two writes carrying the placeholder may commit in one transaction, and so at
                // one timestamp.
                return value.isCommitTimestamp() ? COMMIT_TIMESTAMP : value.getTimestamp();
            case UUID:
                return code == Type.Code.UUID ? value.getUuid() : UNNAMEABLE;
            default:
                return UNNAMEABLE;
        }
    }

    /** Returns a delete key part as the key column's type compares it, {@code null} for NULL. */
    @Nullable
    private static Object keyPart(@Nullable Object part, SpannerTypeSpelling.Kind kind) {
        if (part == null) {
            return null;
        }
        switch (kind) {
            case BOOL:
                return part instanceof Boolean ? part : UNNAMEABLE;
            case INT64:
                // Key stores an Integer as a Long.
                return part instanceof Long ? part : UNNAMEABLE;
            case FLOAT32:
            case FLOAT64:
                // Key stores a Float as a Double.
                return part instanceof Double ? floating((Double) part, kind) : UNNAMEABLE;
            case NUMERIC:
                if (part instanceof BigDecimal) {
                    return numeric((BigDecimal) part);
                }
                return part instanceof String ? numeric((String) part) : UNNAMEABLE;
            case STRING:
                return part instanceof String ? part : UNNAMEABLE;
            case BYTES:
                return part instanceof ByteArray ? part : UNNAMEABLE;
            case DATE:
                return part instanceof Date ? part : UNNAMEABLE;
            case TIMESTAMP:
                return part instanceof Timestamp ? part : UNNAMEABLE;
            case UUID:
                return part instanceof UUID ? part : UNNAMEABLE;
            default:
                return UNNAMEABLE;
        }
    }

    private static Object floating(double value, SpannerTypeSpelling.Kind kind) {
        double stored = kind == SpannerTypeSpelling.Kind.FLOAT32 ? (float) value : value;
        // -0.0 and 0.0 are one key; Double.equals keeps them apart, and holds NaN equal to NaN.
        return stored == 0.0 ? 0.0 : stored;
    }

    private static Object numeric(BigDecimal value) {
        try {
            // BigDecimal has no negative zero, and stripping makes equal values equal objects.
            return value.stripTrailingZeros();
        } catch (ArithmeticException e) {
            // A scale past int range, which Spanner would refuse anyway.
            return UNNAMEABLE;
        }
    }

    private static Object numeric(String value) {
        if (value.equalsIgnoreCase("NaN")) {
            return "NaN";
        }
        try {
            return numeric(new BigDecimal(value));
        } catch (NumberFormatException e) {
            return UNNAMEABLE;
        }
    }

    /**
     * Returns the key a mutation's table is listed under, which is how the writer remembers a name
     * it has already re-read the schema for.
     */
    String tableKeyOf(Mutation mutation) {
        return tableKey(mutation.getTable(), dialect);
    }

    /**
     * Returns the key a table is compared by: schema and table name without case, the schema
     * defaulted the dialect's way. Unlike the cell weights' key it folds PostgreSQL names too, so
     * that a spelling the service may resolve to the same table is never another table here.
     */
    private static String tableKey(@Nullable String table, Dialect dialect) {
        if (table == null) {
            return "";
        }
        int separator = table.indexOf('.');
        String schema;
        if (separator < 0) {
            schema = dialect == Dialect.POSTGRESQL ? "public" : "";
        } else {
            schema = table.substring(0, separator);
        }
        String name = separator < 0 ? table : table.substring(separator + 1);
        return schema.toLowerCase(Locale.ROOT) + '\u0000' + name.toLowerCase(Locale.ROOT);
    }

    @Override
    public String toString() {
        return "PrimaryKeys{tables=" + keyColumns.size() + "}";
    }

    /** One key column: its name as a mutation's column is matched, and its type. */
    private static final class KeyColumn {

        private final String name;
        private final SpannerTypeSpelling.Kind kind;

        private KeyColumn(String name, SpannerTypeSpelling.Kind kind) {
            this.name = name;
            this.kind = kind;
        }
    }

    /** The rows one mutation addresses: some rows of a table, all of them, or every table's. */
    @Internal
    static final class Footprint {

        private static final Footprint WHOLE_DATABASE = new Footprint(null, null);

        /** The table key; {@code null} for the whole database. */
        @Nullable private final String table;

        /** The rows, each as its compared key values; {@code null} for the whole table. */
        @Nullable private final Set<List<Object>> rows;

        private Footprint(@Nullable String table, @Nullable Set<List<Object>> rows) {
            this.table = table;
            this.rows = rows;
        }

        static Footprint wholeDatabase() {
            return WHOLE_DATABASE;
        }

        static Footprint wholeTable(String table) {
            return new Footprint(table, null);
        }

        static Footprint rows(String table, Set<List<Object>> rows) {
            return new Footprint(table, rows);
        }

        /** The table key; only for a footprint that is not the whole database. */
        String table() {
            if (table == null) {
                throw new IllegalStateException("A whole-database footprint names no table.");
            }
            return table;
        }

        boolean isWholeDatabase() {
            return table == null;
        }

        /** Whether every row of the table is addressed, which the whole database implies. */
        boolean isWholeTable() {
            return rows == null;
        }

        /** The rows; only for a footprint that is not the whole table. */
        Set<List<Object>> rows() {
            if (rows == null) {
                throw new IllegalStateException("A whole-table footprint names no rows.");
            }
            return rows;
        }

        @Override
        public String toString() {
            if (isWholeDatabase()) {
                return "Footprint{whole database}";
            }
            return isWholeTable()
                    ? "Footprint{" + table + ", whole table}"
                    : "Footprint{" + table + ", " + rows.size() + " row(s)}";
        }
    }

    /** Builder for {@link PrimaryKeys}, fed one key-column row at a time. */
    @Internal
    static final class Builder {

        private final Dialect dialect;

        /** Folded table key to its catalog spelling, to find two tables that fold together. */
        private final Map<String, String> spellings = new HashMap<>();

        private final Map<String, TreeMap<Long, KeyColumn>> columns = new HashMap<>();
        private final Set<String> unnameable = new HashSet<>();

        private Builder(Dialect dialect) {
            this.dialect = dialect;
        }

        /** Records one key-column row in the table's default schema. */
        @VisibleForTesting
        Builder keyColumn(String table, String column, long position, String spannerType) {
            String defaultSchema = dialect == Dialect.POSTGRESQL ? "public" : "";
            return keyColumn(defaultSchema, table, column, position, spannerType);
        }

        /**
         * Records that {@code column} is the {@code position}-th column of the table's primary key.
         *
         * @param schema the table's schema
         * @param table the table
         * @param column the key column
         * @param position its one-based position in the key
         * @param spannerType the column's type as {@code INFORMATION_SCHEMA} spells it
         * @return this builder
         */
        Builder keyColumn(
                String schema, String table, String column, long position, String spannerType) {
            String key = foldedKey(schema, table);
            String spelling = SpannerTableName.catalogKey(schema, table, dialect);
            String previous = spellings.putIfAbsent(key, spelling);
            if (previous != null && !previous.equals(spelling)) {
                unnameable.add(key);
            }
            SpannerTypeSpelling type = SpannerTypeSpelling.parse(spannerType, dialect);
            columns.computeIfAbsent(key, t -> new TreeMap<>())
                    .put(
                            position,
                            new KeyColumn(
                                    SpannerTableName.catalogIdentifierKey(column, dialect),
                                    type.isArray()
                                            ? SpannerTypeSpelling.Kind.UNSUPPORTED
                                            : type.kind()));
            return this;
        }

        /** Records that a table's key could not be read whole, so its rows cannot be named. */
        Builder unnameable(String schema, String table) {
            unnameable.add(foldedKey(schema, table));
            return this;
        }

        private String foldedKey(String schema, String table) {
            return schema.toLowerCase(Locale.ROOT) + '\u0000' + table.toLowerCase(Locale.ROOT);
        }

        PrimaryKeys build() {
            Map<String, List<KeyColumn>> keyColumns = new HashMap<>();
            columns.forEach(
                    (table, byPosition) -> {
                        if (!unnameable.contains(table)) {
                            keyColumns.put(table, new ArrayList<>(byPosition.values()));
                        }
                    });
            Set<String> tables = new HashSet<>(columns.keySet());
            tables.addAll(unnameable);
            return new PrimaryKeys(dialect, keyColumns, tables);
        }
    }
}
