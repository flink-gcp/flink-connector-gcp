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

package io.github.flink.gcp.connector.spanner.table.source;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinition;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.ReadContext;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.TimestampBound;
import io.github.flink.gcp.connector.spanner.DatabaseDestination;
import io.github.flink.gcp.connector.spanner.SpannerTableName;
import io.github.flink.gcp.connector.spanner.source.SpannerReadOperation;
import io.github.flink.gcp.connector.spanner.table.SpannerTableSchemaConverter;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Shared SQL, native-read and Flink oracles for emulator and real-service float-key tests. */
public final class SpannerFloatKeyReadTestSupport {
    private static final double[] VALUES = {
        Double.NEGATIVE_INFINITY,
        -Double.MAX_VALUE,
        -1.0d,
        -Double.MIN_VALUE,
        -0.0d,
        0.0d,
        Double.MIN_VALUE,
        1.0d,
        Math.nextUp(1.0d),
        Double.MAX_VALUE,
        Double.POSITIVE_INFINITY
    };
    private static final List<String> COLUMNS = List.of("bucket", "ratio", "id");
    private static final RowType ROW =
            (RowType)
                    DataTypes.ROW(
                                    DataTypes.FIELD("bucket", DataTypes.BIGINT().notNull()),
                                    DataTypes.FIELD("ratio", DataTypes.DOUBLE()),
                                    DataTypes.FIELD("id", DataTypes.BIGINT().notNull()))
                            .getLogicalType();

    private SpannerFloatKeyReadTestSupport() {}

    static String[] ddl(Dialect dialect) {
        boolean pg = dialect == Dialect.POSTGRESQL;
        String integer = pg ? "bigint" : "INT64";
        String floating = pg ? "float8" : "FLOAT64";
        List<String> ddl = new ArrayList<>();
        for (String table : tables(dialect)) {
            boolean indexed = table.equals("float_indexed");
            String key =
                    indexed
                            ? "id"
                            : "bucket, ratio" + (table.endsWith("desc") ? " DESC" : "") + ", id";
            String columns =
                    "bucket "
                            + integer
                            + " NOT NULL, ratio "
                            + floating
                            + (indexed ? "" : " NOT NULL")
                            + ", id "
                            + integer
                            + " NOT NULL";
            ddl.add(
                    "CREATE TABLE "
                            + table
                            + " ("
                            + columns
                            + (pg
                                    ? ", PRIMARY KEY (" + key + "))"
                                    : ") PRIMARY KEY (" + key + ")"));
        }
        ddl.add("CREATE INDEX float_by_ratio_asc ON float_indexed (bucket, ratio)");
        ddl.add("CREATE INDEX float_by_ratio_desc ON float_indexed (bucket, ratio DESC)");
        ddl.add(
                pg
                        ? "CREATE TABLE float_unique (ratio float8 NOT NULL PRIMARY KEY)"
                        : "CREATE TABLE float_unique (ratio FLOAT64 NOT NULL) PRIMARY KEY (ratio)");
        return ddl.toArray(new String[0]);
    }

    static Timestamp seed(DatabaseClient client, Dialect dialect, boolean includeNanKeys) {
        List<Mutation> mutations = new ArrayList<>();
        for (String table : tables(dialect)) {
            long id = 0;
            for (long bucket : new long[] {1L, 2L}) {
                for (double value : VALUES) {
                    mutations.add(row(table, bucket, value, ++id));
                }
            }
            if (includeNanKeys) {
                mutations.add(row(table, 1L, Double.NaN, ++id));
            }
            if (table.equals("float_indexed")) {
                mutations.add(row(table, 1L, null, ++id));
            }
        }
        return client.write(mutations);
    }

    static void assertNativeRanges(DatabaseClient client, Dialect dialect, Timestamp snapshot)
            throws Exception {
        for (AccessPath path : paths(dialect)) {
            for (Comparison comparison : Comparison.values()) {
                for (double bound :
                        new double[] {
                            Double.NEGATIVE_INFINITY, -0.0d, 0.0d, 1.0d, Double.POSITIVE_INFINITY
                        }) {
                    for (boolean reversed : List.of(false, true)) {
                        ResolvedExpression ratio = comparison.predicate(bound, reversed);
                        SpannerFilterPushDown.State state =
                                SpannerFilterPushDown.translate(
                                        schema(dialect, path.index != null),
                                        List.of(bucket(), ratio),
                                        path.index != null);
                        if (path.index == null) {
                            assertThat(state.result().getRemainingFilters()).isEmpty();
                        } else {
                            assertThat(state.result().getRemainingFilters())
                                    .containsExactly(bucket(), ratio);
                        }
                        SpannerTableName table = SpannerTableName.of(null, path.table, dialect);
                        SpannerReadOperation operation =
                                new SpannerTableReadResolver(
                                                schema(dialect, path.index != null),
                                                table,
                                                path.index == null
                                                        ? null
                                                        : table.accessPath(
                                                                path.index, "scan.index"),
                                                COLUMNS,
                                                false,
                                                dialect,
                                                state.runtime())
                                        .resolve(client, snapshot);
                        assertThat(operation.getKeys()).isNotEqualTo(KeySet.all());
                        String parameter = dialect == Dialect.POSTGRESQL ? "$1" : "@bound";
                        String condition =
                                reversed
                                        ? parameter + " " + comparison.sql + " ratio"
                                        : "ratio " + comparison.sql + " " + parameter;
                        Statement statement =
                                Statement.newBuilder(
                                                "SELECT id FROM "
                                                        + path.table
                                                        + " WHERE bucket = 1 AND "
                                                        + condition)
                                        .bind(dialect == Dialect.POSTGRESQL ? "p1" : "bound")
                                        .to(bound)
                                        .build();
                        try (ReadContext context =
                                        client.singleUse(TimestampBound.ofReadTimestamp(snapshot));
                                ResultSet expected = context.executeQuery(statement);
                                ReadContext read =
                                        client.singleUse(TimestampBound.ofReadTimestamp(snapshot));
                                ResultSet actual =
                                        path.index == null
                                                ? read.read(
                                                        operation.getTable(),
                                                        operation.getKeys(),
                                                        List.of("id"))
                                                : read.readUsingIndex(
                                                        operation.getTable(),
                                                        operation.getIndex(),
                                                        operation.getKeys(),
                                                        List.of("id"))) {
                            assertThat(ids(actual))
                                    .as(
                                            "%s %s %s bound=%s reversed=%s",
                                            dialect, path, condition, bound, reversed)
                                    .containsExactlyInAnyOrderElementsOf(ids(expected));
                        }
                    }
                }
            }
        }
    }

    static void assertSignedZeroKeys(DatabaseClient client) {
        client.write(
                List.of(Mutation.newInsertBuilder("float_unique").set("ratio").to(-0.0d).build()));
        assertThatThrownBy(
                        () ->
                                client.write(
                                        List.of(
                                                Mutation.newInsertBuilder("float_unique")
                                                        .set("ratio")
                                                        .to(0.0d)
                                                        .build())))
                .isInstanceOfSatisfying(
                        SpannerException.class,
                        e -> assertThat(e.getErrorCode()).isEqualTo(ErrorCode.ALREADY_EXISTS));
        try (ResultSet row =
                client.singleUse()
                        .read("float_unique", KeySet.singleKey(Key.of(0.0d)), List.of("ratio"))) {
            assertThat(row.next()).isTrue();
            assertThat(row.getDouble(0) == 0.0d).isTrue();
            assertThat(row.next()).isFalse();
        }
    }

    static void assertNanKeysAreRejected(DatabaseClient client, Dialect dialect) {
        for (String table : tables(dialect)) {
            assertThatThrownBy(
                            () -> client.write(List.of(row(table, 1L, Double.NaN, 1000L))),
                            "%s NaN key write to %s",
                            dialect,
                            table)
                    .isInstanceOfSatisfying(
                            SpannerException.class,
                            e -> {
                                assertThat(e.getErrorCode())
                                        .as(
                                                "%s NaN key write to %s: %s",
                                                dialect, table, e.getMessage())
                                        .isIn(
                                                ErrorCode.INVALID_ARGUMENT,
                                                ErrorCode.FAILED_PRECONDITION);
                                assertThat(e.getMessage())
                                        .containsIgnoringCase("key")
                                        .containsAnyOf("FLOAT64", "DOUBLE");
                            });
        }
    }

    static void assertFlinkScans(
            DatabaseDestination database, Dialect dialect, @Nullable String endpoint)
            throws Exception {
        TableEnvironment flink = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        flink.getConfig().set("parallelism.default", "1");
        flink.createTemporarySystemFunction("keep_double", KeepDouble.class);
        for (AccessPath path : paths(dialect)) {
            String name = path.index == null ? path.table : path.index;
            String key = path.index == null ? "bucket, ratio, id" : "id";
            flink.executeSql(
                    "CREATE TABLE "
                            + name
                            + " (bucket BIGINT, ratio DOUBLE, id BIGINT, PRIMARY KEY ("
                            + key
                            + ") NOT ENFORCED)"
                            + " WITH ('connector'='spanner', 'project'='"
                            + database.getProject()
                            + "', 'instance'='"
                            + database.getInstance()
                            + "', 'database'='"
                            + database.getDatabase()
                            + "', 'table'='"
                            + path.table
                            + "', 'dialect'='"
                            + dialect.name()
                            + "'"
                            + (path.index == null ? "" : ", 'scan.index'='" + path.index + "'")
                            + (endpoint == null ? "" : ", 'emulator-endpoint'='" + endpoint + "'")
                            + ")");
            String candidate = union(name, false);
            String residual = union(name, true);
            String plan =
                    flink.explainSql(
                            "SELECT id FROM " + name + " WHERE bucket = 1 AND ratio < 0.0E0");
            if (path.index == null) {
                assertThat(plan).contains("filter=[").doesNotContain("where=[");
            } else {
                assertThat(plan)
                        .contains("<(ratio")
                        .doesNotContain("filter=[]")
                        .contains("where=[");
            }
            assertThat(flink.explainSql(residual)).contains("keep_double").contains("where=[");
            assertThat(rows(flink, candidate))
                    .as("%s %s", dialect, path)
                    .containsExactlyInAnyOrderElementsOf(rows(flink, residual));
        }
    }

    private static String union(String table, boolean residual) {
        String ratio = residual ? "keep_double(ratio)" : "ratio";
        List<String> selects = new ArrayList<>();
        int label = 0;
        // Infinity string casts remain residual in Flink. The native-read and translator oracles
        // cover literal infinity bounds; these branches check equivalence with residual evaluation.
        for (String condition :
                List.of(
                        ratio + " < 0.0E0",
                        ratio + " <= 0.0E0",
                        ratio + " > 0.0E0",
                        ratio + " >= 0.0E0",
                        ratio + " = -1.0E0",
                        ratio + " = 1.0E0",
                        ratio + " >= CAST('Infinity' AS DOUBLE)",
                        ratio + " > CAST('Infinity' AS DOUBLE)",
                        ratio + " <= CAST('-Infinity' AS DOUBLE)",
                        ratio + " < CAST('-Infinity' AS DOUBLE)",
                        ratio + " > -0.0E0 AND " + ratio + " <= 0.0E0")) {
            selects.add(
                    "SELECT "
                            + label++
                            + " AS scenario, id FROM "
                            + table
                            + " WHERE bucket = 1 AND "
                            + condition);
        }
        return String.join(" UNION ALL ", selects);
    }

    private static List<Row> rows(TableEnvironment table, String sql) throws Exception {
        List<Row> rows = new ArrayList<>();
        try (CloseableIterator<Row> iterator = table.executeSql(sql).collect()) {
            iterator.forEachRemaining(rows::add);
        }
        return rows;
    }

    private static List<Long> ids(ResultSet rows) {
        List<Long> ids = new ArrayList<>();
        while (rows.next()) {
            ids.add(rows.getLong("id"));
        }
        return ids;
    }

    private static Mutation row(String table, long bucket, @Nullable Double ratio, long id) {
        return Mutation.newInsertBuilder(table)
                .set("bucket")
                .to(bucket)
                .set("ratio")
                .to(ratio)
                .set("id")
                .to(id)
                .build();
    }

    private static List<String> tables(Dialect dialect) {
        // PostgreSQL has no descending primary-key DDL; descending secondary indexes are supported.
        return dialect == Dialect.POSTGRESQL
                ? List.of("float_primary_asc", "float_indexed")
                : List.of("float_primary_asc", "float_primary_desc", "float_indexed");
    }

    private static List<AccessPath> paths(Dialect dialect) {
        List<AccessPath> paths = new ArrayList<>();
        paths.add(new AccessPath("float_primary_asc", null));
        if (dialect == Dialect.GOOGLE_STANDARD_SQL) {
            paths.add(new AccessPath("float_primary_desc", null));
        }
        paths.add(new AccessPath("float_indexed", "float_by_ratio_asc"));
        paths.add(new AccessPath("float_indexed", "float_by_ratio_desc"));
        return paths;
    }

    private static SpannerTableSchemaConverter schema(Dialect dialect, boolean index) {
        return SpannerTableSchemaConverter.of(
                ROW,
                index ? new int[] {2} : new int[] {0, 1, 2},
                dialect,
                List.of(),
                List.of(),
                Map.of(),
                Map.of());
    }

    private static ResolvedExpression bucket() {
        return CallExpression.permanent(
                BuiltInFunctionDefinitions.EQUALS,
                List.of(
                        new FieldReferenceExpression("bucket", DataTypes.BIGINT(), 0, 0),
                        new ValueLiteralExpression(1L)),
                DataTypes.BOOLEAN());
    }

    /**
     * Prevents filter pushdown while preserving every DOUBLE value, including signed zero and NaN.
     */
    public static final class KeepDouble extends ScalarFunction {
        public @Nullable Double eval(@Nullable Double value) {
            return value;
        }
    }

    private static final class AccessPath {
        private final String table;
        @Nullable private final String index;

        private AccessPath(String table, @Nullable String index) {
            this.table = table;
            this.index = index;
        }

        @Override
        public String toString() {
            return table + "/" + index;
        }
    }

    private enum Comparison {
        LESS("<", BuiltInFunctionDefinitions.LESS_THAN),
        LESS_EQUAL("<=", BuiltInFunctionDefinitions.LESS_THAN_OR_EQUAL),
        GREATER(">", BuiltInFunctionDefinitions.GREATER_THAN),
        GREATER_EQUAL(">=", BuiltInFunctionDefinitions.GREATER_THAN_OR_EQUAL),
        EQUAL("=", BuiltInFunctionDefinitions.EQUALS);

        private final String sql;
        private final BuiltInFunctionDefinition function;

        Comparison(String sql, BuiltInFunctionDefinition function) {
            this.sql = sql;
            this.function = function;
        }

        private ResolvedExpression predicate(double value, boolean reversed) {
            FieldReferenceExpression field =
                    new FieldReferenceExpression("ratio", DataTypes.DOUBLE(), 1, 1);
            ValueLiteralExpression literal = new ValueLiteralExpression(value);
            return CallExpression.permanent(
                    function,
                    reversed ? List.of(literal, field) : List.of(field, literal),
                    DataTypes.BOOLEAN());
        }
    }
}
