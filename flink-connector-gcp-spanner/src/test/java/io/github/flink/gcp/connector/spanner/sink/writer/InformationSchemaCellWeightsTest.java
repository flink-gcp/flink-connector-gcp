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

package io.github.flink.gcp.connector.spanner.sink.writer;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.ResultSets;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link InformationSchemaCellWeights}. */
class InformationSchemaCellWeightsTest {

    private static final Type ROW_TYPE =
            Type.struct(
                    Type.StructField.of("table_schema", Type.string()),
                    Type.StructField.of("table_name", Type.string()),
                    Type.StructField.of("column_name", Type.string()),
                    Type.StructField.of("index_name", Type.string()));

    @ParameterizedTest
    @EnumSource(Dialect.class)
    void hasAQueryForEveryDialectTheClientLibraryKnows(Dialect dialect) {
        // A dialect added to the client library must fail here rather than silently read nothing,
        // which would undercount every mutation of every table.
        assertThat(InformationSchemaCellWeights.queryFor(dialect)).isNotBlank();
    }

    @Test
    void readsAllGoogleSqlUserSchemasAndSkipsThePrimaryKey() {
        String sql = InformationSchemaCellWeights.queryFor(Dialect.GOOGLE_STANDARD_SQL);

        assertThat(sql)
                .contains("INFORMATION_SCHEMA.INDEX_COLUMNS")
                .contains("TABLE_CATALOG = ''")
                .contains("TABLE_SCHEMA NOT IN ('INFORMATION_SCHEMA', 'SPANNER_SYS')")
                .doesNotContain("TABLE_SCHEMA = ''")
                .contains("INDEX_NAME != 'PRIMARY_KEY'");
    }

    @Test
    void readsAllPostgresUserSchemasAndSkipsThePrimaryKey() {
        String sql = InformationSchemaCellWeights.queryFor(Dialect.POSTGRESQL);

        assertThat(sql)
                .contains("information_schema.index_columns")
                .contains(
                        "table_schema NOT IN"
                                + " ('pg_catalog', 'information_schema', 'SPANNER_SYS')")
                .doesNotContain("table_schema = 'public'")
                .contains("index_name != 'PRIMARY_KEY'");
    }

    @Test
    void buildsWeightsFromTheRowsTheQueryReturns() {
        ResultSet resultSet =
                rows(
                        row("", "Orders", "Total", "OrdersByTotal"),
                        row("", "Orders", "Note", "OrdersByTotalAndNote"),
                        row("", "Orders", "Total", "OrdersByTotalAndNote"));

        CellWeights weights =
                InformationSchemaCellWeights.read(resultSet, Dialect.GOOGLE_STANDARD_SQL);

        assertThat(weights.knows("Orders")).isTrue();
        assertThat(weights.weigh(Mutation.newInsertBuilder("Orders").set("Total").to(1L).build()))
                .isEqualTo(3);
    }

    @Test
    void buildsIndependentWeightsForEveryReturnedSchema() {
        ResultSet resultSet =
                rows(
                        row("sales", "Orders", "Total", "OrdersByTotal"),
                        row("archive", "Orders", "Note", "OrdersByNote"));

        CellWeights weights =
                InformationSchemaCellWeights.read(resultSet, Dialect.GOOGLE_STANDARD_SQL);

        assertThat(
                        weights.weigh(
                                Mutation.newInsertBuilder("sales.Orders")
                                        .set("Total")
                                        .to(1L)
                                        .build()))
                .isEqualTo(2);
        assertThat(
                        weights.weigh(
                                Mutation.newInsertBuilder("archive.Orders")
                                        .set("Note")
                                        .to("kept")
                                        .build()))
                .isEqualTo(2);
    }

    @Test
    void skipsARowWithAMissingName() {
        // Nothing in either dialect's view is documented to be null here, so the guard is defence
        // in depth — but reading a null as a name would throw and take the whole job's startup
        // with it.
        ResultSet resultSet =
                rows(
                        row("", "Orders", null, "OrdersByTotal"),
                        row("", null, "Total", "OrdersByTotal"),
                        row(null, "Orders", "Total", "OrdersByTotal"),
                        row("", "Orders", "Total", null),
                        row("", "Orders", "Total", "OrdersByTotal"));

        CellWeights weights =
                InformationSchemaCellWeights.read(resultSet, Dialect.GOOGLE_STANDARD_SQL);

        assertThat(weights.weigh(Mutation.newInsertBuilder("Orders").set("Total").to(1L).build()))
                .isEqualTo(2);
    }

    @Test
    void anEmptyResultMeansNoIndexesAtAll() {
        CellWeights weights =
                InformationSchemaCellWeights.read(rows(), Dialect.GOOGLE_STANDARD_SQL);

        assertThat(weights.indexedTableCount()).isZero();
        assertThat(weights.weigh(Mutation.newInsertBuilder("Orders").set("Total").to(1L).build()))
                .isEqualTo(1);
    }

    private static ResultSet rows(Struct... structs) {
        List<Struct> list = structs.length == 0 ? Collections.emptyList() : Arrays.asList(structs);
        return ResultSets.forRows(ROW_TYPE, list);
    }

    private static Struct row(String schema, String table, String column, String index) {
        return Struct.newBuilder()
                .set("table_schema")
                .to(schema)
                .set("table_name")
                .to(table)
                .set("column_name")
                .to(column)
                .set("index_name")
                .to(index)
                .build();
    }
}
