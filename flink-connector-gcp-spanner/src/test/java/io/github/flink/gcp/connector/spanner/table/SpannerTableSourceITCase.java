/*
 * Copyright 2026 laughingman7743
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package io.github.flink.gcp.connector.spanner.table;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Value;
import io.github.flink.gcp.connector.spanner.AbstractSpannerEmulatorITCase;
import io.github.flink.gcp.connector.spanner.SpannerDatabase;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** End-to-end bounded SQL scan coverage against both emulator dialects. */
class SpannerTableSourceITCase extends AbstractSpannerEmulatorITCase {

    @ParameterizedTest
    @EnumSource(Dialect.class)
    void scansProjectedColumnsAndAggregates(Dialect dialect) throws Exception {
        SpannerDatabase database = database(dialect);
        client(database)
                .write(
                        List.of(
                                Mutation.newInsertBuilder("records")
                                        .set("id")
                                        .to(1L)
                                        .set("name")
                                        .to("Ada")
                                        .set("metadata")
                                        .to(json(dialect, "{\"rank\":1}"))
                                        .build(),
                                Mutation.newInsertBuilder("records")
                                        .set("id")
                                        .to(2L)
                                        .set("name")
                                        .to("Grace")
                                        .set("metadata")
                                        .to(json(dialect, "{\"rank\":2}"))
                                        .build()));
        TableEnvironment table =
                TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build());
        table.executeSql(tableDdl(database, dialect));

        assertThat(rows(table, "SELECT name FROM source ORDER BY name"))
                .extracting(row -> row.getFieldAs(0).toString())
                .containsExactly("Ada", "Grace");
        assertThat(rows(table, "SELECT metadata FROM source WHERE id = 1"))
                .singleElement()
                .satisfies(
                        row ->
                                assertThat(row.getField(0).toString())
                                        .matches("\\{\\\"rank\\\":\\s*1}"));
        assertThat(rows(table, "SELECT COUNT(*) FROM source"))
                .singleElement()
                .satisfies(row -> assertThat((Object) row.getField(0)).isEqualTo(2L));
    }

    private static List<Row> rows(TableEnvironment table, String sql) throws Exception {
        List<Row> rows = new ArrayList<>();
        try (CloseableIterator<Row> iterator = table.executeSql(sql).collect()) {
            iterator.forEachRemaining(rows::add);
        }
        return rows;
    }

    private static SpannerDatabase database(Dialect dialect) throws Exception {
        if (dialect == Dialect.POSTGRESQL) {
            return createDatabase(
                    dialect,
                    "CREATE TABLE records (id bigint NOT NULL PRIMARY KEY, name varchar(64), metadata jsonb)");
        }
        return createDatabase(
                dialect,
                "CREATE TABLE records (id INT64 NOT NULL, name STRING(64), metadata JSON) PRIMARY KEY (id)");
    }

    private static Value json(Dialect dialect, String value) {
        return dialect == Dialect.POSTGRESQL ? Value.pgJsonb(value) : Value.json(value);
    }

    private static String tableDdl(SpannerDatabase database, Dialect dialect) {
        return "CREATE TABLE source (\n"
                + "  id BIGINT,\n"
                + "  name STRING,\n"
                + "  metadata STRING,\n"
                + "  PRIMARY KEY (id) NOT ENFORCED\n"
                + ") WITH (\n"
                + "  'connector' = 'spanner',\n"
                + "  'project' = '"
                + database.getProject()
                + "',\n"
                + "  'instance' = '"
                + database.getInstance()
                + "',\n"
                + "  'database' = '"
                + database.getDatabase()
                + "',\n"
                + "  'table' = 'records',\n"
                + "  'dialect' = '"
                + dialect.name()
                + "',\n"
                + "  'schema.json-field-paths' = 'metadata',\n"
                + "  'emulator-endpoint' = '"
                + emulatorEndpoint()
                + "'\n"
                + ")";
    }
}
