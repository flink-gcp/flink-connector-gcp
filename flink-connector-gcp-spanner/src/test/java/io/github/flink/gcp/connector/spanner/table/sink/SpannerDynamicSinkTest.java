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

package io.github.flink.gcp.connector.spanner.table.sink;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.utils.FactoryMocks;
import org.apache.flink.types.RowKind;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class SpannerDynamicSinkTest {

    private static final ResolvedSchema SCHEMA =
            ResolvedSchema.of(
                    Column.physical("id", DataTypes.BIGINT().notNull()),
                    Column.physical("name", DataTypes.STRING()));

    private static Map<String, String> options() {
        Map<String, String> options = new HashMap<>();
        options.put("connector", "spanner");
        options.put("project", "project");
        options.put("instance", "instance");
        options.put("database", "database");
        options.put("table", "people");
        return options;
    }

    private static DynamicTableSink sink(boolean primaryKey) {
        ResolvedSchema schema = SCHEMA;
        if (primaryKey) {
            schema =
                    new ResolvedSchema(
                            SCHEMA.getColumns(),
                            Collections.emptyList(),
                            UniqueConstraint.primaryKey("pk", Arrays.asList("id")));
        }
        return FactoryMocks.createTableSink(schema, options());
    }

    @Test
    void noPrimaryKeyIsAppendOnly() {
        assertThat(sink(false).getChangelogMode(ChangelogMode.all()))
                .isEqualTo(ChangelogMode.insertOnly());
    }

    @Test
    void aPrimaryKeyAcceptsUpsertsButKeepsInsertOnlyInputsAppendOnly() {
        ChangelogMode upsert = sink(true).getChangelogMode(ChangelogMode.all());

        assertThat(upsert).isEqualTo(CrossVersionChangelogMode.upsert());
        assertThat(upsert.getContainedKinds())
                .containsExactlyInAnyOrder(RowKind.INSERT, RowKind.UPDATE_AFTER, RowKind.DELETE);
        assertThat(sink(true).getChangelogMode(ChangelogMode.insertOnly()))
                .isEqualTo(ChangelogMode.insertOnly());
    }
}
