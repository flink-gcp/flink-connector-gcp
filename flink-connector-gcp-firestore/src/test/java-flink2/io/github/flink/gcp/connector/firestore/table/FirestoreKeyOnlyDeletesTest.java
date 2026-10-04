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

package io.github.flink.gcp.connector.firestore.table;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.factories.utils.FactoryMocks;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Holds the Flink 2.x half of the changelog shim: a keyed table declares that a delete may carry
 * the key alone, which is all the serializer reads from one.
 */
class FirestoreKeyOnlyDeletesTest {

    private static final ResolvedSchema KEYED =
            new ResolvedSchema(
                    List.of(
                            Column.physical("id", DataTypes.STRING().notNull()),
                            Column.physical("n", DataTypes.BIGINT().notNull())),
                    List.of(),
                    UniqueConstraint.primaryKey("pk", List.of("id")));

    @Test
    void aKeyedTableDeclaresKeyOnlyDeletes() {
        ChangelogMode mode =
                FactoryMocks.createTableSink(
                                KEYED,
                                Map.of(
                                        "connector", "firestore",
                                        "project", "p",
                                        "collection", "c"))
                        .getChangelogMode(ChangelogMode.all());

        assertThat(mode).isEqualTo(ChangelogMode.upsert(true));
        assertThat(mode.keyOnlyDeletes()).isTrue();
    }
}
