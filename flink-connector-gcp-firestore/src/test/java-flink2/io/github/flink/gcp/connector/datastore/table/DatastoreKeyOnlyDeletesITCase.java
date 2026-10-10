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

package io.github.flink.gcp.connector.datastore.table;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import com.google.cloud.datastore.Entity;
import io.github.flink.gcp.connector.datastore.AbstractDatastoreEmulatorITCase;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A Flink 2.x input whose deletes carry the key alone, written into a table: the sink declares it
 * reads a delete's key alone, so Flink hands the delete on without normalizing the input, and the
 * entity goes even when the job never saw its key.
 *
 * <p>The table's other columns are nullable: on Flink 2.2 and 2.3 the not-null enforcer checks a
 * key-only delete's other columns too and fails the job, a Flink bug fixed in 2.4.0 (FLINK-40477).
 */
@Testcontainers
class DatastoreKeyOnlyDeletesITCase extends AbstractDatastoreEmulatorITCase {

    private static Table keyOnlyChangelog(
            StreamExecutionEnvironment env, StreamTableEnvironment table, Row... rows) {
        return table.fromChangelogStream(
                env.fromData(rows)
                        .returns(
                                Types.ROW_NAMED(
                                        new String[] {"id", "n"}, Types.STRING, Types.LONG)),
                Schema.newBuilder()
                        .column("id", DataTypes.STRING().notNull())
                        .column("n", DataTypes.BIGINT())
                        .primaryKey("id")
                        .build(),
                ChangelogMode.upsert(true));
    }

    private static String target(String kind) {
        return "CREATE TABLE target (id STRING NOT NULL, n BIGINT,"
                + " PRIMARY KEY (id) NOT ENFORCED) WITH ('connector' = 'datastore',"
                + " 'project' = '"
                + PROJECT
                + "', 'kind' = '"
                + kind
                + "', 'emulator-endpoint' = '"
                + emulatorEndpoint()
                + "')";
    }

    @Test
    void aTableOfNullableColumnsTakesAKeyOnlyDeleteAsItIs() throws Exception {
        String kind = uniqueKind();
        client().put(Entity.newBuilder(key(kind, "unseen")).set("n", 9L).build());
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment table = StreamTableEnvironment.create(env);
        Table changes =
                keyOnlyChangelog(env, table, Row.ofKind(RowKind.DELETE, "unseen", null));
        table.executeSql(target(kind));

        changes.executeInsert("target").await(60, TimeUnit.SECONDS);

        assertThat(read(key(kind, "unseen"))).isNull();
    }
}
