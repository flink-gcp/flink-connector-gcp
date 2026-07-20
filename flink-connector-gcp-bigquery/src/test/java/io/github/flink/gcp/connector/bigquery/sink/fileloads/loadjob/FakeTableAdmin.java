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

package io.github.flink.gcp.connector.bigquery.sink.fileloads.loadjob;

import com.google.cloud.bigquery.storage.v1.TableSchema;
import io.github.flink.gcp.connector.bigquery.sink.TableCreateOptions;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.tables.TableAdmin;
import io.github.flink.gcp.connector.bigquery.sink.tables.TableSchemaSnapshot;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Recording in-memory {@link TableAdmin} fake with scriptable lost update races. */
public final class FakeTableAdmin implements TableAdmin {

    public final Map<TableDestination, TableSchema> tables = new HashMap<>();
    public final List<TableDestination> created = new ArrayList<>();
    public final List<TableDestination> schemaUpdates = new ArrayList<>();
    public int updateRacesToLose;

    @Override
    public void create(
            TableDestination destination, TableSchema schema, TableCreateOptions options) {
        tables.putIfAbsent(destination, schema);
        created.add(destination);
    }

    @Override
    public TableSchemaSnapshot getSchema(TableDestination destination) {
        TableSchema schema = tables.get(destination);
        return schema == null ? null : TableSchemaSnapshot.of(schema, null);
    }

    @Override
    public boolean updateSchema(
            TableDestination destination, TableSchemaSnapshot base, TableSchema proposed) {
        if (updateRacesToLose > 0) {
            updateRacesToLose--;
            return false;
        }
        tables.put(destination, proposed);
        schemaUpdates.add(destination);
        return true;
    }
}
