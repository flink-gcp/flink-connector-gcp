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

package io.github.flink.gcp.connector.bigquery;

import com.google.cloud.bigquery.storage.v1.TableSchema;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.tables.StorageSchemaConverter;
import io.github.flink.gcp.connector.testutils.bigquery.RealBigQuery;

/**
 * The two gated-ITCase helpers that take connector types, and so cannot live beside {@link
 * RealBigQuery} in the test utilities, which the connector depends on rather than the reverse.
 */
public final class RealTables {

    private RealTables() {}

    /** The gated dataset's destination for {@code table}, as the sink builders take it. */
    public static TableDestination destination(String table) {
        return TableDestination.of(RealBigQuery.project(), RealBigQuery.dataset(), table);
    }

    /** Creates {@code table} in the gated dataset with the given Storage Write API schema. */
    public static void createTable(String table, TableSchema schema) {
        RealBigQuery.createTable(table, StorageSchemaConverter.toBigQuerySchema(schema));
    }
}
