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

package io.github.flink.gcp.connector.bigquery.table.source;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import org.apache.avro.generic.GenericRecord;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;

/** Evaluates decoded Storage Read rows through Flink SQL without a remote prefilter. */
final class BinaryFilterOracle {
    private BinaryFilterOracle() {}

    static List<Map<String, Set<Long>>> evaluate(
            List<List<GenericRecord>> batches, DataType binaryType, List<String> predicates)
            throws Exception {
        RowType physical =
                (RowType)
                        DataTypes.ROW(
                                        DataTypes.FIELD("id", DataTypes.BIGINT()),
                                        DataTypes.FIELD("binary_value", binaryType))
                                .getLogicalType();
        GenericRecordToRowDataConverter converter =
                new GenericRecordToRowDataConverter(physical, new int[] {0, 1});
        RowType inputType =
                (RowType)
                        DataTypes.ROW(
                                        DataTypes.FIELD("batch_id", DataTypes.INT()),
                                        DataTypes.FIELD("id", DataTypes.BIGINT()),
                                        DataTypes.FIELD("binary_value", binaryType))
                                .getLogicalType();
        List<RowData> input = new ArrayList<>();
        List<Map<Long, byte[]>> originals = new ArrayList<>();
        List<Map<String, Set<Long>>> matches = new ArrayList<>();
        for (int batch = 0; batch < batches.size(); batch++) {
            Map<Long, byte[]> values = new HashMap<>();
            originals.add(values);
            Map<String, Set<Long>> results = new LinkedHashMap<>();
            predicates.forEach(predicate -> results.put(predicate, new TreeSet<>()));
            matches.add(results);
            for (GenericRecord record : batches.get(batch)) {
                RowData row = converter.convert(record);
                byte[] bytes = row.isNullAt(1) ? null : row.getBinary(1);
                long id = row.getLong(0);
                assertThat(values.containsKey(id))
                        .as("duplicate id %s in batch %s", id, batch)
                        .isFalse();
                values.put(id, bytes);
                input.add(GenericRowData.of(batch, id, bytes));
            }
        }
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.setParallelism(1);
        StreamTableEnvironment table = StreamTableEnvironment.create(env);
        // RowData enters with its declared type and actual byte length. fromValues would instead
        // turn the inputs into planner literals and could hide source conversion/cast differences.
        table.createTemporaryView(
                "decoded_rows",
                table.fromDataStream(env.fromCollection(input, InternalTypeInfo.of(inputType))));
        StringBuilder query = new StringBuilder("SELECT batch_id, id, binary_value");
        for (int i = 0; i < predicates.size(); i++) {
            query.append(", (").append(predicates.get(i)).append(") AS p").append(i);
        }
        query.append(" FROM decoded_rows");
        int count = 0;
        try (CloseableIterator<Row> rows = table.executeSql(query.toString()).collect()) {
            while (rows.hasNext()) {
                Row row = rows.next();
                int batch = (Integer) row.getField(0);
                long id = (Long) row.getField(1);
                assertThat(originals.get(batch)).containsKey(id);
                assertThat((byte[]) row.getField(2))
                        .as("unmodified bytes for batch %s, id %s, type %s", batch, id, binaryType)
                        .isEqualTo(originals.get(batch).remove(id));
                for (int i = 0; i < predicates.size(); i++) {
                    if (Boolean.TRUE.equals(row.getField(i + 3))) {
                        matches.get(batch).get(predicates.get(i)).add(id);
                    }
                }
                count++;
            }
        }
        assertThat(count).isEqualTo(input.size());
        originals.forEach(values -> assertThat(values).isEmpty());
        return matches;
    }
}
