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

package io.github.flink.gcp.connector.docs;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import com.google.cloud.bigtable.data.v2.models.Filters;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowCell;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.source.BigtableSource;
import io.github.flink.gcp.connector.bigtable.source.serializer.BigtableRowDeserializationSchema;

// tag::bigtable-quickstart-read[]
public class BigtableQuickstartRead {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Source<String, ?, ?> source =
                BigtableSource.<String>builder()
                        .table(TableDestination.of("my-project", "my-instance", "orders"))
                        // Zero or more records per row: this one emits the payload of each cell.
                        .deserializer(
                                new BigtableRowDeserializationSchema<String>() {
                                    @Override
                                    public void deserialize(Row row, Collector<String> out) {
                                        for (RowCell cell : row.getCells("cf", "payload")) {
                                            out.collect(
                                                    row.getKey().toStringUtf8()
                                                            + " = "
                                                            + cell.getValue().toStringUtf8());
                                        }
                                    }

                                    @Override
                                    public TypeInformation<String> getProducedType() {
                                        return TypeInformation.of(String.class);
                                    }
                                })
                        // Only the rows this job needs: a prefix is sugar for the range it
                        // describes, and what a filter excludes never leaves the server.
                        .prefix("order#")
                        .filter(Filters.FILTERS.family().exactMatch("cf"))
                        .build();

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "orders").print();
        env.execute("read-orders");
    }
}
// end::bigtable-quickstart-read[]
