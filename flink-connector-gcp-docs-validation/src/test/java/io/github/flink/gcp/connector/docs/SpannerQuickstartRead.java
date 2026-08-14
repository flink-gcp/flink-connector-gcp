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

import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Struct;
import io.github.flink.gcp.connector.spanner.SpannerDatabase;
import io.github.flink.gcp.connector.spanner.source.SpannerReadOperation;
import io.github.flink.gcp.connector.spanner.source.SpannerSource;
import io.github.flink.gcp.connector.spanner.source.serializer.SpannerStructDeserializationSchema;

final class SpannerQuickstartRead {

    private SpannerQuickstartRead() {}

    static void run() throws Exception {
        // tag::spanner-quickstart-read[]
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Source<String, ?, ?> source =
                SpannerSource.<String>builder()
                        .database(SpannerDatabase.of("my-project", "my-instance", "orders-db"))
                        .readOperation(
                                SpannerReadOperation.query(
                                        Statement.of("SELECT OrderId FROM Orders")))
                        .deserializer(
                                new SpannerStructDeserializationSchema<String>() {
                                    @Override
                                    public void deserialize(Struct row, Collector<String> out) {
                                        out.collect(row.getString("OrderId"));
                                    }

                                    @Override
                                    public TypeInformation<String> getProducedType() {
                                        return TypeInformation.of(String.class);
                                    }
                                })
                        .build();

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "orders").print();
        env.execute("spanner-read-quickstart");
        // end::spanner-quickstart-read[]
    }
}
