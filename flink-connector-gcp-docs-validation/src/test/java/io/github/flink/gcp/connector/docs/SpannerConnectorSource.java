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

package io.github.flink.gcp.connector.docs;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.google.cloud.spanner.Statement;
import io.github.flink.gcp.connector.docs.SpannerDocumentationTypes.Singer;
import io.github.flink.gcp.connector.docs.SpannerDocumentationTypes.SingerDeserializer;
import io.github.flink.gcp.connector.spanner.DatabaseDestination;
import io.github.flink.gcp.connector.spanner.source.SpannerReadOperation;
import io.github.flink.gcp.connector.spanner.source.SpannerSource;

final class SpannerConnectorSource {

    private SpannerConnectorSource() {}

    static void build(StreamExecutionEnvironment env) {
        // tag::spanner-connector-source[]
        Source<Singer, ?, ?> source =
                SpannerSource.<Singer>builder()
                        .database(DatabaseDestination.of("my-project", "my-instance", "my-db"))
                        .readOperation(
                                SpannerReadOperation.query(
                                        Statement.of("SELECT id, name FROM singers")))
                        .deserializer(new SingerDeserializer())
                        .build();

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "singers");
        // end::spanner-connector-source[]
    }
}
