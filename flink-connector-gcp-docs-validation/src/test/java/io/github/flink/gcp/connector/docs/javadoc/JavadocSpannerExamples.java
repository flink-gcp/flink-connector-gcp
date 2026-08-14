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

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.source.Source;

import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Statement;
import io.github.flink.gcp.connector.docs.SpannerDocumentationTypes.OrderEvent;
import io.github.flink.gcp.connector.docs.SpannerDocumentationTypes.Singer;
import io.github.flink.gcp.connector.spanner.SpannerDatabase;
import io.github.flink.gcp.connector.spanner.sink.SpannerSink;
import io.github.flink.gcp.connector.spanner.source.SpannerReadOperation;
import io.github.flink.gcp.connector.spanner.source.SpannerSource;
import io.github.flink.gcp.connector.spanner.source.serializer.SpannerStructDeserializationSchema;

import java.util.Arrays;

final class JavadocSpannerExamples {

    private JavadocSpannerExamples() {}

    static Sink<OrderEvent> sink() {
        // tag::sink[]
        Sink<OrderEvent> sink =
                SpannerSink.<OrderEvent>builder()
                        .database(SpannerDatabase.of("my-project", "my-instance", "orders-db"))
                        .serializer(
                                (event, context) ->
                                        Mutation.newInsertOrUpdateBuilder("Orders")
                                                .set("OrderId")
                                                .to(event.getId())
                                                .set("Total")
                                                .to(event.getTotal())
                                                .build())
                        .build();
        // end::sink[]
        return sink;
    }

    static Source<Singer, ?, ?> source(
            SpannerStructDeserializationSchema<Singer> mySingerDeserializer) {
        // tag::source[]
        Source<Singer, ?, ?> source =
                SpannerSource.<Singer>builder()
                        .database(SpannerDatabase.of("my-project", "my-instance", "my-db"))
                        .readOperation(
                                SpannerReadOperation.query(
                                        Statement.of("SELECT id, name FROM singers")))
                        .deserializer(mySingerDeserializer)
                        .build();
        // end::source[]
        return source;
    }

    static void readOperations() {
        // tag::read-operations[]
        SpannerReadOperation.query(Statement.of("SELECT id, name FROM singers"));
        SpannerReadOperation.read("singers", KeySet.all(), Arrays.asList("id", "name"));
        SpannerReadOperation.readUsingIndex(
                "singers", "singers_by_name", KeySet.all(), Arrays.asList("id", "name"));
        // end::read-operations[]
    }
}
