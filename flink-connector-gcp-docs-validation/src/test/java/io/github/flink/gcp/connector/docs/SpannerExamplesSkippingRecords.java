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

import com.google.cloud.spanner.Mutation;
import io.github.flink.gcp.connector.docs.SpannerDocumentationTypes.Event;
import io.github.flink.gcp.connector.spanner.SpannerDatabase;
import io.github.flink.gcp.connector.spanner.sink.SpannerSink;

final class SpannerExamplesSkippingRecords {

    private SpannerExamplesSkippingRecords() {}

    static void build() {
        // tag::spanner-examples-skipping-records[]
        SpannerSink.<Event>builder()
                .database(SpannerDatabase.of("my-project", "my-instance", "events-db"))
                .serializer(
                        (event, context) ->
                                event.isHeartbeat()
                                        ? null
                                        : Mutation.newInsertOrUpdateBuilder("Events")
                                                .set("EventId")
                                                .to(event.getId())
                                                .build())
                .build();
        // end::spanner-examples-skipping-records[]
    }
}
