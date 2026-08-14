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

import com.google.cloud.spanner.Mutation;
import io.github.flink.gcp.connector.spanner.SpannerDatabase;
import io.github.flink.gcp.connector.spanner.sink.SpannerSink;

final class DynamicDestinationsSpannerTables {

    private DynamicDestinationsSpannerTables() {}

    static void build() {
        // tag::spanner-tables[]
        SpannerSink.<Event>builder()
                .database(SpannerDatabase.of("my-project", "my-instance", "events-db"))
                .serializer(
                        (event, context) ->
                                Mutation.newInsertOrUpdateBuilder(
                                                event.isAudit() ? "AuditEvents" : "Events")
                                        .set("EventId")
                                        .to(event.getId())
                                        .set("Body")
                                        .to(event.getBody())
                                        .build())
                .build();
        // end::spanner-tables[]
    }

    private static final class Event {

        private final boolean audit;
        private final String id;
        private final String body;

        private Event(boolean audit, String id, String body) {
            this.audit = audit;
            this.id = id;
            this.body = body;
        }

        boolean isAudit() {
            return audit;
        }

        String getId() {
            return id;
        }

        String getBody() {
            return body;
        }
    }
}
