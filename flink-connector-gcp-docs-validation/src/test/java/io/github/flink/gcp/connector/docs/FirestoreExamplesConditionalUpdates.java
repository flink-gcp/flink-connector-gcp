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

import com.google.cloud.Timestamp;
import io.github.flink.gcp.connector.base.failure.FailureHandler;
import io.github.flink.gcp.connector.firestore.DatabaseDestination;
import io.github.flink.gcp.connector.firestore.sink.FirestoreSink;
import io.github.flink.gcp.connector.firestore.sink.FirestoreWrite;
import io.github.flink.gcp.connector.firestore.sink.PreconditionFailurePolicy;

import java.util.Map;

final class FirestoreExamplesConditionalUpdates {

    private FirestoreExamplesConditionalUpdates() {}

    /** A price change computed from a document snapshot whose update time it carries. */
    static final class PriceChange {

        String getProductId() {
            return "p-1";
        }

        double getPrice() {
            return 1.0;
        }

        Timestamp getUpdateTime() {
            return Timestamp.now();
        }
    }

    static void build() {
        // tag::firestore-examples-conditional-updates[]
        FirestoreSink.<PriceChange>builder()
                .database(DatabaseDestination.of("my-project"))
                .serializer(
                        (change, context) ->
                                FirestoreWrite.update(
                                        "products/" + change.getProductId(),
                                        Map.of("price", change.getPrice()),
                                        change.getUpdateTime()))
                .preconditionFailurePolicy(PreconditionFailurePolicy.ROUTE_TO_FAILURE_HANDLER)
                .failedWriteHandler(FailureHandler.logAndDrop())
                .build();
        // end::firestore-examples-conditional-updates[]
    }
}
