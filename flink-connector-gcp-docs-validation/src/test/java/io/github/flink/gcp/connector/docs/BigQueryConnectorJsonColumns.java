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

import io.github.flink.gcp.connector.bigquery.sink.serializer.proto.ProtoSchemaOptions;
import io.github.flink.gcp.connector.docs.BigQueryDocumentationTypes.MyAnnotations;

final class BigQueryConnectorJsonColumns {

    private BigQueryConnectorJsonColumns() {}

    static void buildPaths() {
        // tag::bigquery-connector-json-columns-paths[]
        ProtoSchemaOptions.builder()
                .jsonFieldPath("payload")
                .jsonFieldPath("event.details")
                .build();
        // end::bigquery-connector-json-columns-paths[]
    }

    static void buildOption() {
        // tag::bigquery-connector-json-columns-option[]
        ProtoSchemaOptions.builder().jsonFieldOption(MyAnnotations.json).build();
        // end::bigquery-connector-json-columns-option[]
    }

    static void buildOptionNumber() {
        // tag::bigquery-connector-json-columns-option-number[]
        ProtoSchemaOptions.builder().jsonFieldOptionNumber(50000).build();
        // end::bigquery-connector-json-columns-option-number[]
    }
}
