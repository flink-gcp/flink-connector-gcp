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

import io.github.flink.gcp.connector.bigquery.sink.serializer.avro.AvroSchemaOptions;
import io.github.flink.gcp.connector.bigquery.sink.serializer.proto.ProtoSchemaOptions;
import io.github.flink.gcp.connector.docs.BigQueryDocumentationTypes.MyAnnotations;

final class BigQueryConnectorGeographyColumns {

    private BigQueryConnectorGeographyColumns() {}

    static void buildPaths() {
        // tag::bigquery-connector-geography-columns-paths[]
        ProtoSchemaOptions.builder().geographyFieldPath("site.boundary").build();
        AvroSchemaOptions.builder().geographyFieldPath("site.boundary").build();
        // end::bigquery-connector-geography-columns-paths[]
    }

    static void buildOption() {
        // tag::bigquery-connector-geography-columns-option[]
        ProtoSchemaOptions.builder().geographyFieldOption(MyAnnotations.geography).build();
        // or, when only the number is available:
        ProtoSchemaOptions.builder().geographyFieldOptionNumber(50006).build();
        // end::bigquery-connector-geography-columns-option[]
    }
}
