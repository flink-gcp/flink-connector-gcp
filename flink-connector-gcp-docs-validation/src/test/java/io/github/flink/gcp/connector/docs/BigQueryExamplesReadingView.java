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

import io.github.flink.gcp.connector.bigquery.source.BigQuerySource;
import io.github.flink.gcp.connector.bigquery.source.serializer.BigQueryRowDeserializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

final class BigQueryExamplesReadingView {

    private BigQueryExamplesReadingView() {}

    static void build(Schema readerSchema) {
        // tag::bigquery-examples-reading-view[]
        BigQuerySource.<GenericRecord>builder()
                .query("SELECT id, name FROM `my-project.my_dataset.active_accounts`")
                .parentProject("my-project")
                .deserializer(BigQueryRowDeserializer.genericRecord(readerSchema))
                .build();
        // end::bigquery-examples-reading-view[]
    }
}
