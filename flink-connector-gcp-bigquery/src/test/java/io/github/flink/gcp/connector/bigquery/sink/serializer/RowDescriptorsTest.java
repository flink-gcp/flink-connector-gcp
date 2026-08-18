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

package io.github.flink.gcp.connector.bigquery.sink.serializer;

import com.google.cloud.bigquery.storage.v1.TableFieldSchema;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.protobuf.Descriptors;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link RowDescriptors}. */
class RowDescriptorsTest {

    private static TableFieldSchema field(String name, TableFieldSchema.Type type) {
        return TableFieldSchema.newBuilder()
                .setName(name)
                .setType(type)
                .setMode(TableFieldSchema.Mode.NULLABLE)
                .build();
    }

    @Test
    void derivesARowDescriptorCarryingTheSchemasColumns() {
        Descriptors.Descriptor descriptor =
                RowDescriptors.derive(
                        TableSchema.newBuilder()
                                .addFields(field("name", TableFieldSchema.Type.STRING))
                                .addFields(field("n", TableFieldSchema.Type.INT64))
                                .build(),
                        "the supplied schema");

        assertThat(descriptor.getFields())
                .extracting(Descriptors.FieldDescriptor::getName)
                .containsExactly("name", "n");
    }

    @Test
    void namesWhatTheDescriptorWasDerivedFromWhenValidationFails() {
        // Two columns that differ only in case: BigQuery has no such table, but a schema handed to
        // the JSON serializer is not read back from one, and the descriptor the library generates
        // from it holds the same field name twice. Neither name carries an "i", because the
        // library lower-cases with the *default* locale, under which Turkish "ID" is "ıd" and no
        // longer collides (ADR-0024 records the same trap on the Avro side).
        TableSchema ambiguous =
                TableSchema.newBuilder()
                        .addFields(field("count", TableFieldSchema.Type.INT64))
                        .addFields(field("COUNT", TableFieldSchema.Type.INT64))
                        .build();

        assertThatThrownBy(() -> RowDescriptors.derive(ambiguous, "com.example.Record"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage(
                        "Failed to derive a BigQuery-storage compatible descriptor for"
                                + " com.example.Record")
                .cause()
                .isInstanceOf(Descriptors.DescriptorValidationException.class);
    }

    @Test
    void leavesWhatTheLibraryRefusesToMapAsItReportsIt() {
        // A RANGE column without its element type. The library maps RANGE itself, so this is a
        // schema it rejects rather than a type it cannot express — which is the point: whatever
        // it rejects, it rejects with an unchecked exception of its own, and that one is passed
        // through rather than relabelled.
        TableSchema unmappable =
                TableSchema.newBuilder()
                        .addFields(field("window", TableFieldSchema.Type.RANGE))
                        .build();

        assertThatThrownBy(() -> RowDescriptors.derive(unmappable, "com.example.Record"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("RANGE requires range element type");
    }
}
