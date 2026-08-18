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

import org.apache.flink.annotation.Internal;

import com.google.cloud.bigquery.storage.v1.BQTableSchemaToProtoDescriptor;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.protobuf.Descriptors;

import java.util.Locale;

/** Derives the protobuf row descriptor a BigQuery table schema is written through. */
@Internal
public final class RowDescriptors {

    private RowDescriptors() {}

    /**
     * Returns the BigQuery-storage compatible row descriptor of the given table schema.
     *
     * <p>Everything the client library refuses to map is still reported as it reports it: a {@code
     * RANGE} column with no element type, for instance, is an {@link IllegalArgumentException}
     * thrown from the call below and passed through unwrapped. The checked {@link
     * com.google.protobuf.Descriptors.DescriptorValidationException DescriptorValidationException}
     * is what remains: a schema <em>handed to</em> this connector reaches it, since the generated
     * descriptor's field names are lower-cased and two columns differing only in case collide
     * there, while a schema this connector derives is checked for exactly that first — each schema
     * converter rejects such names under {@link Locale#ROOT}, which forecloses the collision for
     * every ASCII name, and ASCII is every name the Avro and protobuf front ends allow. It is
     * wrapped rather than declared because a schema is not a record: each serializer derives once
     * in its constructor, where this fails on the client, and a checked exception would oblige the
     * paths that derive again on a task manager to treat a configuration error as a bad row.
     *
     * @param tableSchema the destination table schema
     * @param schemaDescription how the schema is named in the failure message
     * @return the row descriptor
     */
    public static Descriptors.Descriptor derive(TableSchema tableSchema, String schemaDescription) {
        try {
            return BQTableSchemaToProtoDescriptor.convertBQTableSchemaToProtoDescriptor(
                    tableSchema);
        } catch (Descriptors.DescriptorValidationException e) {
            throw new IllegalStateException(
                    "Failed to derive a BigQuery-storage compatible descriptor for "
                            + schemaDescription,
                    e);
        }
    }
}
