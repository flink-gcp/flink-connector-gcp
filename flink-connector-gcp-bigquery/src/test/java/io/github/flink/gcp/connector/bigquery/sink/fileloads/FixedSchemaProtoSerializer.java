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

package io.github.flink.gcp.connector.bigquery.sink.fileloads;

import com.google.protobuf.Descriptors;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.serializer.BigQueryProtoSerializationSchema;

/**
 * Base for the FILE_LOADS ITCase serializers: one schema for every destination, and a row
 * descriptor derived once. Subclasses supply {@link #getTableSchema} and {@link #serialize} only.
 *
 * <p>The descriptor is the whole point. {@link BigQueryProtoSerializationSchema#getDescriptor}
 * already derives one from the table schema and its javadoc asks implementations to override it
 * with a cached one — which is all this does, so nothing here repeats the conversion or its checked
 * exception.
 *
 * <p>The schema arrives as an overridden method rather than a constructor argument so that nothing
 * new is captured when the sink is Java-serialized into the job graph: subclasses return their own
 * {@code static final} constant, and the cached descriptor is {@code transient}.
 */
abstract class FixedSchemaProtoSerializer<T> extends BigQueryProtoSerializationSchema<T> {

    private static final long serialVersionUID = 1L;

    private transient Descriptors.Descriptor descriptor;

    @Override
    public final Descriptors.Descriptor getDescriptor(TableDestination destination) {
        if (descriptor == null) {
            descriptor = super.getDescriptor(destination);
        }
        return descriptor;
    }

    /**
     * The row descriptor. No destination, because the schema does not depend on one — which is also
     * why a derivation failure would name {@code null} as the destination it was deriving for.
     */
    final Descriptors.Descriptor descriptor() {
        return getDescriptor(null);
    }

    /** The row descriptor's field of that name. */
    final Descriptors.FieldDescriptor field(String name) {
        return descriptor().findFieldByName(name);
    }
}
