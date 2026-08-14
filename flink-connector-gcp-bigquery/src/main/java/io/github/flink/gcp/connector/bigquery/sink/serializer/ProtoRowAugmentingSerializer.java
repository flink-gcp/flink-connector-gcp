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

package io.github.flink.gcp.connector.bigquery.sink.serializer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

import com.google.cloud.bigquery.storage.v1.TableFieldSchema;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.serializer.ProtoRowAugmentationField.SchemaOwnership;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.stream.Collectors;

/** Adds ordered physical or write-only fields to delegate protobuf rows. */
@Internal
public final class ProtoRowAugmentingSerializer<T> implements Serializable {

    private static final long serialVersionUID = 1L;

    private final BigQueryProtoSerializer<? super T> delegate;
    private final List<ProtoRowAugmentationField<? super T>> fields;
    private final String descriptorFieldDescription;
    private final String rowFailureMessage;

    private transient Map<TableDestination, SchemaSurfaces> schemaCache;

    public ProtoRowAugmentingSerializer(
            BigQueryProtoSerializer<? super T> delegate,
            List<? extends ProtoRowAugmentationField<? super T>> fields,
            String descriptorFieldDescription,
            String rowFailureMessage) {
        this.delegate = Preconditions.checkNotNull(delegate, "delegate must not be null");
        Preconditions.checkArgument(
                !fields.isEmpty(), "At least one augmentation field is required");
        this.fields = Collections.unmodifiableList(new ArrayList<>(fields));
        this.descriptorFieldDescription =
                Preconditions.checkNotNull(
                        descriptorFieldDescription, "descriptorFieldDescription must not be null");
        this.rowFailureMessage =
                Preconditions.checkNotNull(rowFailureMessage, "rowFailureMessage must not be null");
    }

    /** Returns the delegate table schema plus physical additional fields in declaration order. */
    public TableSchema getTableSchema(TableDestination destination) {
        return schemaSurfaces(destination).tableSchema;
    }

    /** Returns the delegate descriptor plus every physical and write-only augmentation field. */
    public Descriptors.Descriptor getDescriptor(TableDestination destination) {
        return schemaSurfaces(destination).descriptor;
    }

    /** Returns the delegate schema fingerprint; augmentation declarations are immutable. */
    public Object getSchemaFingerprint(TableDestination destination) {
        return delegate.getSchemaFingerprint(destination);
    }

    /**
     * Derives both schema surfaces before row-failure handling so destination-wide conflicts fail
     * the job rather than being dropped once per record.
     */
    public void prepare(TableDestination destination) {
        schemaSurfaces(destination);
    }

    private TableSchema augmentedTableSchema(TableDestination destination) {
        TableSchema base = delegate.getTableSchema(destination);
        List<TableFieldSchema> physical =
                fields.stream()
                        .filter(field -> field.getSchemaOwnership() == SchemaOwnership.PHYSICAL)
                        .map(ProtoRowAugmentationField::getTableField)
                        .collect(Collectors.toList());
        if (physical.isEmpty()) {
            return base;
        }
        validateTableSchema(base, physical);
        return base.toBuilder().addAllFields(physical).build();
    }

    private Descriptors.Descriptor augmentedDescriptor(TableDestination destination) {
        return ProtoDescriptorAugmenter.augment(
                delegate.getDescriptor(destination),
                fields.stream()
                        .map(ProtoRowAugmentationField::getDescriptorField)
                        .collect(Collectors.toList()),
                descriptorFieldDescription);
    }

    /** Serializes one row, preserving the delegate's {@code null} means skip contract. */
    @Nullable
    public ByteString serialize(T element, TableDestination destination) throws IOException {
        ByteString serialized = delegate.serialize(element);
        if (serialized == null) {
            return null;
        }

        Descriptors.Descriptor descriptor = schemaSurfaces(destination).descriptor;
        DynamicMessage.Builder row = DynamicMessage.newBuilder(descriptor).mergeFrom(serialized);
        for (ProtoRowAugmentationField<? super T> field : fields) {
            Object value;
            try {
                value = field.getValueProvider().getValue(element);
            } catch (IOException e) {
                throw e;
            } catch (RuntimeException e) {
                throw new IOException(field.getProviderFailureMessage(), e);
            }
            if (value == null) {
                if (field.getNullPolicy() == AdditionalFieldNullPolicy.REQUIRED) {
                    throw new IOException(field.getNullValueMessage());
                }
                continue;
            }
            try {
                row.setField(
                        descriptor.findFieldByName(field.getDescriptorField().getName()), value);
            } catch (RuntimeException e) {
                throw new IOException(rowFailureMessage, e);
            }
        }
        try {
            // CDC deletes may intentionally omit physical REQUIRED fields. Every augmentation
            // field enforces its own required-value policy before reaching this point.
            return row.buildPartial().toByteString();
        } catch (RuntimeException e) {
            throw new IOException(rowFailureMessage, e);
        }
    }

    private static void validateTableSchema(
            TableSchema base, List<TableFieldSchema> physicalFields) {
        Set<String> names = new HashSet<>();
        for (TableFieldSchema field : base.getFieldsList()) {
            names.add(field.getName().toLowerCase(Locale.ROOT));
        }
        for (TableFieldSchema field : physicalFields) {
            if (!names.add(field.getName().toLowerCase(Locale.ROOT))) {
                throw new IllegalStateException(
                        "The physical BigQuery table schema must not declare additional field "
                                + field.getName());
            }
        }
    }

    private synchronized SchemaSurfaces schemaSurfaces(TableDestination destination) {
        Object fingerprint = delegate.getSchemaFingerprint(destination);
        SchemaSurfaces cached = cache().get(destination);
        if (cached != null && Objects.equals(cached.fingerprint, fingerprint)) {
            return cached;
        }
        SchemaSurfaces derived =
                new SchemaSurfaces(
                        fingerprint,
                        augmentedTableSchema(destination),
                        augmentedDescriptor(destination));
        cache().put(destination, derived);
        return derived;
    }

    private Map<TableDestination, SchemaSurfaces> cache() {
        Map<TableDestination, SchemaSurfaces> local = schemaCache;
        if (local == null) {
            local = new WeakHashMap<>();
            schemaCache = local;
        }
        return local;
    }

    private static final class SchemaSurfaces {
        private final Object fingerprint;
        private final TableSchema tableSchema;
        private final Descriptors.Descriptor descriptor;

        private SchemaSurfaces(
                Object fingerprint, TableSchema tableSchema, Descriptors.Descriptor descriptor) {
            this.fingerprint = fingerprint;
            this.tableSchema = tableSchema;
            this.descriptor = descriptor;
        }
    }
}
