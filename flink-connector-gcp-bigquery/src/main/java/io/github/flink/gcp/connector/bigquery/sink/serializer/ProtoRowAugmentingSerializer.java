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
import java.util.HashMap;
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

    private final BigQueryProtoSerializationSchema<? super T> delegate;
    private final List<ProtoRowAugmentationField<? super T>> fields;
    private final String descriptorFieldDescription;
    private final String rowFailureMessage;

    private transient Map<TableDestination, SchemaSurfaces> schemaCache;

    /** Each evolving destination's augmentation field numbers, in declaration order. */
    private transient Map<String, int[]> appendedNumbers;

    public ProtoRowAugmentingSerializer(
            BigQueryProtoSerializationSchema<? super T> delegate,
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
     * Derives the destination's schema surfaces before row-failure handling so destination-wide
     * conflicts fail the job rather than being dropped once per record.
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

        SchemaSurfaces surfaces = schemaSurfaces(destination);
        DynamicMessage.Builder row =
                DynamicMessage.newBuilder(surfaces.descriptor).mergeFrom(serialized);
        for (int i = 0; i < fields.size(); i++) {
            ProtoRowAugmentationField<? super T> field = fields.get(i);
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
                row.setField(surfaces.augmentationFields.get(i), value);
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
        // The table schema first: a name a destination already carries is reported as the column
        // collision it is, rather than as the descriptor collision it also causes.
        TableSchema augmentedTableSchema = augmentedTableSchema(destination);
        Descriptors.Descriptor descriptor = augmentedDescriptor(destination);
        List<Descriptors.FieldDescriptor> augmentationFields = augmentationFields(descriptor);
        if (fingerprint != null) {
            checkNumbersKept(destination, augmentationFields);
        }
        SchemaSurfaces derived =
                new SchemaSurfaces(
                        fingerprint, augmentedTableSchema, descriptor, augmentationFields);
        cache().put(destination, derived);
        return derived;
    }

    /**
     * Resolves each augmentation field against the augmented descriptor, in declaration order, so
     * that the record path sets fields by descriptor rather than by name.
     */
    private List<Descriptors.FieldDescriptor> augmentationFields(
            Descriptors.Descriptor descriptor) {
        List<Descriptors.FieldDescriptor> resolved = new ArrayList<>(fields.size());
        for (ProtoRowAugmentationField<? super T> field : fields) {
            String name = field.getDescriptorField().getName();
            Descriptors.FieldDescriptor resolvedField = descriptor.findFieldByName(name);
            // ProtoDescriptorAugmenter appends exactly these names, having rejected a collision
            // with an existing one, so a miss would mean the augmenter broke its own contract.
            Preconditions.checkState(
                    resolvedField != null, "The augmented row descriptor has no field %s", name);
            resolved.add(resolvedField);
        }
        return Collections.unmodifiableList(resolved);
    }

    /**
     * Fails an evolution that moved an augmentation field to another number. The writers append
     * rows serialized before a schema change through the stream writer rebuilt for it, without
     * re-encoding them, so a moved field would be read as whatever the evolved descriptor numbers
     * there. The numbers move only when the evolved base descriptor claims or frees a number in the
     * region the augmentation fields count down through, which {@link
     * ProtoDescriptorAugmenter#augment(Descriptors.Descriptor, List, String)} keeps out of reach of
     * a base that appends its fields after its highest number.
     *
     * <p>The numbers a destination was first given are kept strongly by table path, apart from the
     * weakly keyed schema cache: that cache's key is whichever equal destination object derived the
     * entry first, which a writer retaining rows need not hold, so the entry can be collected while
     * rows remain. Only serializers that report a fingerprint can evolve, so only they keep the
     * numbers.
     */
    private void checkNumbersKept(
            TableDestination destination, List<Descriptors.FieldDescriptor> current) {
        int[] numbers = new int[current.size()];
        for (int i = 0; i < numbers.length; i++) {
            numbers[i] = current.get(i).getNumber();
        }
        // Keyed by the table path, which no component's '/' can make ambiguous, rather than by
        // the caller's object: holding that would also keep the schema cache's weak key, and with
        // it every descriptor derived for it, alive for as long as this map lives.
        int[] previous = appendedNumbers().putIfAbsent(destination.toTablePath(), numbers);
        if (previous == null) {
            return;
        }
        for (int i = 0; i < numbers.length; i++) {
            if (previous[i] != numbers[i]) {
                throw new IllegalStateException(
                        String.format(
                                "The evolved row descriptor for %s moves %s %s from field number"
                                        + " %d to %d, which would misread rows serialized before"
                                        + " the change; the evolved descriptor must leave field"
                                        + " numbers %d and above as they were",
                                destination,
                                descriptorFieldDescription,
                                current.get(i).getName(),
                                previous[i],
                                numbers[i],
                                Math.min(previous[i], numbers[i])));
            }
        }
    }

    private Map<String, int[]> appendedNumbers() {
        Map<String, int[]> local = appendedNumbers;
        if (local == null) {
            local = new HashMap<>();
            appendedNumbers = local;
        }
        return local;
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
        private final List<Descriptors.FieldDescriptor> augmentationFields;

        private SchemaSurfaces(
                Object fingerprint,
                TableSchema tableSchema,
                Descriptors.Descriptor descriptor,
                List<Descriptors.FieldDescriptor> augmentationFields) {
            this.fingerprint = fingerprint;
            this.tableSchema = tableSchema;
            this.descriptor = descriptor;
            this.augmentationFields = augmentationFields;
        }
    }
}
