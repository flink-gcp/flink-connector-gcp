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

package io.github.flink.gcp.connector.bigquery.sink.cdc;

import org.apache.flink.annotation.Internal;

import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.serializer.BigQueryProtoSerializer;
import io.github.flink.gcp.connector.bigquery.sink.serializer.ProtoDescriptorAugmenter;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.regex.Pattern;

/**
 * Serializes delegate rows with BigQuery CDC pseudocolumns in the write descriptor and row bytes.
 */
@Internal
public final class CdcProtoRowSerializer<T> implements Serializable {

    private static final long serialVersionUID = 1L;

    static final String CHANGE_TYPE_FIELD = "_change_type";
    static final String SEQUENCE_NUMBER_FIELD = "_change_sequence_number";

    private static final Pattern SEQUENCE_PATTERN =
            Pattern.compile("[0-9A-Fa-f]{1,16}(?:/[0-9A-Fa-f]{1,16}){0,3}");

    private final BigQueryProtoSerializer<? super T> delegate;
    private final CdcOptions<? super T> options;

    private transient Map<Descriptors.Descriptor, Descriptors.Descriptor> descriptorCache;

    public CdcProtoRowSerializer(
            BigQueryProtoSerializer<? super T> delegate, CdcOptions<? super T> options) {
        this.delegate = delegate;
        this.options = options;
    }

    /** Returns the physical table schema's descriptor augmented only for the write stream. */
    public Descriptors.Descriptor getDescriptor(TableDestination destination) {
        Descriptors.Descriptor base = delegate.getDescriptor(destination);
        synchronized (this) {
            return cache().computeIfAbsent(base, this::augmentDescriptor);
        }
    }

    /** Serializes one row, preserving the delegate's {@code null} means skip contract. */
    @Nullable
    public ByteString serialize(T element, TableDestination destination) throws IOException {
        ByteString serialized = delegate.serialize(element);
        if (serialized == null) {
            return null;
        }

        CdcChangeType changeType;
        try {
            changeType = options.getChangeTypeProvider().getChangeType(element);
        } catch (RuntimeException e) {
            throw new IOException("The CDC change type provider failed", e);
        }
        if (changeType == null) {
            throw new IOException("The CDC change type provider returned null");
        }

        String sequenceNumber = null;
        CdcSequenceNumberProvider<? super T> sequenceProvider = options.getSequenceNumberProvider();
        if (sequenceProvider != null) {
            try {
                sequenceNumber = normalizeSequence(sequenceProvider.getSequenceNumber(element));
            } catch (RuntimeException e) {
                throw new IOException("The CDC sequence number provider failed", e);
            }
        }

        Descriptors.Descriptor descriptor = getDescriptor(destination);
        try {
            DynamicMessage.Builder row =
                    DynamicMessage.newBuilder(descriptor).mergeFrom(serialized);
            row.setField(descriptor.findFieldByName(CHANGE_TYPE_FIELD), changeType.name());
            if (sequenceNumber != null) {
                row.setField(descriptor.findFieldByName(SEQUENCE_NUMBER_FIELD), sequenceNumber);
            }
            return row.build().toByteString();
        } catch (RuntimeException e) {
            throw new IOException("Failed to add BigQuery CDC metadata to a serialized row", e);
        }
    }

    private static String normalizeSequence(String sequenceNumber) throws IOException {
        if (sequenceNumber == null) {
            throw new IOException("The CDC sequence number provider returned null");
        }
        if (!SEQUENCE_PATTERN.matcher(sequenceNumber).matches()) {
            throw new IOException(
                    "A BigQuery CDC sequence number must contain one to four slash-separated"
                            + " hexadecimal sections of at most 16 digits each");
        }
        return sequenceNumber.toUpperCase(Locale.ROOT);
    }

    private Map<Descriptors.Descriptor, Descriptors.Descriptor> cache() {
        Map<Descriptors.Descriptor, Descriptors.Descriptor> local = descriptorCache;
        if (local == null) {
            local = Collections.synchronizedMap(new WeakHashMap<>());
            descriptorCache = local;
        }
        return local;
    }

    private Descriptors.Descriptor augmentDescriptor(Descriptors.Descriptor base) {
        List<FieldDescriptorProto> fields = new ArrayList<>();
        fields.add(stringField(CHANGE_TYPE_FIELD));
        if (options.hasSequenceNumberProvider()) {
            fields.add(stringField(SEQUENCE_NUMBER_FIELD));
        }
        return ProtoDescriptorAugmenter.augment(base, fields, "BigQuery CDC pseudocolumn");
    }

    private static FieldDescriptorProto stringField(String name) {
        return FieldDescriptorProto.newBuilder()
                .setName(name)
                .setType(FieldDescriptorProto.Type.TYPE_STRING)
                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)
                .build();
    }
}
