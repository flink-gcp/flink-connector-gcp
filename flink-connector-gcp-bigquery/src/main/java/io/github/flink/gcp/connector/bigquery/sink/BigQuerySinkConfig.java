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

package io.github.flink.gcp.connector.bigquery.sink;

import org.apache.flink.annotation.Internal;

import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import io.github.flink.gcp.connector.base.failure.FailureHandler;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.bigquery.sink.cdc.CdcOptions;
import io.github.flink.gcp.connector.bigquery.sink.cdc.CdcProtoRowFields;
import io.github.flink.gcp.connector.bigquery.sink.failure.FailedRow;
import io.github.flink.gcp.connector.bigquery.sink.serializer.AdditionalField;
import io.github.flink.gcp.connector.bigquery.sink.serializer.AdditionalFields;
import io.github.flink.gcp.connector.bigquery.sink.serializer.BigQueryProtoSerializer;
import io.github.flink.gcp.connector.bigquery.sink.serializer.ProtoRowAugmentationField;
import io.github.flink.gcp.connector.bigquery.sink.serializer.ProtoRowAugmentingSerializer;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Immutable configuration shared by all {@link WriteMethod} implementations, assembled by {@link
 * BigQuerySinkBuilder}.
 *
 * @param <T> type of the records written by the sink
 */
@Internal
public final class BigQuerySinkConfig<T> implements Serializable {

    private static final long serialVersionUID = 1L;

    private final DestinationResolver<? super T> destinationResolver;
    private final BigQueryProtoSerializer<? super T> serializer;
    @Nullable private final CdcOptions<? super T> cdcOptions;
    @Nullable private final ProtoRowAugmentingSerializer<T> rowAugmentingSerializer;
    private final CreateDisposition createDisposition;
    private final TableCreateOptionsProvider tableCreateOptionsProvider;
    private final SchemaUpdateOptions schemaUpdateOptions;
    private final FailureHandler<? super FailedRow> failedRowHandler;
    private final String location;
    @Nullable private final String serviceAccountKeyFile;
    @Nullable private final EmulatorEndpoint emulatorEndpoint;
    @Nullable private final EmulatorEndpoint emulatorRestEndpoint;

    BigQuerySinkConfig(
            DestinationResolver<? super T> destinationResolver,
            BigQueryProtoSerializer<? super T> serializer,
            @Nullable AdditionalFields<? super T> additionalFields,
            @Nullable CdcOptions<? super T> cdcOptions,
            CreateDisposition createDisposition,
            TableCreateOptionsProvider tableCreateOptionsProvider,
            SchemaUpdateOptions schemaUpdateOptions,
            FailureHandler<? super FailedRow> failedRowHandler,
            String location,
            @Nullable String serviceAccountKeyFile,
            @Nullable EmulatorEndpoint emulatorEndpoint,
            @Nullable EmulatorEndpoint emulatorRestEndpoint) {
        this.destinationResolver = destinationResolver;
        this.serializer = serializer;
        this.cdcOptions = cdcOptions;
        List<ProtoRowAugmentationField<? super T>> augmentationFields = new ArrayList<>();
        if (additionalFields != null) {
            for (AdditionalField<? super T> field : additionalFields.getFields()) {
                augmentationFields.add(ProtoRowAugmentationField.physical(field));
            }
        }
        if (cdcOptions != null) {
            augmentationFields.addAll(CdcProtoRowFields.create(cdcOptions));
        }
        boolean cdcOnly = additionalFields == null && cdcOptions != null;
        this.rowAugmentingSerializer =
                augmentationFields.isEmpty()
                        ? null
                        : new ProtoRowAugmentingSerializer<>(
                                serializer,
                                augmentationFields,
                                cdcOnly ? "BigQuery CDC pseudocolumn" : "additional BigQuery field",
                                cdcOnly
                                        ? "Failed to add BigQuery CDC metadata to a serialized row"
                                        : "Failed to add fields to a serialized BigQuery row");
        this.createDisposition = createDisposition;
        this.tableCreateOptionsProvider = tableCreateOptionsProvider;
        this.schemaUpdateOptions = schemaUpdateOptions;
        this.failedRowHandler = failedRowHandler;
        this.location = location;
        this.serviceAccountKeyFile = serviceAccountKeyFile;
        this.emulatorEndpoint = emulatorEndpoint;
        this.emulatorRestEndpoint = emulatorRestEndpoint;
    }

    /** Returns the per-record destination resolver. */
    public DestinationResolver<? super T> getDestinationResolver() {
        return destinationResolver;
    }

    /** Returns the record serializer. */
    public BigQueryProtoSerializer<? super T> getSerializer() {
        return serializer;
    }

    /** Returns the configured CDC options, or {@code null} when ordinary appends are used. */
    @Nullable
    public CdcOptions<? super T> getCdcOptions() {
        return cdcOptions;
    }

    /** Returns the physical table schema, including configured additional fields. */
    public TableSchema getTableSchema(TableDestination destination) {
        return rowAugmentingSerializer == null
                ? serializer.getTableSchema(destination)
                : rowAugmentingSerializer.getTableSchema(destination);
    }

    /** Returns the descriptor sent to a writer, including every additional or write-only field. */
    public Descriptors.Descriptor getWriteDescriptor(TableDestination destination) {
        return rowAugmentingSerializer == null
                ? serializer.getDescriptor(destination)
                : rowAugmentingSerializer.getDescriptor(destination);
    }

    /** Returns the delegate fingerprint; configured additional-field declarations are immutable. */
    public Object getSchemaFingerprint(TableDestination destination) {
        return rowAugmentingSerializer == null
                ? serializer.getSchemaFingerprint(destination)
                : rowAugmentingSerializer.getSchemaFingerprint(destination);
    }

    /** Validates the effective schema surfaces before entering per-row failure handling. */
    public void prepareWriteSchema(TableDestination destination) {
        if (rowAugmentingSerializer != null) {
            rowAugmentingSerializer.prepare(destination);
        }
    }

    /**
     * Serializes a row for its destination, including configured additional or write-only fields.
     */
    @Nullable
    public ByteString serialize(T element, TableDestination destination) throws IOException {
        return rowAugmentingSerializer == null
                ? serializer.serialize(element)
                : rowAugmentingSerializer.serialize(element, destination);
    }

    /** Returns the table create disposition. */
    public CreateDisposition getCreateDisposition() {
        return createDisposition;
    }

    /** Returns the per-destination creation options provider for auto-created tables. */
    public TableCreateOptionsProvider getTableCreateOptionsProvider() {
        return tableCreateOptionsProvider;
    }

    /** Returns the options gating connector-driven table schema updates. */
    public SchemaUpdateOptions getSchemaUpdateOptions() {
        return schemaUpdateOptions;
    }

    /** Returns the handler for rows that terminally fail to be written. */
    public FailureHandler<? super FailedRow> getFailedRowHandler() {
        return failedRowHandler;
    }

    /**
     * Returns the BigQuery location (for example {@code US} or {@code asia-northeast1}) of the
     * destination tables, or {@code null} when unset.
     */
    public String getLocation() {
        return location;
    }

    /**
     * Returns the service-account JSON key-file path, or {@code null} when clients use ADC.
     *
     * <p>Only the path is serialized into the job graph; runtime components load the file when they
     * create their clients.
     */
    @Nullable
    public String getServiceAccountKeyFile() {
        return serviceAccountKeyFile;
    }

    /**
     * Returns the emulator endpoint the Storage Write API connections use, or {@code null} when the
     * sink talks to the production service.
     */
    @Nullable
    public EmulatorEndpoint getEmulatorEndpoint() {
        return emulatorEndpoint;
    }

    /**
     * Returns the emulator endpoint the table metadata (REST) client uses, or {@code null} when it
     * talks to the production service. Separate from {@link #getEmulatorEndpoint()} because
     * BigQuery serves gRPC and REST on different ports.
     */
    @Nullable
    public EmulatorEndpoint getEmulatorRestEndpoint() {
        return emulatorRestEndpoint;
    }
}
