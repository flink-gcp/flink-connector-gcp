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

package io.github.flink.gcp.connector.bigquery.table.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import com.google.cloud.bigquery.storage.v1.BQTableSchemaToProtoDescriptor;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.serializer.BigQueryProtoSerializer;

import java.io.IOException;

/**
 * Serializes the {@code RowData} of a SQL table into BigQuery protobuf rows.
 *
 * <p>The serializable state is the table's {@code RowType} and its schema options; the {@code
 * TableSchema}, the descriptor and the row converter are derived from them. Derivation happens
 * <b>eagerly in the constructor</b> — the rule this module follows everywhere — so a column type
 * BigQuery cannot hold fails while the job graph is built, rather than from {@code serialize()}
 * inside the writers' failure handler, where a dropping policy would swallow one misconfiguration
 * once per record and leave the table empty under a green job. The derived state is also {@code
 * transient}, so a task manager rebuilds it after deserialization.
 *
 * <p>{@code @Internal} and not part of the public serializer family: promoting it is cheap if a
 * DataStream-with-{@code RowData} need appears, and starting internal keeps the new Flink-type
 * mapping out of the published API surface until it has settled.
 *
 * <p>This serializer never returns {@code null}, so the SPI's skip contract is simply never
 * exercised: the converter throws on a value it cannot map, and SQL has no way to express a filter
 * at this layer anyway.
 */
@Internal
public final class RowDataSerializer extends BigQueryProtoSerializer<RowData> {

    private static final long serialVersionUID = 1L;

    private final RowType rowType;
    private final RowDataSchemaOptions options;

    private transient volatile ConversionState state;

    /**
     * Creates the serializer.
     *
     * @param rowType the physical columns of the SQL table
     * @param options the schema mapping options
     * @throws IllegalArgumentException if a column has no BigQuery equivalent
     */
    public RowDataSerializer(RowType rowType, RowDataSchemaOptions options) {
        this.rowType = Preconditions.checkNotNull(rowType, "rowType must not be null");
        this.options = Preconditions.checkNotNull(options, "options must not be null");
        // Eagerly, so the failure lands on the client rather than on a task manager.
        state();
    }

    @Override
    public TableSchema getTableSchema(TableDestination destination) {
        return state().tableSchema;
    }

    @Override
    public Descriptors.Descriptor getDescriptor(TableDestination destination) {
        return state().rowDescriptor;
    }

    @Override
    public ByteString serialize(RowData element) throws IOException {
        return state().rowConverter.convert(element).toByteString();
    }

    private ConversionState state() {
        ConversionState localState = state;
        if (localState == null) {
            localState = initialize();
        }
        return localState;
    }

    private synchronized ConversionState initialize() {
        ConversionState localState = state;
        if (localState != null) {
            return localState;
        }
        TableSchema tableSchema = RowTypeToTableSchemaConverter.convert(rowType, options);
        Descriptors.Descriptor rowDescriptor;
        try {
            rowDescriptor =
                    BQTableSchemaToProtoDescriptor.convertBQTableSchemaToProtoDescriptor(
                            tableSchema);
        } catch (Descriptors.DescriptorValidationException e) {
            throw new IllegalStateException(
                    "Failed to derive a BigQuery-storage compatible descriptor for the table's"
                            + " columns",
                    e);
        }
        localState =
                new ConversionState(
                        tableSchema,
                        rowDescriptor,
                        new RowDataToProtoConverter(rowType, tableSchema, rowDescriptor));
        state = localState;
        return localState;
    }

    /** The derived triple, published through one volatile read on the per-record path. */
    private static final class ConversionState {

        private final TableSchema tableSchema;
        private final Descriptors.Descriptor rowDescriptor;
        private final RowDataToProtoConverter rowConverter;

        ConversionState(
                TableSchema tableSchema,
                Descriptors.Descriptor rowDescriptor,
                RowDataToProtoConverter rowConverter) {
            this.tableSchema = tableSchema;
            this.rowDescriptor = rowDescriptor;
            this.rowConverter = rowConverter;
        }
    }
}
