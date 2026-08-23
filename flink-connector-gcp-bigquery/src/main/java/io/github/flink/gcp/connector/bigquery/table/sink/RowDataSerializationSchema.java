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

package io.github.flink.gcp.connector.bigquery.table.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Preconditions;

import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.serializer.BigQueryProtoSerializationSchema;
import io.github.flink.gcp.connector.bigquery.sink.serializer.LazyDerivedState;
import io.github.flink.gcp.connector.bigquery.sink.serializer.RowDescriptors;

import javax.annotation.Nullable;

import java.io.IOException;

/**
 * Serializes the {@code RowData} of a SQL table into BigQuery protobuf rows.
 *
 * <p>The serializable state is the table's {@code RowType}, its schema options, and the physical
 * indexes of any CDC primary key; the {@code TableSchema}, descriptor, and row converters are
 * derived from them. Derivation happens <b>eagerly in the constructor</b> — the rule this module
 * follows everywhere — so a column type BigQuery cannot hold fails while the job graph is built,
 * rather than from {@code serialize()} inside the writers' failure handler, where a dropping policy
 * would swallow one misconfiguration once per record and leave the table empty under a green job.
 * The derived state is also {@code transient}, so a task manager rebuilds it after deserialization.
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
final class RowDataSerializationSchema extends BigQueryProtoSerializationSchema<RowData> {

    private static final long serialVersionUID = 1L;

    private final RowType rowType;
    private final RowDataSchemaOptions options;
    private final int[] primaryKeyIndexes;

    private final LazyDerivedState<ConversionState> conversionState = new LazyDerivedState<>();

    /**
     * Creates the serializer.
     *
     * @param rowType the physical columns of the SQL table
     * @param options the schema mapping options
     * @throws IllegalArgumentException if a column has no BigQuery equivalent
     */
    public RowDataSerializationSchema(RowType rowType, RowDataSchemaOptions options) {
        this(rowType, options, new int[0]);
    }

    /** Creates a serializer whose CDC deletes contain only the selected physical key columns. */
    public RowDataSerializationSchema(
            RowType rowType, RowDataSchemaOptions options, int[] primaryKeyIndexes) {
        this.rowType = Preconditions.checkNotNull(rowType, "rowType must not be null");
        this.options = Preconditions.checkNotNull(options, "options must not be null");
        this.primaryKeyIndexes =
                Preconditions.checkNotNull(primaryKeyIndexes, "primaryKeyIndexes must not be null")
                        .clone();
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
        RowKind kind = element.getRowKind();
        if (kind == RowKind.UPDATE_BEFORE) {
            throw new IOException("UPDATE_BEFORE is not part of the BigQuery CDC sink changelog");
        }
        ConversionState conversion = state();
        if (kind == RowKind.DELETE && conversion.deleteRowConverter != null) {
            return conversion.deleteRowConverter.convertPartial(element).toByteString();
        }
        return conversion.rowConverter.convert(element).toByteString();
    }

    private ConversionState state() {
        return conversionState.get(this, RowDataSerializationSchema::deriveConversionState);
    }

    private ConversionState deriveConversionState() {
        TableSchema tableSchema = RowTypeToTableSchemaConverter.convert(rowType, options);
        Descriptors.Descriptor rowDescriptor =
                RowDescriptors.derive(tableSchema, "the table's columns");
        RowDataToProtoConverter deleteRowConverter =
                primaryKeyIndexes.length == 0
                        ? null
                        : new RowDataToProtoConverter(
                                rowType, tableSchema, rowDescriptor, primaryKeyIndexes);
        return new ConversionState(
                tableSchema,
                rowDescriptor,
                new RowDataToProtoConverter(rowType, tableSchema, rowDescriptor),
                deleteRowConverter);
    }

    /** The derived conversion state {@link LazyDerivedState} holds for this serializer. */
    private static final class ConversionState {

        private final TableSchema tableSchema;
        private final Descriptors.Descriptor rowDescriptor;
        private final RowDataToProtoConverter rowConverter;
        @Nullable private final RowDataToProtoConverter deleteRowConverter;

        ConversionState(
                TableSchema tableSchema,
                Descriptors.Descriptor rowDescriptor,
                RowDataToProtoConverter rowConverter,
                @Nullable RowDataToProtoConverter deleteRowConverter) {
            this.tableSchema = tableSchema;
            this.rowDescriptor = rowDescriptor;
            this.rowConverter = rowConverter;
            this.deleteRowConverter = deleteRowConverter;
        }
    }
}
