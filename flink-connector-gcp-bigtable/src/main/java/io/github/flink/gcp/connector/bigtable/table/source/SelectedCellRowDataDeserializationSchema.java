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

package io.github.flink.gcp.connector.bigtable.table.source;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import io.github.flink.gcp.connector.base.source.SynchronousDeserializationCollector;
import io.github.flink.gcp.connector.bigtable.RowRanges;
import io.github.flink.gcp.connector.bigtable.source.changestream.BigtableChangeStreamMutation;
import io.github.flink.gcp.connector.bigtable.source.serializer.BigtableChangeStreamDeserializationSchema;
import io.github.flink.gcp.connector.bigtable.table.CellValueCodec;
import io.github.flink.gcp.connector.bigtable.table.SelectedCellTableSchema;
import io.github.flink.gcp.connector.bigtable.table.TrailingBytes;

import java.io.IOException;

/** Decodes one selected cell into a keyed table upsert or key-only delete. */
@Internal
final class SelectedCellRowDataDeserializationSchema
        implements BigtableChangeStreamDeserializationSchema<RowData> {

    private static final long serialVersionUID = 1L;

    private final DeserializationSchema<RowData> payloadDeserializer;
    private final SelectedCellMutationClassifier classifier;
    private final CellValueCodec.FieldDecoder primaryKeyDecoder;
    private final int primaryKeyIndex;
    private final RowData.FieldGetter[] payloadGetters;
    private final ChangeStreamReadableMetadata[] metadata;
    private final TypeInformation<RowData> producedType;

    SelectedCellRowDataDeserializationSchema(
            DeserializationSchema<RowData> payloadDeserializer,
            SelectedCellMutationClassifier classifier,
            SelectedCellTableSchema schema,
            TrailingBytes trailingBytes,
            ChangeStreamReadableMetadata[] metadata,
            TypeInformation<RowData> producedType) {
        this.payloadDeserializer =
                Preconditions.checkNotNull(
                        payloadDeserializer, "payloadDeserializer must not be null");
        this.classifier = Preconditions.checkNotNull(classifier, "classifier must not be null");
        this.primaryKeyDecoder = CellValueCodec.decoder(schema.getPrimaryKeyType(), trailingBytes);
        this.primaryKeyIndex = schema.getPrimaryKeyIndex();
        RowType payloadType = (RowType) schema.getPayloadDataType().getLogicalType();
        this.payloadGetters = new RowData.FieldGetter[payloadType.getFieldCount()];
        for (int index = 0; index < payloadGetters.length; index++) {
            payloadGetters[index] = RowData.createFieldGetter(payloadType.getTypeAt(index), index);
        }
        this.metadata = Preconditions.checkNotNull(metadata, "metadata must not be null").clone();
        this.producedType =
                Preconditions.checkNotNull(producedType, "producedType must not be null");
    }

    @Override
    public void open(DeserializationSchema.InitializationContext context) throws Exception {
        payloadDeserializer.open(context);
    }

    @Override
    public void deserialize(BigtableChangeStreamMutation mutation, Collector<RowData> out)
            throws IOException {
        SelectedCellMutationClassifier.Result result = classifier.classify(mutation);
        if (result.getKind() == SelectedCellMutationClassifier.Kind.UNRELATED) {
            return;
        }

        RowKind rowKind =
                result.getKind() == SelectedCellMutationClassifier.Kind.UPSERT
                        ? RowKind.UPDATE_AFTER
                        : RowKind.DELETE;
        GenericRowData physical = new GenericRowData(rowKind, payloadGetters.length + 1);
        byte[] key = mutation.getRowKey().toByteArray();
        try {
            physical.setField(primaryKeyIndex, primaryKeyDecoder.decode(key));
        } catch (RuntimeException e) {
            // The guard RowToRowDataConverter's row-key decode carries, for the same reason: a row
            // key is as externally written as a cell, and a fixed-width decoder reading a shorter
            // key otherwise throws a bare ArrayIndexOutOfBoundsException naming nothing. The key is
            // escaped rather than decoded as UTF-8, since the question the message asks is whether
            // the row was written under a different encoding.
            throw new IOException(
                    String.format(
                            "The row key of the row '%s' holds %d byte(s), which the declared"
                                    + " primary-key column type cannot decode. Was the row written"
                                    + " under a different encoding?",
                            RowRanges.format(mutation.getRowKey()), key.length),
                    e);
        }
        if (result.getKind() == SelectedCellMutationClassifier.Kind.UPSERT) {
            RowData[] payload = new RowData[1];
            long emittedCount =
                    SynchronousDeserializationCollector.<RowData, IOException>deserialize(
                            row -> payload[0] = row,
                            collector ->
                                    payloadDeserializer.deserialize(
                                            Preconditions.checkNotNull(
                                                            result.getValue(),
                                                            "an upsert must carry a selected-cell"
                                                                    + " value")
                                                    .toByteArray(),
                                            collector));
            if (emittedCount != 1) {
                throw new IOException(
                        "The selected-cell value format must emit exactly one non-null row, but"
                                + " emitted "
                                + emittedCount
                                + " rows.");
            }
            if (payload[0].getRowKind() != RowKind.INSERT) {
                throw new IOException(
                        "The selected-cell value format declared an insert-only changelog but"
                                + " emitted row kind "
                                + payload[0].getRowKind()
                                + ".");
            }
            for (int payloadIndex = 0; payloadIndex < payloadGetters.length; payloadIndex++) {
                int physicalIndex =
                        payloadIndex < primaryKeyIndex ? payloadIndex : payloadIndex + 1;
                physical.setField(
                        physicalIndex, payloadGetters[payloadIndex].getFieldOrNull(payload[0]));
            }
        }

        if (metadata.length == 0) {
            out.collect(physical);
            return;
        }
        GenericRowData metadataRow = new GenericRowData(metadata.length);
        for (int index = 0; index < metadata.length; index++) {
            metadataRow.setField(index, metadata[index].read(mutation));
        }
        out.collect(new JoinedRowData(rowKind, physical, metadataRow));
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return producedType;
    }
}
