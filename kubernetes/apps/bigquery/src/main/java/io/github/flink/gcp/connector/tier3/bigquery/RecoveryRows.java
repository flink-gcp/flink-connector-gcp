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

package io.github.flink.gcp.connector.tier3.bigquery;

import org.apache.flink.annotation.Internal;

import com.google.cloud.bigquery.storage.v1.TableFieldSchema;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.protobuf.ByteString;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.Descriptors;
import com.google.protobuf.UnsafeByteOperations;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.serializer.BigQueryProtoSerializationSchema;
import io.github.flink.gcp.connector.bigquery.sink.serializer.RowDescriptors;

import java.io.IOException;
import java.util.List;
import java.util.Random;

/** Deterministic, fixed-size protobuf rows shared by every destination. */
@Internal
final class RecoveryRows extends BigQueryProtoSerializationSchema<Long> {
    private static final long serialVersionUID = 1L;
    private static final TableSchema TABLE_SCHEMA =
            TableSchema.newBuilder()
                    .addAllFields(
                            List.of(
                                    field("run_id", TableFieldSchema.Type.STRING),
                                    field("sequence", TableFieldSchema.Type.INT64),
                                    field("destination", TableFieldSchema.Type.INT64),
                                    field("payload", TableFieldSchema.Type.BYTES)))
                    .build();
    private static final Descriptors.Descriptor DESCRIPTOR =
            RowDescriptors.derive(TABLE_SCHEMA, "BigQuery recovery row");
    private final RecoveryOptions options;

    RecoveryRows(RecoveryOptions options) {
        this.options = options;
    }

    @Override
    public TableSchema getTableSchema(TableDestination destination) {
        return TABLE_SCHEMA;
    }

    private static TableFieldSchema field(String name, TableFieldSchema.Type type) {
        return TableFieldSchema.newBuilder()
                .setName(name)
                .setType(type)
                .setMode(TableFieldSchema.Mode.NULLABLE)
                .build();
    }

    @Override
    public Descriptors.Descriptor getDescriptor(TableDestination destination) {
        return DESCRIPTOR;
    }

    @Override
    public ByteString serialize(Long sequence) throws IOException {
        int route = options.destination(sequence);
        int header =
                CodedOutputStream.computeStringSize(1, options.runId)
                        + CodedOutputStream.computeInt64Size(2, sequence)
                        + CodedOutputStream.computeInt64Size(3, route);
        int padding = options.mode.rowBytes - header - 1;
        while (header + 1 + CodedOutputStream.computeUInt32SizeNoTag(padding) + padding
                > options.mode.rowBytes) {
            padding--;
        }
        byte[] payload = new byte[padding];
        new Random(sequence ^ ((long) options.runId.hashCode() << 32)).nextBytes(payload);
        byte[] row = new byte[options.mode.rowBytes];
        CodedOutputStream output = CodedOutputStream.newInstance(row);
        output.writeString(1, options.runId);
        output.writeInt64(2, sequence);
        output.writeInt64(3, route);
        output.writeByteArray(4, payload);
        output.checkNoSpaceLeft();
        // This fresh array is never retained or mutated after transfer.
        return UnsafeByteOperations.unsafeWrap(row);
    }
}
