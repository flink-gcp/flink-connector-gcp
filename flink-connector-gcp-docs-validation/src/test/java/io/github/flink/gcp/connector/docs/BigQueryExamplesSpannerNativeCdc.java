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

package io.github.flink.gcp.connector.docs;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import io.github.flink.gcp.connector.base.source.StartPosition;
import io.github.flink.gcp.connector.bigquery.sink.BigQuerySink;
import io.github.flink.gcp.connector.bigquery.sink.CdcTableOptions;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.WriteMethod;
import io.github.flink.gcp.connector.bigquery.sink.cdc.CdcChangeType;
import io.github.flink.gcp.connector.bigquery.sink.cdc.CdcOptions;
import io.github.flink.gcp.connector.bigquery.sink.cdc.SpannerCdcSequenceNumber;
import io.github.flink.gcp.connector.bigquery.sink.serializer.BigQueryProtoSerializer;
import io.github.flink.gcp.connector.bigquery.sink.serializer.json.JsonDocumentSerializer;
import io.github.flink.gcp.connector.spanner.DatabaseDestination;
import io.github.flink.gcp.connector.spanner.source.SpannerChangeStreamSource;
import io.github.flink.gcp.connector.spanner.source.changestream.DataChangeRecord;
import io.github.flink.gcp.connector.spanner.source.changestream.Mod;
import io.github.flink.gcp.connector.spanner.source.changestream.ModType;
import io.github.flink.gcp.connector.spanner.source.serializer.SpannerChangeStreamDeserializationSchema;

import java.io.IOException;
import java.io.Serializable;
import java.time.Instant;
import java.util.Collections;
import java.util.List;

final class BigQueryExamplesSpannerNativeCdc {

    private BigQueryExamplesSpannerNativeCdc() {}

    static void runDataStream(StreamExecutionEnvironment env, TableSchema rowSchema)
            throws Exception {
        // tag::bigquery-spanner-native-cdc-datastream[]
        env.enableCheckpointing(60_000);
        SpannerChangeStreamSource<OrderMod> source =
                SpannerChangeStreamSource.<OrderMod>builder()
                        .database(DatabaseDestination.of("my-project", "my-instance", "orders-db"))
                        .changeStreamName("order_changes")
                        .deserializer(new OrderModDeserializer())
                        .startPosition(StartPosition.latest())
                        .build();

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "spanner-order-changes")
                .sinkTo(
                        BigQuerySink.<OrderMod>builder()
                                .writeMethod(WriteMethod.STORAGE_API_AT_LEAST_ONCE)
                                .table(
                                        TableDestination.of(
                                                "my-project", "analytics", "current_orders"))
                                .serializer(
                                        new OrderModSerializer(
                                                JsonDocumentSerializer.of(rowSchema)))
                                .cdcTableOptions(
                                        CdcTableOptions.builder()
                                                .primaryKeyColumns(
                                                        Collections.singletonList("OrderId"))
                                                .build())
                                .cdcOptions(
                                        CdcOptions.<OrderMod>builder(
                                                        mod ->
                                                                mod.isDeletion()
                                                                        ? CdcChangeType.DELETE
                                                                        : CdcChangeType.UPSERT)
                                                .sequenceNumberProvider(
                                                        mod ->
                                                                SpannerCdcSequenceNumber.of(
                                                                        mod.commitTimestamp(),
                                                                        mod.recordSequence(),
                                                                        mod.modNumber()))
                                                .build())
                                .build());

        env.execute("spanner-change-stream-to-bigquery-cdc");
        // end::bigquery-spanner-native-cdc-datastream[]
    }

    // tag::bigquery-spanner-native-cdc-deserializer[]

    /**
     * Emits one element per mod. The mod number is the mod's zero-based position in the change
     * record, which is what makes several mods of one record mutually orderable.
     */
    static final class OrderModDeserializer
            implements SpannerChangeStreamDeserializationSchema<OrderMod> {

        private static final long serialVersionUID = 1L;

        @Override
        public void deserialize(DataChangeRecord record, Collector<OrderMod> out)
                throws IOException {
            List<Mod> mods = record.getMods();
            for (int modNumber = 0; modNumber < mods.size(); modNumber++) {
                Mod mod = mods.get(modNumber);
                boolean deletion = record.getModType() == ModType.DELETE;
                out.collect(
                        new OrderMod(
                                record.getCommitTimestamp(),
                                record.getRecordSequence(),
                                modNumber,
                                deletion,
                                deletion
                                        ? mod.getKeysJson()
                                        : mod.getNewValuesJson()
                                                .orElseThrow(
                                                        () ->
                                                                new IOException(
                                                                        "The change stream captures no new"
                                                                                + " values for this mod"))));
            }
        }

        @Override
        public TypeInformation<OrderMod> getProducedType() {
            return TypeInformation.of(OrderMod.class);
        }
    }

    /** One Spanner mod with the three coordinates BigQuery orders it by. */
    static final class OrderMod implements Serializable {

        private static final long serialVersionUID = 1L;

        private final Instant commitTimestamp;
        private final String recordSequence;
        private final int modNumber;
        private final boolean deletion;
        private final String rowJson;

        OrderMod(
                Instant commitTimestamp,
                String recordSequence,
                int modNumber,
                boolean deletion,
                String rowJson) {
            this.commitTimestamp = commitTimestamp;
            this.recordSequence = recordSequence;
            this.modNumber = modNumber;
            this.deletion = deletion;
            this.rowJson = rowJson;
        }

        Instant commitTimestamp() {
            return commitTimestamp;
        }

        String recordSequence() {
            return recordSequence;
        }

        int modNumber() {
            return modNumber;
        }

        boolean isDeletion() {
            return deletion;
        }

        String rowJson() {
            return rowJson;
        }
    }

    // end::bigquery-spanner-native-cdc-deserializer[]

    private static final class OrderModSerializer extends BigQueryProtoSerializer<OrderMod> {

        private static final long serialVersionUID = 1L;

        private final JsonDocumentSerializer delegate;

        private OrderModSerializer(JsonDocumentSerializer delegate) {
            this.delegate = delegate;
        }

        @Override
        public TableSchema getTableSchema(TableDestination destination) {
            return delegate.getTableSchema(destination);
        }

        @Override
        public Descriptors.Descriptor getDescriptor(TableDestination destination) {
            return delegate.getDescriptor(destination);
        }

        @Override
        public Object getSchemaFingerprint(TableDestination destination) {
            return delegate.getSchemaFingerprint(destination);
        }

        @Override
        public ByteString serialize(OrderMod mod) throws IOException {
            return delegate.serialize(mod.rowJson());
        }
    }
}
