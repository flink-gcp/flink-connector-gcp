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

package io.github.flink.gcp.connector.pubsub.table.source;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import com.google.pubsub.v1.PubsubMessage;
import io.github.flink.gcp.connector.pubsub.source.SubscriptionDestination;
import io.github.flink.gcp.connector.pubsub.source.serializer.PubSubDeserializationSchema;

import java.io.IOException;

/**
 * Turns a Pub/Sub message into table rows: the format decodes the payload into the physical
 * columns, and the selected {@link ReadableMetadata} columns are appended to whatever it produced.
 *
 * <p>Pub/Sub has no key/value split, so the produced row is a plain concatenation — {@code
 * [physical columns | metadata columns]} — rather than the projection Kafka's equivalent needs.
 *
 * <p>One message may produce any number of rows, so the metadata is appended by a collector wrapped
 * around the output rather than after the fact: every row the format emits gets the same metadata,
 * and a format that emits none produces none.
 */
@Internal
final class RowDataDeserializationSchema implements PubSubDeserializationSchema<RowData> {

    private static final long serialVersionUID = 1L;

    private final DeserializationSchema<RowData> physical;
    private final ReadableMetadata[] metadata;
    private final TypeInformation<RowData> producedTypeInfo;

    /** Reused across messages; a deserialization schema is confined to the task thread. */
    private transient MetadataAppendingCollector collector;

    RowDataDeserializationSchema(
            DeserializationSchema<RowData> physical,
            ReadableMetadata[] metadata,
            TypeInformation<RowData> producedTypeInfo) {
        this.physical = Preconditions.checkNotNull(physical, "physical must not be null");
        this.metadata = Preconditions.checkNotNull(metadata, "metadata must not be null");
        this.producedTypeInfo =
                Preconditions.checkNotNull(producedTypeInfo, "producedTypeInfo must not be null");
    }

    @Override
    public void open(DeserializationSchema.InitializationContext context) throws Exception {
        physical.open(context);
    }

    @Override
    public void deserialize(
            PubsubMessage message, SubscriptionDestination subscription, Collector<RowData> out)
            throws IOException {
        if (metadata.length == 0) {
            // Exactly what the dataOnly adapter does, and the common case: no wrapper, no copy.
            physical.deserialize(message.getData().toByteArray(), out);
            return;
        }
        if (collector == null) {
            collector = new MetadataAppendingCollector();
        }
        collector.bind(message, subscription, out);
        try {
            physical.deserialize(message.getData().toByteArray(), collector);
        } finally {
            collector.unbind();
        }
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return producedTypeInfo;
    }

    /**
     * Appends a delivery's metadata columns to every row the format emits for it.
     *
     * <p>Bound to one message at a time and reused, so the metadata row is built once per message
     * rather than once per emitted row. The rows are joined rather than copied, and the physical
     * row's {@link org.apache.flink.types.RowKind} is carried over: the source delegates its
     * changelog mode to the format, so a format that emits updates must keep emitting them through
     * this.
     *
     * <p>Same shape as {@code PubSubRecordEmitter.SourceOutputCollector}, and nested for the same
     * reason: it is a detail of its one owner, and it is never serialized — the field holding it is
     * transient and it is rebuilt on the task.
     */
    private final class MetadataAppendingCollector implements Collector<RowData> {

        private Collector<RowData> out;
        private GenericRowData metadataRow;

        private void bind(
                PubsubMessage message,
                SubscriptionDestination subscription,
                Collector<RowData> out) {
            this.out = out;
            this.metadataRow = new GenericRowData(metadata.length);
            for (int i = 0; i < metadata.length; i++) {
                metadataRow.setField(i, metadata[i].getConverter().read(message, subscription));
            }
        }

        private void unbind() {
            this.out = null;
            this.metadataRow = null;
        }

        @Override
        public void collect(RowData physicalRow) {
            out.collect(new JoinedRowData(physicalRow.getRowKind(), physicalRow, metadataRow));
        }

        @Override
        public void close() {}
    }
}
