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
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.util.Collector;

import com.google.pubsub.v1.PubsubMessage;
import io.github.flink.gcp.connector.pubsub.source.SubscriptionDestination;

import java.io.Serializable;

/**
 * Appends a delivery's metadata columns to every row the format emits for it.
 *
 * <p>Bound to one message at a time and reused, so the metadata row is built once per message
 * rather than once per emitted row — a format that produces many rows from one message reads the
 * message's metadata once.
 *
 * <p>The rows are joined rather than copied, and the physical row's {@link
 * org.apache.flink.types.RowKind} is carried over: the source delegates its changelog mode to the
 * format, so a format that emits updates must keep emitting them through this.
 */
@Internal
final class MetadataAppendingCollector implements Collector<RowData>, Serializable {

    private static final long serialVersionUID = 1L;

    private final ReadableMetadata[] metadata;

    private transient Collector<RowData> out;
    private transient GenericRowData metadataRow;

    MetadataAppendingCollector(ReadableMetadata[] metadata) {
        this.metadata = metadata;
    }

    void bind(PubsubMessage message, SubscriptionDestination subscription, Collector<RowData> out) {
        this.out = out;
        this.metadataRow = new GenericRowData(metadata.length);
        for (int i = 0; i < metadata.length; i++) {
            metadataRow.setField(i, metadata[i].getConverter().read(message, subscription));
        }
    }

    void unbind() {
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
