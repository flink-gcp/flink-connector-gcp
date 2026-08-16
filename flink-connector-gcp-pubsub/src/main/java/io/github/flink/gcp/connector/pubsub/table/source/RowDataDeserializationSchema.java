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
import io.github.flink.gcp.connector.base.source.SynchronousDeserializationCollector;
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
            // Exactly what the payload adapter does, and the common case: no wrapper, no copy.
            physical.deserialize(message.getData().toByteArray(), out);
            return;
        }
        GenericRowData metadataRow = new GenericRowData(metadata.length);
        for (int i = 0; i < metadata.length; i++) {
            metadataRow.setField(i, metadata[i].getConverter().read(message, subscription));
        }
        SynchronousDeserializationCollector.<RowData, IOException>deserialize(
                physicalRow ->
                        out.collect(
                                new JoinedRowData(
                                        physicalRow.getRowKind(), physicalRow, metadataRow)),
                physicalOut -> physical.deserialize(message.getData().toByteArray(), physicalOut));
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return producedTypeInfo;
    }
}
