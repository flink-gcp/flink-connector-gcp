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

package io.github.flink.gcp.connector.pubsub.table.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.utils.ProjectedRowData;
import org.apache.flink.util.Preconditions;

import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import io.github.flink.gcp.connector.pubsub.sink.serializer.PubSubSerializationSchema;

import java.io.IOException;
import java.util.stream.IntStream;

/**
 * Turns a table row into a Pub/Sub message: the format encodes the physical columns into the
 * payload, and the selected {@link WritableMetadata} columns become the message's attributes and
 * ordering key.
 *
 * <p>The row this receives is {@code [physical columns | metadata columns]} — the format's encoder
 * must see only the physical prefix, so the row is passed through a {@link ProjectedRowData} that
 * exposes exactly it. The projection is a view rather than a copy, and is reused across records.
 *
 * <p>The metadata could instead be layered on with {@link PubSubSerializationSchema#withAttributes}
 * and {@link PubSubSerializationSchema#withOrderingKey}. Writing the message here directly is
 * deliberate: the attributes extractor of those combinators returns a {@code Map<String, String>},
 * so every record would allocate an intermediate map only to copy it into the protobuf builder,
 * while this writes {@code MapData} straight into the builder.
 */
@Internal
final class RowDataSerializationSchema implements PubSubSerializationSchema<RowData> {

    private static final long serialVersionUID = 1L;

    private final SerializationSchema<RowData> physical;
    private final int physicalArity;
    private final WritableMetadata[] metadata;

    /**
     * The projection exposing the physical prefix of a consumed row. {@link RowData} is not
     * serializable, so this is built on the task rather than shipped with the job graph.
     */
    private transient ProjectedRowData projection;

    RowDataSerializationSchema(
            SerializationSchema<RowData> physical, int physicalArity, WritableMetadata[] metadata) {
        Preconditions.checkArgument(physicalArity >= 0, "physicalArity must not be negative");
        this.physical = Preconditions.checkNotNull(physical, "physical must not be null");
        this.physicalArity = physicalArity;
        this.metadata = Preconditions.checkNotNull(metadata, "metadata must not be null");
    }

    @Override
    public void open(SerializationSchema.InitializationContext context) throws Exception {
        physical.open(context);
    }

    @Override
    public PubsubMessage serialize(RowData element) throws IOException {
        byte[] data = physical.serialize(metadata.length == 0 ? element : projected(element));
        if (data == null) {
            throw new IOException(
                    "The format "
                            + physical.getClass().getName()
                            + " returned null for a row. Flink's SerializationSchema contract has"
                            + " no null in it, so this is a serialization failure rather than the"
                            + " sink's skip-the-record convention, which SQL cannot express.");
        }
        PubsubMessage.Builder builder =
                PubsubMessage.newBuilder().setData(ByteString.copyFrom(data));
        for (int i = 0; i < metadata.length; i++) {
            metadata[i].getWriter().write(element, physicalArity + i, builder);
        }
        return builder.build();
    }

    private RowData projected(RowData element) {
        if (projection == null) {
            projection = ProjectedRowData.from(IntStream.range(0, physicalArity).toArray());
        }
        return projection.replaceRow(element);
    }
}
