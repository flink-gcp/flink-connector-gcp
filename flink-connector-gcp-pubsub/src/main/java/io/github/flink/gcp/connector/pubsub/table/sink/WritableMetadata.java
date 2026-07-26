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
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

import com.google.pubsub.v1.PubsubMessage;

import java.io.IOException;
import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * The parts of a {@link PubsubMessage} other than its payload that a table can write, exposed as
 * {@code METADATA} columns.
 *
 * <p>The payload itself is not here — it comes from the table's format.
 */
@Internal
enum WritableMetadata {

    /**
     * The message attributes. Pub/Sub attributes are a {@code map<string, string>} and can hold
     * neither a null key nor a null value, so an entry with either fails the write rather than
     * being dropped: a silently discarded attribute is data loss the query cannot see.
     */
    ATTRIBUTES(
            "attributes",
            // Nullable key and value make the column easier to produce in a query (Flink infers
            // nullable element types for a MAP literal); the runtime rejects an actual null.
            DataTypes.MAP(DataTypes.STRING().nullable(), DataTypes.STRING().nullable()).nullable(),
            (row, pos, builder) -> {
                if (row.isNullAt(pos)) {
                    return;
                }
                MapData map = row.getMap(pos);
                ArrayData keys = map.keyArray();
                ArrayData values = map.valueArray();
                for (int i = 0; i < keys.size(); i++) {
                    if (keys.isNullAt(i) || values.isNullAt(i)) {
                        throw new IOException(
                                "A Pub/Sub message attribute has a null "
                                        + (keys.isNullAt(i) ? "key" : "value")
                                        + ", which Pub/Sub cannot represent. Filter such entries"
                                        + " out of the 'attributes' metadata column.");
                    }
                    builder.putAttributes(
                            keys.getString(i).toString(), values.getString(i).toString());
                }
            }),

    /**
     * The message ordering key. A null or empty value sets no key, matching the DataStream
     * serialization schema's combinators.
     *
     * <p>A non-empty key only reaches Pub/Sub when {@code sink.message-ordering.enabled} is true;
     * the writer rejects a keyed message otherwise, so {@link PubSubDynamicSink} refuses the pair
     * at plan time.
     */
    ORDERING_KEY(
            "ordering-key",
            DataTypes.STRING().nullable(),
            (row, pos, builder) -> {
                if (row.isNullAt(pos)) {
                    return;
                }
                String orderingKey = row.getString(pos).toString();
                if (!orderingKey.isEmpty()) {
                    builder.setOrderingKey(orderingKey);
                }
            });

    /** Writes one metadata column of a row into the message being built. */
    @FunctionalInterface
    interface MetadataWriter extends Serializable {

        void write(RowData row, int pos, PubsubMessage.Builder builder) throws IOException;
    }

    private final String key;
    private final DataType dataType;
    private final MetadataWriter writer;

    WritableMetadata(String key, DataType dataType, MetadataWriter writer) {
        this.key = key;
        this.dataType = dataType;
        this.writer = writer;
    }

    String getKey() {
        return key;
    }

    MetadataWriter getWriter() {
        return writer;
    }

    /**
     * Returns the metadata this connector can write, keyed by metadata key, in declaration order.
     *
     * <p>{@code SupportsWritingMetadata} hands the selected keys back in the iteration order of
     * this map, and the consumed row is laid out from that same list — so correctness does not rest
     * on <em>which</em> order this is, only on it being the one the planner echoes back. The map is
     * ordered anyway, as the ability's javadoc recommends, so the column layout of a plan is a
     * property of this declaration rather than of a hash function.
     */
    static Map<String, DataType> listAll() {
        Map<String, DataType> metadata = new LinkedHashMap<>();
        for (WritableMetadata value : values()) {
            metadata.put(value.key, value.dataType);
        }
        return metadata;
    }

    /** Returns the constant with the given metadata key. */
    static WritableMetadata of(String key) {
        for (WritableMetadata value : values()) {
            if (value.key.equals(key)) {
                return value;
            }
        }
        throw new IllegalArgumentException("Unknown Pub/Sub writable metadata key '" + key + "'.");
    }
}
