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

package io.github.flink.gcp.connector.pubsub.sink;

import org.apache.flink.annotation.Internal;

import io.github.flink.gcp.connector.pubsub.sink.serializer.PubSubSerializationSchema;

import java.io.Serializable;

/**
 * Immutable sink configuration assembled by {@link PubSubSinkBuilder}.
 *
 * @param <T> type of the records written by the sink
 */
@Internal
public final class PubSubSinkConfig<T> implements Serializable {

    private static final long serialVersionUID = 1L;

    private final DestinationResolver<? super T> destinationResolver;
    private final PubSubSerializationSchema<? super T> serializer;
    private final CreateDisposition createDisposition;

    PubSubSinkConfig(
            DestinationResolver<? super T> destinationResolver,
            PubSubSerializationSchema<? super T> serializer,
            CreateDisposition createDisposition) {
        this.destinationResolver = destinationResolver;
        this.serializer = serializer;
        this.createDisposition = createDisposition;
    }

    /** Returns the per-record destination resolver. */
    public DestinationResolver<? super T> getDestinationResolver() {
        return destinationResolver;
    }

    /** Returns the record serialization schema. */
    public PubSubSerializationSchema<? super T> getSerializer() {
        return serializer;
    }

    /** Returns whether the sink may create destination topics that do not exist. */
    public CreateDisposition getCreateDisposition() {
        return createDisposition;
    }
}
