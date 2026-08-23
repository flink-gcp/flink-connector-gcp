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

package io.github.flink.gcp.connector.bigtable.source.serializer;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.util.Collector;

import io.github.flink.gcp.connector.bigtable.source.changestream.BigtableChangeStreamMutation;

import java.io.IOException;
import java.io.Serializable;

/**
 * Turns one Bigtable change-stream mutation into zero or more user records.
 *
 * <p>Returning successfully without collecting skips the mutation and increments {@code
 * recordsSkipped} once. Collected records must be non-null. The collector is valid only for the
 * synchronous duration of the call; an implementation must not retain it.
 *
 * @param <T> the record type produced
 */
@PublicEvolving
public interface BigtableChangeStreamDeserializationSchema<T>
        extends Serializable, ResultTypeQueryable<T> {

    /**
     * Prepares this deserializer, once per reader, before any mutation reaches it.
     *
     * @param context the initialization context, which carries the metric group and the user code
     *     class loader
     * @throws Exception if initialization fails, which fails the job
     */
    default void open(DeserializationSchema.InitializationContext context) throws Exception {}

    /**
     * Deserializes one mutation.
     *
     * @param mutation the complete connector-owned mutation from Bigtable
     * @param out the collector for non-null output records; it is valid only for this synchronous
     *     call and must not be retained
     * @throws IOException if the mutation cannot be deserialized
     */
    void deserialize(BigtableChangeStreamMutation mutation, Collector<T> out) throws IOException;

    /**
     * Returns the type of the records this produces.
     *
     * <p>Declared here rather than inherited silently, because a source has no other way to type
     * its output: nothing about a {@link BigtableChangeStreamMutation} says what a job means to
     * make of it.
     */
    @Override
    TypeInformation<T> getProducedType();
}
