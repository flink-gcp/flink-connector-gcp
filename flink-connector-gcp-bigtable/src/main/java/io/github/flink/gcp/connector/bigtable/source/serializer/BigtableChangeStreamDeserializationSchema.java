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

package io.github.flink.gcp.connector.bigtable.source.serializer;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.util.Collector;

import com.google.cloud.bigtable.data.v2.models.ChangeStreamMutation;

import java.io.IOException;
import java.io.Serializable;

/**
 * Turns one Bigtable change-stream mutation into zero or more user records.
 *
 * <p>Returning successfully without collecting skips the mutation and increments {@code
 * recordsSkipped} once. Collected records must be non-null. The collector is valid only for the
 * synchronous duration of the call; an implementation must not retain it.
 */
@PublicEvolving
public interface BigtableChangeStreamDeserializationSchema<T>
        extends Serializable, ResultTypeQueryable<T> {

    default void open(DeserializationSchema.InitializationContext context) throws Exception {}

    /**
     * Deserializes one mutation.
     *
     * @param mutation the mutation from Bigtable
     * @param out the collector for non-null output records; it is valid only for this synchronous
     *     call and must not be retained
     * @throws IOException if the mutation cannot be deserialized
     */
    void deserialize(ChangeStreamMutation mutation, Collector<T> out) throws IOException;

    @Override
    TypeInformation<T> getProducedType();
}
