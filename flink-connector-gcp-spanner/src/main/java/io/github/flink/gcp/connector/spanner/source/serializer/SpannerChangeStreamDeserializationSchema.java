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

package io.github.flink.gcp.connector.spanner.source.serializer;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.util.Collector;

import io.github.flink.gcp.connector.spanner.source.changestream.DataChangeRecord;

import java.io.IOException;
import java.io.Serializable;

/** Turns one Spanner data-change record into zero or more user records. */
@PublicEvolving
public interface SpannerChangeStreamDeserializationSchema<T>
        extends Serializable, ResultTypeQueryable<T> {

    default void open(DeserializationSchema.InitializationContext context) throws Exception {}

    /**
     * Deserializes one self-describing change record.
     *
     * @param record the record, including the column types active when Spanner captured it
     * @param out the collector for zero or more output records; it is valid only for this method
     *     call, so implementations must collect synchronously and must not retain it
     * @throws IOException if the change cannot be deserialized
     */
    void deserialize(DataChangeRecord record, Collector<T> out) throws IOException;

    @Override
    TypeInformation<T> getProducedType();
}
