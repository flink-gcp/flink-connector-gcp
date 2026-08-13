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

import com.google.cloud.spanner.Struct;

import java.io.IOException;
import java.io.Serializable;

/**
 * Turns a Spanner row into zero or more output records.
 *
 * <p>A row arrives as an immutable {@link Struct}, read through the column names or ordinals the
 * read operation asked for. The producing type is declared by {@link #getProducedType()}, so a job
 * needs no {@code returns(...)} call after the source.
 *
 * <p>One row may produce zero, one, or several records. Returning successfully without collecting
 * skips the row: nothing is emitted, it is not a failure, and {@code recordsSkipped} is the only
 * thing that reports it. Use that contract to filter rows a predicate cannot express; throw when a
 * row cannot be read.
 *
 * <p>Collected records must be non-null. The collector is valid only for the synchronous duration
 * of the call; an implementation must not retain it.
 *
 * @param <T> the record type produced
 */
@PublicEvolving
public interface SpannerStructDeserializationSchema<T>
        extends Serializable, ResultTypeQueryable<T> {

    /**
     * Prepares the deserializer, once per reader, before any row reaches it.
     *
     * @param context the initialization context
     * @throws Exception if the deserializer cannot be prepared, which fails the job
     */
    default void open(DeserializationSchema.InitializationContext context) throws Exception {}

    /**
     * Turns one row into zero or more records.
     *
     * @param row the row, valid only for the duration of this call
     * @param out the collector for non-null output records; it is valid only for this synchronous
     *     call and must not be retained
     * @throws IOException if the row cannot be turned into output records
     */
    void deserialize(Struct row, Collector<T> out) throws IOException;

    @Override
    TypeInformation<T> getProducedType();
}
