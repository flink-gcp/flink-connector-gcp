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

import com.google.cloud.spanner.Struct;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;

/**
 * Turns a Spanner row into a record.
 *
 * <p>A row arrives as an immutable {@link Struct}, read through the column names or ordinals the
 * read operation asked for. The producing type is declared by {@link #getProducedType()}, so a job
 * needs no {@code returns(...)} call after the source.
 *
 * <p><b>Returning {@code null} skips the row.</b> The record is emitted nowhere, it is not a
 * failure, and {@code recordsSkipped} is the only thing that reports it — the same contract the
 * sink's serialization SPI carries in the other direction. Use it to filter rows a predicate cannot
 * express; do not use it to swallow a row you could not read, which is a failure and should be
 * thrown.
 *
 * <p>One row becomes at most one record. A Spanner row is a relational row, so a fan-out is a
 * {@code flatMap} in the job rather than a shape this SPI takes.
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
     * Turns one row into a record.
     *
     * @param row the row, valid only for the duration of this call
     * @return the record, or {@code null} to skip the row
     * @throws IOException if the row cannot be turned into a record
     */
    @Nullable
    T deserialize(Struct row) throws IOException;

    @Override
    TypeInformation<T> getProducedType();
}
