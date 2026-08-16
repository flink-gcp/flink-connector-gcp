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

import com.google.cloud.bigtable.data.v2.models.Row;

import java.io.IOException;
import java.io.Serializable;

/**
 * Turns a Bigtable row into the records a job works with.
 *
 * <p>A row may produce no record, one, or several. A Bigtable row is a whole row — many column
 * families, many qualifiers, many timestamped cell versions — and fanning one out into a record per
 * qualifier or per cell is a mapping wide-table jobs genuinely want, so the collector shape is
 * offered rather than a single nullable return. Emitting nothing filters the row: it is not a
 * failure, it never reaches any handler, and the {@code recordsSkipped} counter is the only thing
 * that reports it.
 *
 * <p>Collected records must be non-null. The collector is valid only for the synchronous duration
 * of the call; an implementation must not retain it. The source advances its row-key progress once
 * after a successful call, independently of the number of outputs.
 *
 * <p>Configuration errors belong at job-build time, thrown unchecked from a builder or a
 * constructor, not per row. A schema this deserializer cannot work with is the same error for every
 * row in the table, and discovering it on a TaskManager turns one mistake into a restart loop.
 *
 * <p>Implementations are {@link Serializable} because the source configuration travels in the job
 * graph. Anything that cannot be serialized — a parsed schema, a client — is a {@code transient}
 * field rebuilt in {@link #open}.
 *
 * <p>The {@code Row} in the name is deliberate rather than incidental: the argument is the client
 * library's {@code Row}, a whole Bigtable row, and naming it is what distinguishes this from a
 * schema over cells or over a projected subset — either of which a later Table API layer may want
 * beside it.
 *
 * @param <T> the record type produced
 */
@PublicEvolving
public interface BigtableRowDeserializationSchema<T> extends Serializable, ResultTypeQueryable<T> {

    /**
     * Prepares this deserializer, once per reader, before any row reaches it.
     *
     * @param context the initialization context, which carries the metric group and the user code
     *     class loader
     * @throws Exception if initialization fails, which fails the job
     */
    default void open(DeserializationSchema.InitializationContext context) throws Exception {}

    /**
     * Turns one row into zero or more records.
     *
     * @param row the row read from Bigtable, whose cells are ordered as the service returned them
     * @param out the collector for non-null output records; emitting nothing skips the row, and the
     *     collector is valid only for this synchronous call and must not be retained
     * @throws IOException if the row cannot be deserialized, which fails the job
     */
    void deserialize(Row row, Collector<T> out) throws IOException;

    /**
     * Returns the type of the records this produces.
     *
     * <p>Declared here rather than inherited silently, because a source has no other way to type
     * its output: nothing about a {@link Row} says what a job means to make of it.
     */
    @Override
    TypeInformation<T> getProducedType();
}
