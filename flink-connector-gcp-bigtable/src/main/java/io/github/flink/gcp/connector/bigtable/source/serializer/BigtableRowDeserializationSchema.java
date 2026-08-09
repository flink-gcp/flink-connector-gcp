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
 * <p><b>The Bigtable source can offer this and the BigQuery source cannot</b>, and the difference
 * is in what a checkpoint resumes from rather than in taste. A BigQuery read stream resumes at a
 * count of rows handed downstream, so a row producing several records would move that count off the
 * rows it counts; this source resumes at a <em>row key</em>, so a row producing none or five
 * advances the resume point by exactly one row either way. The rule the two connectors share is
 * therefore: the collector where a one-to-many mapping is meaningful and the resume unit permits
 * it, the nullable return where the resume unit forbids it.
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
     * @param out the collector the records are handed to; emitting nothing skips the row
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
