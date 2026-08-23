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

package io.github.flink.gcp.connector.bigquery.source.serializer;

import org.apache.flink.annotation.Public;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.util.Collector;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;

/**
 * Converts a row read from the BigQuery Storage Read API into zero or more output records.
 *
 * <p>The Storage Read API delivers rows as Avro binary blocks described by a schema the read
 * session carries, so the connector decodes each row into a {@link GenericRecord} and hands it
 * here. One call may produce zero, one, or several records. The source resumes a stream by the
 * number of input rows consumed, independently of how many records each row produces.
 *
 * <p>Implementations are shipped inside the Flink job graph and must be {@link Serializable}. Avro
 * {@link Schema} implements that contract and may be held directly.
 *
 * <p>Exception contract: {@link #deserialize(GenericRecord, Collector)} throws {@link IOException}
 * for per-record failures, which fail the job. Configuration errors — a malformed schema, a column
 * the implementation cannot map — must surface as unchecked exceptions when the deserializer is
 * built, not per record, so a misconfigured job fails at submission instead of once rows flow.
 *
 * <p>Returning successfully without collecting skips the row: nothing is emitted, it is not a
 * failure, and the {@code recordsSkipped} metric is the only report of it. Collected records must
 * be non-null. The collector is valid only for the synchronous duration of the call; an
 * implementation must not retain it.
 *
 * @param <T> type of the records produced by the source
 */
@Public
public interface BigQueryRowDeserializationSchema<T> extends Serializable, ResultTypeQueryable<T> {

    /**
     * Initializes the deserializer on the subtask that will use it, before any row is read.
     *
     * @param context the initialization context
     * @throws Exception if the deserializer cannot be initialized; the job fails
     */
    default void open(DeserializationSchema.InitializationContext context) throws Exception {}

    /**
     * Returns the Avro schema rows are decoded into, or {@code null} (the default) to decode them
     * into the read session's own schema.
     *
     * <p>When a schema is returned, rows are resolved from the session's schema into it by Avro's
     * schema-resolution rules, and the {@link GenericRecord} handed to {@link
     * #deserialize(GenericRecord, Collector)} carries the returned schema. An implementation that
     * declares a produced type derived from a schema must return that same schema here, so that the
     * records it emits and the type it declares agree.
     *
     * <p>Called once per assigned stream, not per row.
     *
     * @return the reader schema, or {@code null} to use the session's schema
     */
    @Nullable
    default Schema getReaderSchema() {
        return null;
    }

    /**
     * Converts one row into zero or more records.
     *
     * @param row the decoded row
     * @param out the collector for non-null output records; it is valid only for this synchronous
     *     call and must not be retained
     * @throws IOException if the row cannot be converted; the job fails
     */
    void deserialize(GenericRecord row, Collector<T> out) throws IOException;

    /**
     * Returns a deserializer handing rows on as the {@link GenericRecord}s they were decoded into.
     *
     * <p>The schema is required rather than taken from the read session because Flink needs the
     * produced type when the job graph is built, while a read session exists only once the job
     * runs. Rows are resolved into this schema, so it may differ from the table's own as far as
     * Avro's schema-resolution rules allow.
     *
     * <p>Using this deserializer requires {@code flink-avro} on the job's classpath: it is what
     * supplies Flink's Avro serializer for {@link GenericRecord}, without which Flink falls back to
     * Kryo, which cannot serialize one.
     *
     * @param readerSchema the Avro schema rows are read into
     * @return the deserializer
     */
    static BigQueryRowDeserializationSchema<GenericRecord> genericRecord(Schema readerSchema) {
        return new GenericRecordDeserializer(readerSchema);
    }

    /**
     * Returns a deserializer handing rows on as the {@link GenericRecord}s they were decoded into.
     *
     * @param readerSchemaJson the Avro schema rows are read into, in its JSON form
     * @return the deserializer
     * @see #genericRecord(Schema)
     */
    static BigQueryRowDeserializationSchema<GenericRecord> genericRecord(String readerSchemaJson) {
        return new GenericRecordDeserializer(readerSchemaJson);
    }
}
