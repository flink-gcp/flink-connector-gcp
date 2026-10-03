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

package io.github.flink.gcp.connector.firestore.source.serializer;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.util.Collector;

import com.google.cloud.firestore.DocumentSnapshot;

import java.io.IOException;
import java.io.Serializable;

/**
 * Turns a Firestore document into the records a job works with.
 *
 * <p>A document may produce no record, one, or several. Emitting nothing filters the document: it
 * is not a failure, it never reaches any handler, and the {@code recordsSkipped} counter is the
 * only thing that reports it.
 *
 * <p>Collected records must be non-null. The collector is valid only for the synchronous duration
 * of the call; an implementation must not retain it. The source moves its resume point past the
 * document once the call returns normally, whatever the number of outputs; a call that throws fails
 * the job instead.
 *
 * <p>The snapshot is the client library's own, read at the source's snapshot time: {@link
 * DocumentSnapshot#getReadTime()} is that time for every document of one job, and {@link
 * DocumentSnapshot#getReference()} names the document. When the source was given a projection, the
 * snapshot holds only the projected fields.
 *
 * <p>Implementations are {@link Serializable} because the source configuration travels in the job
 * graph. Anything that cannot be serialized is a {@code transient} field rebuilt in {@link #open}.
 *
 * @param <T> the record type produced
 */
@PublicEvolving
public interface FirestoreDocumentDeserializationSchema<T>
        extends Serializable, ResultTypeQueryable<T> {

    /**
     * Prepares this deserializer, once per reader, before any document reaches it.
     *
     * @param context the initialization context, which carries the metric group and the user code
     *     class loader
     * @throws Exception if initialization fails, which fails the job
     */
    default void open(DeserializationSchema.InitializationContext context) throws Exception {}

    /**
     * Turns one document into zero or more records.
     *
     * @param document the document as the source read it
     * @param out the collector for non-null output records; emitting nothing skips the document,
     *     and the collector is valid only for this synchronous call and must not be retained
     * @throws IOException if the document cannot be deserialized, which fails the job
     */
    void deserialize(DocumentSnapshot document, Collector<T> out) throws IOException;

    /**
     * Returns the type of the records this produces.
     *
     * <p>Declared here rather than inherited silently, because a source has no other way to type
     * its output: nothing about a document says what a job means to make of it.
     */
    @Override
    TypeInformation<T> getProducedType();
}
