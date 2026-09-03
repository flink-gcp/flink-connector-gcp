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

package io.github.flink.gcp.connector.bigtable.sink.singlerow.writer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.connector.sink2.SinkWriter;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;

/**
 * Turns a record into the single-row request the sink surface sends for it.
 *
 * <p>The runtime's seam, not a user's: the per-operation sinks adapt their own public serialization
 * schemas to it, and what those schemas look like is theirs to settle. The contract is the one
 * every sink serializer of this project has (ADR-0001): {@code null} means skip the record, and a
 * thrown exception means the record could not be serialized and is handed to the failure handler.
 *
 * @param <T> type of the records
 */
@Internal
public interface RowRequestSerializer<T> extends Serializable {

    /**
     * Initializes the serializer once per subtask, before the first record.
     *
     * @param context the initialization context
     * @throws Exception if the serializer cannot be initialized
     */
    default void open(SerializationSchema.InitializationContext context) throws Exception {}

    /**
     * Builds the request for a record.
     *
     * @param element the record
     * @param context the sink writer context
     * @return the request, or {@code null} to skip the record
     * @throws IOException if the record cannot be serialized
     */
    @Nullable
    RowRequest<?> serialize(T element, SinkWriter.Context context) throws IOException;
}
