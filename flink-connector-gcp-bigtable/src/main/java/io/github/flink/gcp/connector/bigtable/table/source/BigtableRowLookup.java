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

package io.github.flink.gcp.connector.bigtable.table.source;

import org.apache.flink.annotation.Internal;

import com.google.api.core.ApiFuture;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.protobuf.ByteString;

import javax.annotation.Nullable;

import java.io.Serializable;

/**
 * The point-read seam shared by the synchronous and asynchronous lookup functions.
 *
 * <p>Both reads report "no such row" as {@code null} rather than as an exception or an empty
 * collection, which is also how the client library reports it. A key outside the configured row
 * ranges is the same answer, because the connector does not read a row it would then have to
 * discard.
 */
@Internal
interface BigtableRowLookup extends Serializable, AutoCloseable {

    /** Opens the client resources used by the point reads. */
    void open() throws Exception;

    /** Reads one row synchronously, returning {@code null} when there is no such row to read. */
    @Nullable
    Row read(ByteString rowKey);

    /**
     * Reads one row asynchronously. The future completes with {@code null} when there is no such
     * row to read.
     */
    ApiFuture<Row> readAsync(ByteString rowKey);

    /** Releases the client resources. */
    @Override
    void close() throws Exception;
}
