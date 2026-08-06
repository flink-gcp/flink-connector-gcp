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

package io.github.flink.gcp.connector.bigquery.sink.storage.writer;

import org.apache.flink.annotation.Internal;

import com.google.api.core.ApiFuture;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.ProtoRows;

/**
 * Appends batches of serialized rows to one destination's default write stream.
 *
 * <p>Abstracts the Storage Write API {@code StreamWriter} so writer logic is unit-testable: that
 * class cannot be subclassed to stand in for one. Not because it is {@code final} — it is not, and
 * neither is {@code BigQueryWriteClient}, as this said until #325 checked both against
 * bigquerystorage 3.30.0 — but because its constructors are not accessible, which forbids a
 * subclass just as effectively. The same correction #324 made for {@code Publisher}.
 */
@Internal
public interface RowAppender extends AutoCloseable {

    /**
     * Appends a batch of rows asynchronously.
     *
     * @param rows the serialized rows
     * @return the append result future
     */
    ApiFuture<AppendRowsResponse> append(ProtoRows rows);

    /** Closes the underlying stream writer. */
    @Override
    void close();
}
