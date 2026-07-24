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

package io.github.flink.gcp.connector.bigquery.sink.storageapi.writer;

import org.apache.flink.annotation.Internal;

import com.google.api.core.ApiFuture;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.ProtoRows;

/**
 * Appends row batches to one buffered write stream at explicit offsets.
 *
 * <p>A separate SPI from {@link RowAppender} rather than an extension: the default-stream contract
 * has no offsets and its repair model (rebuild the appender, re-append) is incompatible with
 * offset-tracked appends, so the two writers share no appender machinery.
 */
@Internal
public interface OffsetRowAppender extends AutoCloseable {

    /**
     * Appends the rows at the given stream offset.
     *
     * @param rows the serialized rows
     * @param offset the offset of the first row in the stream
     * @return the pending server response
     */
    ApiFuture<AppendRowsResponse> append(ProtoRows rows, long offset);

    @Override
    void close();
}
