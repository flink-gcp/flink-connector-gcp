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

package io.github.flink.gcp.connector.bigtable.table.source;

import com.google.api.core.ApiFuture;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.protobuf.ByteString;

import java.io.Serializable;

/** The point-read seam shared by the synchronous and asynchronous lookup functions. */
interface BigtableRowLookup extends Serializable, AutoCloseable {

    /** Opens the client resources used by the point reads. */
    void open() throws Exception;

    /** Reads one row synchronously. */
    Row read(ByteString rowKey);

    /** Reads one row asynchronously. */
    ApiFuture<Row> readAsync(ByteString rowKey);

    /** Releases the client resources. */
    @Override
    void close() throws Exception;
}
