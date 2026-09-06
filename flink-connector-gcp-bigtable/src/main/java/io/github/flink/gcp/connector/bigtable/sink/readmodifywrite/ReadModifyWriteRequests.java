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

package io.github.flink.gcp.connector.bigtable.sink.readmodifywrite;

import org.apache.flink.annotation.Internal;

import io.github.flink.gcp.connector.bigtable.sink.singlerow.BigtableRow;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.writer.RowRequest;

/** Internal bridge from read-modify-write models to the shared request runtime. */
@Internal
public final class ReadModifyWriteRequests {
    private ReadModifyWriteRequests() {}

    /** Adapts ordered rules without exposing the SDK through the public request model. */
    public static RowRequest<BigtableRow> adapt(ReadModifyWriteRequest request) {
        return request.toRequest();
    }
}
