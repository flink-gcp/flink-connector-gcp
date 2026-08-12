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

package io.github.flink.gcp.connector.spanner.source;

import org.apache.flink.annotation.Internal;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.DatabaseClient;

import java.io.IOException;
import java.io.Serializable;

/** Resolves a table-layer read after the batch transaction has selected its snapshot. */
@Internal
public interface SpannerReadOperationResolver extends Serializable {

    /**
     * Resolves the concrete read operation at the batch transaction's exact snapshot.
     *
     * @param client a client for the source database
     * @param readTimestamp the batch transaction's read timestamp
     * @return a concrete query or table read
     * @throws IOException if metadata cannot be read or validated
     */
    SpannerReadOperation resolve(DatabaseClient client, Timestamp readTimestamp) throws IOException;
}
