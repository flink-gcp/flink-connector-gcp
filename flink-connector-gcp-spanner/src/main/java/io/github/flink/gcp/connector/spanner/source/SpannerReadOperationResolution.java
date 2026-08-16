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

package io.github.flink.gcp.connector.spanner.source;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.DatabaseClient;

import java.io.IOException;

/** Internal bridge between a table-layer deferred read and the batch partition planner. */
@Internal
public final class SpannerReadOperationResolution {

    private SpannerReadOperationResolution() {}

    /** Creates a deferred operation without adding a table-only method to the public value type. */
    public static SpannerReadOperation deferred(SpannerReadOperationResolver resolver) {
        return SpannerReadOperation.deferred(
                Preconditions.checkNotNull(resolver, "resolver must not be null"));
    }

    /** Resolves the operation when necessary, otherwise returns it unchanged. */
    public static SpannerReadOperation resolve(
            SpannerReadOperation operation, DatabaseClient client, Timestamp readTimestamp)
            throws IOException {
        SpannerReadOperationResolver resolver = operation.getResolver();
        return resolver == null ? operation : resolver.resolve(client, readTimestamp);
    }
}
