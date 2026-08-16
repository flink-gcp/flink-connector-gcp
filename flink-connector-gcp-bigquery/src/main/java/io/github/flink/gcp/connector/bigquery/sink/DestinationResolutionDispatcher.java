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

package io.github.flink.gcp.connector.bigquery.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.sink2.SinkWriter;

import java.io.IOException;

/** Dispatches destination-resolution results to the writer that owns the record. */
@Internal
public final class DestinationResolutionDispatcher {

    private DestinationResolutionDispatcher() {}

    /** Handles every destination-resolution result supported by the connector. */
    @Internal
    public interface Visitor<T> {

        /** Handles a record with a resolved table destination. */
        void visit(TableDestination destination, T element, SinkWriter.Context context)
                throws IOException;

        /** Handles a record-specific destination-resolution failure. */
        void visit(UnroutableRecord failure, T element, SinkWriter.Context context)
                throws IOException;
    }

    /** Dispatches a result without allocating a per-record callback. */
    public static <T> void dispatch(
            DestinationResolution resolution,
            T element,
            SinkWriter.Context context,
            Visitor<T> visitor)
            throws IOException {
        if (resolution == null) {
            throw new IOException("The destination resolver returned null for a record.");
        }
        resolution.accept(element, context, visitor);
    }
}
