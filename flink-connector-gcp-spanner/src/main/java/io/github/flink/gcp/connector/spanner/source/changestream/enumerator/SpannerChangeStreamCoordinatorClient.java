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

package io.github.flink.gcp.connector.spanner.source.changestream.enumerator;

import org.apache.flink.annotation.Internal;

import java.time.Duration;

/** Metadata operations the Spanner Change Streams coordinator needs during initialization. */
@Internal
public interface SpannerChangeStreamCoordinatorClient extends AutoCloseable {

    /** Rejects a stream whose partition mode requires record types this source does not support. */
    void validatePartitionMode() throws Exception;

    /** Returns the stream's effective retention period. */
    Duration retention() throws Exception;

    @Override
    void close() throws Exception;
}
