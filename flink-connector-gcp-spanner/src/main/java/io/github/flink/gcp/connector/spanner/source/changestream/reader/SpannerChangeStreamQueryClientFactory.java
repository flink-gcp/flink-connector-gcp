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

package io.github.flink.gcp.connector.spanner.source.changestream.reader;

import org.apache.flink.annotation.Internal;

import io.github.flink.gcp.connector.base.metrics.ClientMetrics;

import javax.annotation.Nullable;

import java.io.Serializable;

/** Serializable factory for the TaskManager-local Change Streams query client. */
@Internal
@FunctionalInterface
public interface SpannerChangeStreamQueryClientFactory extends Serializable {

    /**
     * Opens one client.
     *
     * <p>The client metrics are a parameter rather than state of this factory, because the factory
     * travels in the job graph and every reader built from it registers its own: the client owns
     * them from here on and closes them after its service handle, and a creation that fails closes
     * them.
     *
     * @param clientMetrics the client metrics the owning reader registered, or {@code null} to
     *     export nothing to Flink
     * @return the client, owned by the caller
     * @throws Exception if the client cannot be opened
     */
    SpannerChangeStreamQueryClient create(@Nullable ClientMetrics clientMetrics) throws Exception;
}
