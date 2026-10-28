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

package io.github.flink.gcp.connector.spanner.source.changestream.enumerator;

import org.apache.flink.annotation.Internal;

import io.github.flink.gcp.connector.base.metrics.ClientMetrics;

import javax.annotation.Nullable;

import java.io.Serializable;

/** Serializable job-graph seam that opens the coordinator's runtime metadata client. */
@Internal
@FunctionalInterface
public interface SpannerChangeStreamCoordinatorClientFactory extends Serializable {

    /**
     * Opens one client.
     *
     * <p>The client metrics are a parameter rather than state of this factory, because the factory
     * travels in the job graph and the client metrics are runtime state of the source. The client
     * records into them and never closes them: every enumerator the source builds shares them.
     *
     * @param clientMetrics the source's client metrics for its enumerators, or {@code null} to
     *     export nothing to Flink
     * @return the client, owned by the caller
     * @throws Exception if the client cannot be opened
     */
    SpannerChangeStreamCoordinatorClient create(@Nullable ClientMetrics clientMetrics)
            throws Exception;
}
