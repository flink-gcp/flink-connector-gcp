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

package io.github.flink.gcp.connector.bigquery.sink.serializer;

import org.apache.flink.annotation.Internal;

import io.github.flink.gcp.connector.bigquery.sink.TableDestination;

/**
 * A destination's write schema failed to derive while a record was being serialized.
 *
 * <p>The writers derive a destination's schema before their row-failure boundary, so that a schema
 * conflict fails the job instead of reaching the failure handler once per record. A serializer that
 * changes its schema fingerprint while serializing a record makes the derivation run again inside
 * that boundary. This type marks that derivation's failure so the writers rethrow it there rather
 * than handle it as the record's failure.
 */
@Internal
public final class SchemaDerivationException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    /**
     * Creates the failure for a destination whose schema failed to derive while a record was
     * serialized.
     *
     * @param destination the destination whose schema failed to derive
     * @param cause the derivation failure
     */
    public SchemaDerivationException(TableDestination destination, RuntimeException cause) {
        super(
                "The schema of " + destination + " failed to derive while a record was serialized",
                cause);
    }
}
